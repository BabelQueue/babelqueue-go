// Package asynq bridges the [github.com/hibiken/asynq] task queue to the
// BabelQueue canonical envelope, so an asynq-based Go app interoperates
// byte-for-byte with the PHP/Laravel, Python, ... SDKs over Redis.
//
// asynq is not a BabelQueue broker binding (its Redis storage is §1); it is a
// framework adapter, mirroring how Node's BullMQ adapter wraps the same core.
// The mapping is direct and lossless:
//
//   - the canonical envelope JSON IS the asynq task Payload (byte-identical to
//     every other SDK — no wrapping, no added fields), and
//   - the asynq task TypeName IS the envelope URN (job), so asynq routes on the
//     URN the same way every BabelQueue consumer does.
//
// Produce wraps [babelqueue.Make]/Encode into an *asynq.Task and enqueues it;
// consume decodes the task Payload back into an [babelqueue.Envelope] and
// dispatches by URN through an *asynq.ServeMux.
//
//	// produce
//	p := asynq.NewProducer(client) // client is *hibiken/asynq.Client
//	p.Dispatch(ctx, "urn:babel:orders:created", map[string]any{"order_id": 1042},
//		babelqueue.WithQueue("orders"))
//
//	// consume — route by URN on an asynq ServeMux
//	mux := hasynq.NewServeMux()
//	asynq.Register(mux, "urn:babel:orders:created",
//		func(ctx context.Context, env babelqueue.Envelope) error {
//			// env.Data, env.TraceID ...
//			return nil
//		})
//
// The envelope is unchanged (schema_version stays 1); asynq is purely additive.
//
// Full spec: https://babelqueue.com
package asynq

import (
	"context"
	"errors"

	babelqueue "github.com/babelqueue/babelqueue-go"
	hasynq "github.com/hibiken/asynq"
)

// ErrNotAccepted is returned by [Bind]/[Register] handlers when an inbound task
// payload fails consumer-side validation ([babelqueue.Envelope.Accepts]) — a
// missing URN, an unsupported schema_version, a blank trace_id, or missing data.
var ErrNotAccepted = errors.New("babelqueue/asynq: task payload is not an acceptable envelope")

// Enqueuer is the subset of *hibiken/asynq.Client the [Producer] uses. The
// concrete client satisfies it, and a fake satisfies it in tests.
type Enqueuer interface {
	EnqueueContext(ctx context.Context, task *hasynq.Task, opts ...hasynq.Option) (*hasynq.TaskInfo, error)
}

// Task builds an asynq task from an already-built envelope: TypeName = the URN
// (env.Job) and Payload = the canonical envelope JSON. Extra asynq options
// (e.g. hasynq.MaxRetry, hasynq.ProcessIn) are forwarded onto the task. It
// returns [babelqueue.ErrEmptyURN] when the envelope has no URN, and surfaces an
// encode error otherwise.
func Task(env babelqueue.Envelope, opts ...hasynq.Option) (*hasynq.Task, error) {
	if env.URN() == "" {
		return nil, babelqueue.ErrEmptyURN
	}
	body, err := env.Encode()
	if err != nil {
		return nil, err
	}
	return hasynq.NewTask(env.URN(), body, opts...), nil
}

// TaskFor builds the canonical envelope for (urn, data) via [babelqueue.Make]
// and wraps it as an asynq task (see [Task]). Pass envelope options
// ([babelqueue.WithQueue], [babelqueue.WithTraceID]) and/or asynq task options
// — the two option families are distinguished by type. It mints a fresh trace id
// unless [babelqueue.WithTraceID] is given.
func TaskFor(urn string, data map[string]any, opts ...any) (*hasynq.Task, error) {
	envOpts, taskOpts := splitOptions(opts)
	env, err := babelqueue.Make(urn, data, envOpts...)
	if err != nil {
		return nil, err
	}
	return Task(env, taskOpts...)
}

// Envelope decodes an asynq task's Payload into a BabelQueue envelope, resolving
// the urn alias (see [babelqueue.Decode]). The task TypeName is the URN, but the
// authoritative identity is the decoded body — TypeName is the routing key.
func Envelope(task *hasynq.Task) (babelqueue.Envelope, error) {
	return babelqueue.Decode(task.Payload())
}

// Producer publishes BabelQueue envelopes as asynq tasks over an [Enqueuer].
// It is safe for concurrent use when the underlying *asynq.Client is.
type Producer struct {
	client Enqueuer
}

// NewProducer wraps an [Enqueuer] (a *hibiken/asynq.Client, or a fake in tests).
func NewProducer(client Enqueuer) *Producer {
	return &Producer{client: client}
}

// Dispatch builds the canonical envelope for (urn, data) and enqueues it as an
// asynq task routed by URN, returning the asynq TaskInfo. Mix envelope options
// ([babelqueue.WithQueue], [babelqueue.WithTraceID]) and asynq task options in
// opts; they are distinguished by type. The asynq queue defaults to the task's
// queue option (or asynq's "default") — pass hasynq.Queue(...) to set it, since
// meta.queue records the logical BabelQueue queue, which need not equal asynq's.
func (p *Producer) Dispatch(ctx context.Context, urn string, data map[string]any, opts ...any) (*hasynq.TaskInfo, error) {
	task, err := TaskFor(urn, data, opts...)
	if err != nil {
		return nil, err
	}
	return p.client.EnqueueContext(ctx, task)
}

// Enqueue enqueues an already-built envelope as an asynq task (see [Task]),
// forwarding any asynq task options.
func (p *Producer) Enqueue(ctx context.Context, env babelqueue.Envelope, opts ...hasynq.Option) (*hasynq.TaskInfo, error) {
	task, err := Task(env, opts...)
	if err != nil {
		return nil, err
	}
	return p.client.EnqueueContext(ctx, task)
}

// Bind adapts a BabelQueue [babelqueue.Handler] to an asynq handler: it decodes
// the task Payload into an envelope, rejects it with [ErrNotAccepted] when it
// fails [babelqueue.Envelope.Accepts] (so asynq applies its own retry/archive
// policy), and otherwise invokes handler. Use [Register] to also route by URN on
// a ServeMux.
func Bind(handler babelqueue.Handler) hasynq.HandlerFunc {
	return func(ctx context.Context, task *hasynq.Task) error {
		env, err := babelqueue.Decode(task.Payload())
		if err != nil {
			return err
		}
		if !env.Accepts() {
			return ErrNotAccepted
		}
		return handler(ctx, env)
	}
}

// muxRegistrar is the subset of *hibiken/asynq.ServeMux that [Register] needs.
type muxRegistrar interface {
	Handle(pattern string, handler hasynq.Handler)
}

// Register wires handler onto mux for the given URN: it registers [Bind](handler)
// under the URN as the asynq TypeName pattern, so asynq routes tasks whose
// TypeName equals the URN to it — the asynq-native equivalent of BabelQueue's
// URN routing.
func Register(mux muxRegistrar, urn string, handler babelqueue.Handler) {
	mux.Handle(urn, Bind(handler))
}

// splitOptions partitions a mixed option slice into BabelQueue envelope options
// and asynq task options by concrete type. Unknown types are ignored.
func splitOptions(opts []any) (envOpts []babelqueue.Option, taskOpts []hasynq.Option) {
	for _, o := range opts {
		switch opt := o.(type) {
		case babelqueue.Option:
			envOpts = append(envOpts, opt)
		case hasynq.Option:
			taskOpts = append(taskOpts, opt)
		}
	}
	return envOpts, taskOpts
}

// Compile-time guard: the asynq client satisfies the Enqueuer seam.
var _ Enqueuer = (*hasynq.Client)(nil)
