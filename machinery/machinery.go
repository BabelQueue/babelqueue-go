// Package machinery bridges the [github.com/RichardKnop/machinery] task queue to
// the BabelQueue canonical envelope, so a machinery-based Go app interoperates
// byte-for-byte with the PHP/Laravel, Python, ... SDKs.
//
// machinery is not a BabelQueue broker binding (its Redis/AMQP storage is §1/§2);
// it is a framework adapter, mirroring the sibling [github.com/babelqueue/babelqueue-go/asynq]
// module. machinery is **argument-based** (a task is a tasks.Signature with a Name
// and typed Args), unlike asynq's raw-payload model, so the mapping is:
//
//   - the machinery task Name IS the envelope URN (job), so machinery routes on
//     the URN the same way every BabelQueue consumer does, and
//   - a single string Arg carries the canonical envelope JSON byte-for-byte
//     (no wrapping, no added fields — Redis §1 payload identity).
//
// Produce wraps [babelqueue.Make]/Encode into a *tasks.Signature and sends it;
// consume decodes the signature's envelope arg back into a [babelqueue.Envelope]
// and dispatches it through a handler registered under the URN.
//
//	// produce
//	p := machinery.NewProducer(server) // server is *RichardKnop/machinery/v2.Server
//	p.Dispatch(ctx, "urn:babel:orders:created", map[string]any{"order_id": 1042},
//		babelqueue.WithQueue("orders"))
//
//	// consume — register a handler under the URN as the machinery task name
//	machinery.Register(server, "urn:babel:orders:created",
//		func(ctx context.Context, env babelqueue.Envelope) error {
//			// env.Data, env.TraceID ...
//			return nil
//		})
//
// The envelope is unchanged (schema_version stays 1); machinery is purely additive.
//
// Full spec: https://babelqueue.com
package machinery

import (
	"context"
	"errors"

	babelqueue "github.com/babelqueue/babelqueue-go"

	"github.com/RichardKnop/machinery/v2/backends/result"
	"github.com/RichardKnop/machinery/v2/tasks"
)

// envelopeArgName labels the single signature argument that carries the canonical
// envelope JSON. machinery matches a task func's parameters positionally, so the
// name is documentation; Type "string" is what binds it to a string parameter.
const envelopeArgName = "envelope"

// ErrNotAccepted is returned by [Bind]/[Register] handlers when an inbound task
// payload fails consumer-side validation ([babelqueue.Envelope.Accepts]) — a
// missing URN, an unsupported schema_version, a blank trace_id, or missing data.
var ErrNotAccepted = errors.New("babelqueue/machinery: task payload is not an acceptable envelope")

// ErrNoEnvelopeArg is returned by [Envelope] when a signature carries no string
// envelope argument (so it was not produced by this adapter).
var ErrNoEnvelopeArg = errors.New("babelqueue/machinery: signature has no envelope argument")

// Sender is the subset of *RichardKnop/machinery/v2.Server the [Producer] uses.
// The concrete server satisfies it, and a fake satisfies it in tests.
type Sender interface {
	SendTaskWithContext(ctx context.Context, signature *tasks.Signature) (*result.AsyncResult, error)
}

// Registrar is the subset of *RichardKnop/machinery/v2.Server that [Register] needs.
type Registrar interface {
	RegisterTask(name string, taskFunc interface{}) error
}

// Signature builds a machinery task signature from an already-built envelope:
// Name = the URN (env.Job) and a single string Arg = the canonical envelope JSON.
// Set advanced machinery fields (RetryCount, ETA for delayed delivery, RoutingKey)
// on the returned signature before sending. It returns [babelqueue.ErrEmptyURN]
// when the envelope has no URN, and surfaces an encode error otherwise.
func Signature(env babelqueue.Envelope) (*tasks.Signature, error) {
	if env.URN() == "" {
		return nil, babelqueue.ErrEmptyURN
	}
	body, err := env.Encode()
	if err != nil {
		return nil, err
	}
	return &tasks.Signature{
		Name: env.URN(),
		Args: []tasks.Arg{{Name: envelopeArgName, Type: "string", Value: string(body)}},
	}, nil
}

// SignatureFor builds the canonical envelope for (urn, data) via [babelqueue.Make]
// and wraps it as a machinery signature (see [Signature]). It mints a fresh trace
// id unless [babelqueue.WithTraceID] is given.
func SignatureFor(urn string, data map[string]any, opts ...babelqueue.Option) (*tasks.Signature, error) {
	env, err := babelqueue.Make(urn, data, opts...)
	if err != nil {
		return nil, err
	}
	return Signature(env)
}

// Envelope decodes a machinery signature's envelope argument into a BabelQueue
// envelope, resolving the urn alias (see [babelqueue.Decode]). The signature Name
// is the URN, but the authoritative identity is the decoded body.
func Envelope(sig *tasks.Signature) (babelqueue.Envelope, error) {
	if sig == nil || len(sig.Args) == 0 {
		return babelqueue.Envelope{}, ErrNoEnvelopeArg
	}
	body, ok := sig.Args[0].Value.(string)
	if !ok {
		return babelqueue.Envelope{}, ErrNoEnvelopeArg
	}
	return babelqueue.Decode([]byte(body))
}

// Producer publishes BabelQueue envelopes as machinery tasks over a [Sender].
type Producer struct {
	server Sender
}

// NewProducer wraps a [Sender] (a *machinery/v2.Server, or a fake in tests).
func NewProducer(server Sender) *Producer {
	return &Producer{server: server}
}

// Dispatch builds the canonical envelope for (urn, data) and sends it as a
// machinery task routed by URN, returning machinery's AsyncResult. Pass envelope
// options ([babelqueue.WithQueue], [babelqueue.WithTraceID]); meta.queue records
// the logical BabelQueue queue, which need not equal machinery's broker queue.
func (p *Producer) Dispatch(ctx context.Context, urn string, data map[string]any, opts ...babelqueue.Option) (*result.AsyncResult, error) {
	sig, err := SignatureFor(urn, data, opts...)
	if err != nil {
		return nil, err
	}
	return p.server.SendTaskWithContext(ctx, sig)
}

// Send sends an already-built envelope as a machinery task (see [Signature]).
func (p *Producer) Send(ctx context.Context, env babelqueue.Envelope) (*result.AsyncResult, error) {
	sig, err := Signature(env)
	if err != nil {
		return nil, err
	}
	return p.server.SendTaskWithContext(ctx, sig)
}

// Bind adapts a BabelQueue [babelqueue.Handler] to a machinery task function:
// machinery invokes it with the injected context and the envelope-JSON argument;
// it decodes the envelope, rejects it with [ErrNotAccepted] when it fails
// [babelqueue.Envelope.Accepts] (so machinery applies its own retry policy), and
// otherwise invokes handler. Use [Register] to wire it under the URN.
func Bind(handler babelqueue.Handler) func(ctx context.Context, body string) error {
	return func(ctx context.Context, body string) error {
		env, err := babelqueue.Decode([]byte(body))
		if err != nil {
			return err
		}
		if !env.Accepts() {
			return ErrNotAccepted
		}
		return handler(ctx, env)
	}
}

// Register wires handler onto the server for the given URN: it registers
// [Bind](handler) under the URN as the machinery task name, so machinery routes
// tasks whose Name equals the URN to it — the machinery-native equivalent of
// BabelQueue's URN routing.
func Register(server Registrar, urn string, handler babelqueue.Handler) error {
	return server.RegisterTask(urn, Bind(handler))
}
