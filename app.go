package babelqueue

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Handler processes one decoded message. Returning an error triggers the retry /
// dead-letter path; returning nil acknowledges the message.
type Handler func(ctx context.Context, env Envelope) error

// App is the optional BabelQueue runtime: it produces and consumes polyglot
// messages over a [Transport]. Routing is by URN; the wire format is the canonical
// envelope (via the same core codec), so it interoperates with the PHP/Laravel,
// Python, ... SDKs. Retry uses the top-level attempts counter; failures past
// MaxAttempts go to a dead-letter queue when enabled.
//
// The core codec ([Make]/[Encode]/[Decode]) has zero dependencies; the App adds
// no dependencies either (broker drivers live in separate modules). An App is safe
// for concurrent Publish calls; run Consume from a single goroutine per queue.
type App struct {
	transport        Transport
	queue            string
	onUnknownURN     string
	maxAttempts      int
	deadLetter       bool
	deadLetterQueue  string
	deadLetterSuffix string
	pollTimeout      time.Duration
	handlers         map[string]Handler
}

// AppOption customizes NewApp.
type AppOption func(*App)

// WithDefaultQueue sets the queue used by Publish/Consume when none is given
// (default "default").
func WithDefaultQueue(queue string) AppOption { return func(a *App) { a.queue = queue } }

// WithMaxAttempts sets how many delivery attempts a message gets before it is
// dead-lettered or dropped (default 3).
func WithMaxAttempts(n int) AppOption { return func(a *App) { a.maxAttempts = n } }

// WithUnknownURNStrategy sets what happens to a message whose URN has no handler:
// one of [StrategyFail], [StrategyDelete], [StrategyRelease], [StrategyDeadLetter].
func WithUnknownURNStrategy(strategy string) AppOption {
	return func(a *App) { a.onUnknownURN = strategy }
}

// WithDeadLetter enables routing exhausted/failed messages to a dead-letter queue.
func WithDeadLetter(enabled bool) AppOption { return func(a *App) { a.deadLetter = enabled } }

// WithDeadLetterQueue overrides the dead-letter queue name (default: the source
// queue plus the dead-letter suffix).
func WithDeadLetterQueue(queue string) AppOption {
	return func(a *App) { a.deadLetterQueue = queue }
}

// WithDeadLetterSuffix sets the suffix appended to the source queue to derive the
// dead-letter queue name (default ".dlq").
func WithDeadLetterSuffix(suffix string) AppOption {
	return func(a *App) { a.deadLetterSuffix = suffix }
}

// WithPollTimeout sets how long Pop blocks waiting for a message each iteration
// (default 1s). Ignored by transports that do not block.
func WithPollTimeout(d time.Duration) AppOption { return func(a *App) { a.pollTimeout = d } }

// NewApp builds a runtime over the given transport.
func NewApp(transport Transport, opts ...AppOption) *App {
	app := &App{
		transport:        transport,
		queue:            "default",
		onUnknownURN:     StrategyFail,
		maxAttempts:      3,
		deadLetterSuffix: ".dlq",
		pollTimeout:      time.Second,
		handlers:         make(map[string]Handler),
	}
	for _, o := range opts {
		o(app)
	}
	return app
}

// Handle registers handler as the consumer for urn (the last registration wins).
func (a *App) Handle(urn string, handler Handler) {
	a.handlers[urn] = handler
}

// Publish builds the canonical envelope for (urn, data) and publishes it. It
// returns the message id (meta.id). Pass envelope options such as [WithQueue] or
// [WithTraceID] to override the target queue or continue a trace.
func (a *App) Publish(ctx context.Context, urn string, data map[string]any, opts ...Option) (string, error) {
	env, err := Make(urn, data, append([]Option{WithQueue(a.queue)}, opts...)...)
	if err != nil {
		return "", err
	}
	body, err := env.Encode()
	if err != nil {
		return "", err
	}
	if err := a.transport.Publish(ctx, env.Meta.Queue, string(body)); err != nil {
		return "", err
	}
	return env.Meta.ID, nil
}

// Consume processes messages from the default queue (or the optional override)
// until ctx is cancelled. It blocks; run it in its own goroutine. A single bad
// message never stops the loop — it is retried or dead-lettered.
func (a *App) Consume(ctx context.Context, queue ...string) error {
	target := a.queue
	if len(queue) > 0 && queue[0] != "" {
		target = queue[0]
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		msg, err := a.transport.Pop(ctx, target, a.pollTimeout)
		if err != nil {
			return err
		}
		if msg == nil {
			continue
		}
		a.dispatch(ctx, msg)
	}
}

// Run is an alias for Consume on the default queue.
func (a *App) Run(ctx context.Context) error { return a.Consume(ctx) }

// Drain processes up to max messages from queue and returns the count, stopping
// early when the queue is empty. With max <= 0 it drains until empty. Useful for
// tests and one-shot workers.
func (a *App) Drain(ctx context.Context, queue string, max int) (int, error) {
	if queue == "" {
		queue = a.queue
	}
	processed := 0
	for max <= 0 || processed < max {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		msg, err := a.transport.Pop(ctx, queue, a.pollTimeout)
		if err != nil {
			return processed, err
		}
		if msg == nil {
			break
		}
		a.dispatch(ctx, msg)
		processed++
	}
	return processed, nil
}

func (a *App) dispatch(ctx context.Context, msg *ReceivedMessage) {
	env, _ := Decode([]byte(msg.Body))
	urn := env.URN()

	handler, ok := a.handlers[urn]
	if !ok || urn == "" {
		a.routeUnknown(ctx, urn, msg, env)
		return
	}
	if err := handler(ctx, env); err != nil {
		a.retryOrDeadLetter(ctx, msg, env, err)
		return
	}
	_ = a.transport.Ack(ctx, msg)
}

func (a *App) routeUnknown(ctx context.Context, urn string, msg *ReceivedMessage, env Envelope) {
	switch a.onUnknownURN {
	case StrategyDelete:
		_ = a.transport.Ack(ctx, msg)
	case StrategyRelease:
		_ = a.transport.Publish(ctx, msg.Queue, msg.Body)
		_ = a.transport.Ack(ctx, msg)
	case StrategyDeadLetter:
		a.deadLetterMessage(ctx, msg, env, "unknown_urn", nil)
	default: // StrategyFail — surfaced through the retry/dead-letter path.
		a.retryOrDeadLetter(ctx, msg, env, fmt.Errorf("%w: %q", ErrUnknownURN, urn))
	}
}

func (a *App) retryOrDeadLetter(ctx context.Context, msg *ReceivedMessage, env Envelope, cause error) {
	env.Attempts++

	if env.Attempts < a.maxAttempts {
		if body, err := env.Encode(); err == nil {
			_ = a.transport.Publish(ctx, msg.Queue, string(body))
		}
		_ = a.transport.Ack(ctx, msg)
		return
	}

	if a.deadLetter {
		reason := "failed"
		if errors.Is(cause, ErrUnknownURN) {
			reason = "unknown_urn"
		}
		a.deadLetterMessage(ctx, msg, env, reason, cause)
		return
	}

	// Retries exhausted and no dead-letter configured — drop it (ack so it leaves
	// the queue).
	_ = a.transport.Ack(ctx, msg)
}

func (a *App) deadLetterMessage(ctx context.Context, msg *ReceivedMessage, env Envelope, reason string, cause error) {
	originalQueue := msg.Queue
	if env.Meta.Queue != "" {
		originalQueue = env.Meta.Queue
	}
	annotated := Annotate(env, reason, originalQueue, env.Attempts, cause)

	target := a.deadLetterQueue
	if target == "" {
		target = msg.Queue + a.deadLetterSuffix
	}
	if body, err := annotated.Encode(); err == nil {
		_ = a.transport.Publish(ctx, target, string(body))
	}
	_ = a.transport.Ack(ctx, msg)
}
