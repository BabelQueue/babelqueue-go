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
	retryBackoff     time.Duration
	unknownURNDelay  time.Duration
	onAckError       func(ctx context.Context, msg *ReceivedMessage, err error)
	onReleaseError   func(ctx context.Context, msg *ReceivedMessage, err error)
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

// WithRetryBackoff sets how long a failed message waits before it is redelivered
// (default 0 = immediately). It applies to transports implementing [Releaser]
// (e.g. SQS, where it becomes the ChangeMessageVisibility timeout); transports
// without that capability re-publish the retry immediately.
func WithRetryBackoff(d time.Duration) AppOption { return func(a *App) { a.retryBackoff = d } }

// WithUnknownURNReleaseDelay sets the redelivery delay for the [StrategyRelease]
// unknown-URN strategy — the contract's unknown_urn_release_delay (default 0). Like
// [WithRetryBackoff], it takes effect on transports implementing [Releaser].
func WithUnknownURNReleaseDelay(d time.Duration) AppOption {
	return func(a *App) { a.unknownURNDelay = d }
}

// WithAckErrorHandler registers fn to observe a failed acknowledgement (e.g. an SQS
// DeleteMessage error). An ack failure is reported distinctly and never treated as
// a handler failure: a message whose handler succeeded is not retried, released or
// dead-lettered because its ack failed — the broker redelivers it once its
// reservation lapses, which consumer-side idempotency absorbs. Without a handler
// the error is ignored.
func WithAckErrorHandler(fn func(ctx context.Context, msg *ReceivedMessage, err error)) AppOption {
	return func(a *App) { a.onAckError = fn }
}

// WithReleaseErrorHandler registers fn to observe a failed in-place release (e.g. an
// SQS ChangeMessageVisibility error such as AccessDenied). A failed release is
// still treated as handled: the message is neither re-published nor acked, and the
// broker redelivers it once its reservation lapses — so the retry waits for the
// queue's visibility timeout instead of [WithRetryBackoff]. [ErrReleaseUnsupported]
// is not reported (it is a capability signal, answered by re-publish + ack).
// Without a handler the error is ignored.
func WithReleaseErrorHandler(fn func(ctx context.Context, msg *ReceivedMessage, err error)) AppOption {
	return func(a *App) { a.onReleaseError = fn }
}

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

// PublishWithHeaders builds the canonical envelope for (urn, data) and publishes it
// together with out-of-band transport headers, returning the message id (meta.id).
// The headers ride beside the frozen envelope on the transport's per-message
// metadata channel (e.g. a W3C traceparent for cross-hop span linkage, ADR-0028) —
// the envelope itself is never touched (GR-1).
//
// It is the produce-side counterpart of the headers the runtime surfaces to a
// handler via [HeadersFromContext]. When the transport implements [HeaderPublisher]
// the headers are propagated; otherwise it transparently falls back to a plain
// publish (the headers are dropped, exactly as [Redrive] degrades — no error, no
// regression), so callers need not branch on transport capability. Passing nil or
// empty headers is equivalent to [App.Publish].
func (a *App) PublishWithHeaders(
	ctx context.Context,
	urn string,
	data map[string]any,
	headers map[string]string,
	opts ...Option,
) (string, error) {
	env, err := Make(urn, data, append([]Option{WithQueue(a.queue)}, opts...)...)
	if err != nil {
		return "", err
	}
	body, err := env.Encode()
	if err != nil {
		return "", err
	}
	if hp, ok := a.transport.(HeaderPublisher); ok && len(headers) > 0 {
		if err := hp.PublishWithHeaders(ctx, env.Meta.Queue, string(body), headers); err != nil {
			return "", err
		}
		return env.Meta.ID, nil
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
	ctx = withHeaders(ctx, msg.Headers)
	if msg.Headers[HeaderReplayBypass] != "" {
		ctx = withReplay(ctx)
	}
	env, decodeErr := Decode([]byte(msg.Body))
	// The broker's own delivery count is a floor for attempts: an in-place release
	// does not rewrite the body, and an undecodable body carries no counter at all,
	// so without it such a message would never reach WithMaxAttempts.
	if native := msg.DeliveryCount - 1; native > env.Attempts {
		env.Attempts = native
	}
	urn := env.URN()

	if decodeErr != nil || urn == "" {
		a.routePoison(ctx, msg, env, decodeErr)
		return
	}
	handler, ok := a.handlers[urn]
	if !ok {
		a.routeUnknown(ctx, urn, msg, env)
		return
	}
	if err := handler(ctx, env); err != nil {
		a.retryOrDeadLetter(ctx, msg, env, err)
		return
	}
	a.ack(ctx, msg)
}

// ack acknowledges msg and reports a failure to the [WithAckErrorHandler] hook. It
// never feeds the failure back into the retry path.
func (a *App) ack(ctx context.Context, msg *ReceivedMessage) {
	if err := a.transport.Ack(ctx, msg); err != nil && a.onAckError != nil {
		a.onAckError(ctx, msg, err)
	}
}

// routePoison handles a body that does not decode or names no URN. No handler can
// ever be registered for it, so the unknown-URN strategy applies except [StrategyRelease]:
// releasing would redeliver it forever, so it takes the bounded [StrategyFail] path
// instead (retries up to WithMaxAttempts, floored by the broker's delivery count,
// then dead-letter or drop).
func (a *App) routePoison(ctx context.Context, msg *ReceivedMessage, env Envelope, decodeErr error) {
	if a.onUnknownURN != StrategyRelease {
		a.routeUnknown(ctx, "", msg, env)
		return
	}
	cause := fmt.Errorf("%w: %q", ErrUnknownURN, "")
	if decodeErr != nil {
		cause = fmt.Errorf("%w: undecodable body: %v", ErrUnknownURN, decodeErr)
	}
	a.retryOrDeadLetter(ctx, msg, env, cause)
}

func (a *App) routeUnknown(ctx context.Context, urn string, msg *ReceivedMessage, env Envelope) {
	switch a.onUnknownURN {
	case StrategyDelete:
		a.ack(ctx, msg)
	case StrategyRelease:
		if a.release(ctx, msg, a.unknownURNDelay) {
			return
		}
		_ = a.transport.Publish(ctx, msg.Queue, msg.Body)
		a.ack(ctx, msg)
	case StrategyDeadLetter:
		a.deadLetterMessage(ctx, msg, env, "unknown_urn", nil)
	default: // StrategyFail — surfaced through the retry/dead-letter path.
		a.retryOrDeadLetter(ctx, msg, env, fmt.Errorf("%w: %q", ErrUnknownURN, urn))
	}
}

func (a *App) retryOrDeadLetter(ctx context.Context, msg *ReceivedMessage, env Envelope, cause error) {
	env.Attempts++

	if env.Attempts < a.maxAttempts {
		// A Releaser redelivers the original in place; the broker's own receive
		// count then carries the incremented attempt (no re-publish, no ack).
		if a.release(ctx, msg, a.retryBackoff) {
			return
		}
		if body, err := env.Encode(); err == nil {
			_ = a.transport.Publish(ctx, msg.Queue, string(body))
		}
		a.ack(ctx, msg)
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
	a.ack(ctx, msg)
}

// release hands msg back to its queue via the transport's [Releaser] capability.
// It reports false only when the transport lacks the capability or reports
// [ErrReleaseUnsupported] for this message, so the caller falls back to
// re-publish + ack. Any other Release failure is reported to the
// [WithReleaseErrorHandler] hook and still counts as handled: the message is neither
// re-published nor acked, and the broker redelivers it when its reservation lapses
// (contract §3: a release never deletes the message).
func (a *App) release(ctx context.Context, msg *ReceivedMessage, delay time.Duration) bool {
	r, ok := a.transport.(Releaser)
	if !ok {
		return false
	}
	err := r.Release(ctx, msg, delay)
	if err == nil {
		return true
	}
	if errors.Is(err, ErrReleaseUnsupported) {
		return false
	}
	if a.onReleaseError != nil {
		a.onReleaseError(ctx, msg, err)
	}
	return true
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
	a.ack(ctx, msg)
}
