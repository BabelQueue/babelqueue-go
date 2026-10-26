package babelqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"strings"
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
	// deadLetterSuffixSet records an explicit WithDeadLetterSuffix, which a transport's
	// DeadLetterNamer receives instead of choosing its own default.
	deadLetterSuffixSet bool
	pollTimeout         time.Duration
	retryBackoff        time.Duration
	retryMultiplier     float64
	retryBackoffMax     time.Duration
	retryJitter         RetryJitter
	unknownURNDelay     time.Duration
	onAckError          func(ctx context.Context, msg *ReceivedMessage, err error)
	onReleaseError      func(ctx context.Context, msg *ReceivedMessage, err error)
	onDeadLetterErr     func(ctx context.Context, msg *ReceivedMessage, err error)
	rewindPause         time.Duration
	handlers            map[string]Handler
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
// dead-letter queue name (default ".dlq"; on Amazon SQS the transport's [DeadLetterNamer] gives
// "<queue>-dlq", FIFO "<queue>-dlq.fifo", and places an explicit suffix before ".fifo" too).
func WithDeadLetterSuffix(suffix string) AppOption {
	return func(a *App) { a.deadLetterSuffix, a.deadLetterSuffixSet = suffix, true }
}

// WithPollTimeout sets how long Pop blocks waiting for a message each iteration
// (default 1s). Ignored by transports that do not block.
func WithPollTimeout(d time.Duration) AppOption { return func(a *App) { a.pollTimeout = d } }

// WithRetryBackoff sets how long a failed message waits before it is redelivered
// (default 0 = immediately). It applies to transports implementing [Releaser]
// (e.g. SQS, where it becomes the ChangeMessageVisibility timeout); transports
// without that capability re-publish the retry immediately.
func WithRetryBackoff(d time.Duration) AppOption { return func(a *App) { a.retryBackoff = d } }

// RetryJitter selects how the computed retry delay is randomised (ADR-0034 §9).
type RetryJitter string

const (
	// JitterNone applies the computed delay exactly (the default).
	JitterNone RetryJitter = "none"
	// JitterFull draws the delay uniformly from [0, d(n)].
	JitterFull RetryJitter = "full"
)

// WithRetryBackoffMultiplier sets retry_backoff_multiplier: the growth factor between
// consecutive retries (default 1 = constant delay). Values below 1 are ignored.
func WithRetryBackoffMultiplier(m float64) AppOption {
	return func(a *App) {
		if m >= 1 {
			a.retryMultiplier = m
		}
	}
}

// WithRetryBackoffMax sets retry_backoff_max: the cap on a computed retry delay
// (default 0 = unlimited).
func WithRetryBackoffMax(d time.Duration) AppOption {
	return func(a *App) {
		if d >= 0 {
			a.retryBackoffMax = d
		}
	}
}

// WithRetryJitter sets retry_jitter: [JitterNone] (default) or [JitterFull]. Any other
// value is ignored.
func WithRetryJitter(j RetryJitter) AppOption {
	return func(a *App) {
		if j == JitterNone || j == JitterFull {
			a.retryJitter = j
		}
	}
}

// retryDelay computes the worker-side retry delay for the n-th failure (n = attempts + 1,
// 1-based): d(n) = min(max, base * multiplier^(n-1)); with full jitter the result is drawn
// uniformly from [0, d(n)]. rnd returns a value in [0, 1). The defaults (base 0) yield 0, so
// an unconfigured App keeps its historical behaviour.
func retryDelay(base time.Duration, multiplier float64, max time.Duration, jitter RetryJitter, n int, rnd func() float64) time.Duration {
	d := computeRetryDelay(base, multiplier, max, n)
	if jitter == JitterFull && d > 0 {
		return time.Duration(rnd() * float64(d))
	}
	return d
}

func computeRetryDelay(base time.Duration, multiplier float64, max time.Duration, n int) time.Duration {
	if base <= 0 {
		return 0
	}
	if multiplier < 1 {
		multiplier = 1
	}
	if n < 1 {
		n = 1
	}
	d := float64(base) * math.Pow(multiplier, float64(n-1))
	if max > 0 && d > float64(max) {
		return max
	}
	if math.IsInf(d, 0) || math.IsNaN(d) || d >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(d)
}

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
// It also observes the re-publish that answers that signal (a retry or an unknown-URN release
// on a transport without in-place release): when the re-publish fails the message is not
// acknowledged but handed back to the broker (see [Rewinder]; without it the message stays
// reserved and is redelivered per the transport's reservation semantics, e.g. a Redis message
// stays in its processing list until recovered), and the failure is reported here. Without a
// handler a release error is ignored and a re-publish error is only logged.
func WithReleaseErrorHandler(fn func(ctx context.Context, msg *ReceivedMessage, err error)) AppOption {
	return func(a *App) { a.onReleaseError = fn }
}

// WithDeadLetterErrorHandler registers fn to observe a failed dead-letter write (the publish to
// the DLQ, or the record's encoding, failed). It fires on three paths: a version-gated delivery,
// an undecodable (poison) body, and a message whose retries are exhausted (or that hit
// [StrategyDeadLetter] for an unknown URN). Such a message is never acknowledged: the App logs the
// failure, reports it here and hands the message back to the broker (see [Rewinder]). The gate and
// poison paths never run a handler; an exhausted message is delivered again, so its handler runs
// again until the DLQ write succeeds. Without a handler the error is only logged.
func WithDeadLetterErrorHandler(fn func(ctx context.Context, msg *ReceivedMessage, err error)) AppOption {
	return func(a *App) { a.onDeadLetterErr = fn }
}

// WithRewindPause sets how long the App waits after handing a message whose dead-letter write
// failed (version-gated, poison, or retries exhausted) back to the broker before polling again
// (default 1s), so a persistent dead-letter outage does not spin.
func WithRewindPause(d time.Duration) AppOption { return func(a *App) { a.rewindPause = d } }

// NewApp builds a runtime over the given transport.
func NewApp(transport Transport, opts ...AppOption) *App {
	app := &App{
		transport:        transport,
		queue:            "default",
		onUnknownURN:     StrategyFail,
		maxAttempts:      3,
		deadLetterSuffix: ".dlq",
		pollTimeout:      time.Second,
		rewindPause:      time.Second,
		retryMultiplier:  1,
		retryJitter:      JitterNone,
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
	// A version-gated delivery (a transport's reserved unsupported-schema-version route, see
	// [unsupportedSchemaRoutePrefix]) can never be handled by any registered handler, and a retry
	// cannot change that: it is dead-lettered directly, with no retry, whatever its body looks
	// like. It is checked before the body is decoded: a gated body is never parsed, and the DLQ record
	// is the raw body (see settleVersionGated).
	if strings.HasPrefix(msg.RouteURN, unsupportedSchemaRoutePrefix) {
		a.settleVersionGated(ctx, msg)
		return
	}
	// A delivery the transport itself classified as poison (the Redis zset reserve script,
	// broker-bindings §1.1) is dead-lettered with its body untouched, whatever Decode would
	// make of it: a body with a null or missing attempts decodes without error but is still
	// poison, and no handler or retry may run for it.
	if msg.Poison {
		a.settleTransportPoison(ctx, msg)
		return
	}
	env, decodeErr := Decode([]byte(msg.Body))
	// ADR-0032 §6: a body with no valid job or urn alias fails decode and is poison. The one
	// exception is a delivery whose transport supplied the identity out of band (RouteURN, e.g.
	// the pulsar bq-job property): the body is not rewritten and the routing override names
	// the handler, so only that identity-less case is let through.
	noIdentity := errors.Is(decodeErr, ErrNoIdentity)
	if noIdentity && msg.RouteURN != "" {
		decodeErr = nil
	}
	// The broker's own delivery count is a floor for attempts: an in-place release
	// does not rewrite the body, and an undecodable body carries no counter at all,
	// so without it such a message would never reach WithMaxAttempts.
	if native := msg.DeliveryCount - 1; native > env.Attempts {
		env.Attempts = native
	}
	urn := env.URN()
	if decodeErr == nil && msg.RouteURN != "" {
		urn = msg.RouteURN // transport-supplied routing override; the body is not rewritten
	}

	// Non-fatal decode findings (a dropped forbidden key, ADR-0031 (c)) are logged,
	// never fatal, and before the poison check: a message about to be dead-lettered
	// is the one whose producer most needs to hear about them.
	if decodeErr == nil || noIdentity {
		for _, w := range env.Warnings() {
			slog.WarnContext(ctx, "babelqueue: decode warning", "warning", w, "queue", msg.Queue, "route", msg.RouteURN)
		}
	}
	if decodeErr != nil || isBlankASCII(urn) {
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

// unsupportedSchemaRoutePrefix is the reserved [ReceivedMessage.RouteURN] prefix a transport
// uses to hand over a delivery it version-gated (broker-bindings §5.7 step 3, e.g. the pulsar
// module's bq-schema-version check). It is not a "urn:babel:" URN, so no handler owns it. The
// App dead-letters such a delivery directly, with no retry (see settleVersionGated). The string
// is the contract between the transport and this package; it never reaches the wire.
const unsupportedSchemaRoutePrefix = "babelqueue:unsupported-schema-version:"

// declaredVersionMaxChars is the longest rejected bq-schema-version value kept for logs, the
// rejection hook, the reserved route and the dead-letter reason (same cap as the other SDKs).
const declaredVersionMaxChars = 64

// TruncateDeclaredVersion caps a rejected bq-schema-version value at 64 characters (runes)
// followed by "...", so a huge header cannot inflate a log line, a route or a DLQ record. A
// value of at most 64 characters is returned unchanged. Transports apply it to the rejected
// value before reporting it or building the reserved route; the gate comparison itself always
// uses the full, untrimmed value.
func TruncateDeclaredVersion(v string) string {
	n := 0
	for i := range v {
		if n == declaredVersionMaxChars {
			return v[:i] + "..."
		}
		n++
	}
	return v
}

// settleVersionGated settles a version-gated delivery (RouteURN under
// [unsupportedSchemaRoutePrefix]) without retrying it and without ever running a handler. It is
// acknowledged (dropped) under [StrategyDelete], dead-lettered when dead-lettering is on
// (WithDeadLetter, or [StrategyDeadLetter]), and otherwise left to the broker's native mechanism
// when the transport has the [NativeSettler] capability (SQS, Azure Service Bus: ADR-0037 decision
// 1 rule 3) or acknowledged (dropped) when it has not. The DLQ record
// is the raw body, byte-identical to the incoming one (never decoded, no dead_letter block). This
// holds for [StrategyFail] and [StrategyRelease] too, and for every transport that gates on a
// version property.
//
// A failed dead-letter write never loses the message: it is NOT acknowledged. The failure is
// logged and reported to the [WithDeadLetterErrorHandler] hook, and the message is handed back
// to the broker ([Rewinder] when the transport has it; otherwise it stays reserved and the broker
// redelivers it when the reservation lapses). The gate rejects the redelivery again, so the
// dead-letter write is retried; an offset-committing broker (Kafka) is rewound so a later commit
// can never move past this message.
func (a *App) settleVersionGated(ctx context.Context, msg *ReceivedMessage) {
	if a.onUnknownURN == StrategyDelete {
		a.ack(ctx, msg)
		return
	}
	if !a.deadLetter && a.onUnknownURN != StrategyDeadLetter {
		// Rule 3 (broker-bindings §5.7): no DLQ to write. A transport with a native mechanism
		// (SQS, Azure Service Bus) leaves the message to it instead of dropping it; any other
		// transport acknowledges it and logs.
		if a.settleNative(ctx, msg, "unsupported schema version",
			"the message declares a schema version this consumer does not support") {
			return
		}
		a.ack(ctx, msg)
		return
	}
	// §3.7/§4.7/§5.7/§6.7/§7.7: reject WITHOUT decoding. The DLQ record is the raw body exactly as
	// it arrived: an unknown schema is never best-effort parsed or re-encoded, so no dead_letter
	// block is added and the bytes stay identical to the incoming body (Java/.NET/Node parity).
	settled, err := a.deadLetterRaw(ctx, msg, msg.Body)
	if err != nil {
		a.leaveDeadLetterUnsettled(ctx, msg, err, "a version-gated message")
		return
	}
	a.settle(ctx, msg, settled)
}

// settleNative hands msg to the transport's [NativeSettler] capability and reports whether it did:
// false means the transport has none and the caller keeps its previous settle. The event is logged
// (never retried by the SDK); a failure of the call is logged too and leaves the message
// unacknowledged, exactly like the native settle itself, so the broker redelivers it.
func (a *App) settleNative(ctx context.Context, msg *ReceivedMessage, reason, description string) bool {
	ns, ok := a.transport.(NativeSettler)
	if !ok {
		return false
	}
	slog.WarnContext(ctx, "babelqueue: left to the broker's native mechanism (not acknowledged, not retried)",
		"queue", msg.Queue, "route", msg.RouteURN, "reason", reason)
	if err := ns.SettleNative(ctx, msg, reason, description); err != nil {
		slog.ErrorContext(ctx, "babelqueue: native settle failed; the message stays unacknowledged and will be redelivered",
			"queue", msg.Queue, "reason", reason, "error", err)
	}
	return true
}

// settleTransportPoison dead-letters a delivery the transport flagged [ReceivedMessage.Poison]:
// the DLQ record follows ADR-0035 (broker-bindings §1.1/§1.3) exactly like an undecodable body
// ([App.settleRawPoison]): the body is spliced or wrapped and carries a dead_letter block with
// reason "poison", and the reservation is dropped, in every configuration. The body is never
// decoded for routing. A failed write leaves it unacknowledged and handed back to the broker, like
// any other dead-letter failure.
func (a *App) settleTransportPoison(ctx context.Context, msg *ReceivedMessage) {
	// The decode error (when there is one) only documents why the body is poison; a body that
	// decodes yet was flagged by the transport (null attempts, cjson failure) has no cause.
	_, decodeErr := Decode([]byte(msg.Body))
	if errors.Is(decodeErr, ErrNoIdentity) {
		decodeErr = nil
	}
	a.settleRawPoison(ctx, msg, decodeErr)
}

// deadLetterRaw writes body to msg's dead-letter queue. A transport with the [DeadLetterer]
// capability does it as one gated step that also settles msg (settled is true: the caller must
// not ack); otherwise the body is re-published and the caller acks.
func (a *App) deadLetterRaw(ctx context.Context, msg *ReceivedMessage, body string) (settled bool, err error) {
	if dl, ok := a.transport.(DeadLetterer); ok {
		return true, dl.DeadLetter(ctx, msg, a.deadLetterTarget(msg), body)
	}
	return false, a.republish(ctx, msg, a.deadLetterTarget(msg), body)
}

// settle acknowledges msg unless a gated operation already settled it.
func (a *App) settle(ctx context.Context, msg *ReceivedMessage, settled bool) {
	if !settled {
		a.ack(ctx, msg)
	}
}

// leaveDeadLetterUnsettled reports a failed dead-letter write of subject (e.g. a version-gated
// or poison message) and hands the unacknowledged message back to the broker. The message is
// settled first (Rewind) and the failure is reported to the [WithDeadLetterErrorHandler] hook
// afterwards, so a misbehaving hook can never keep the message from being handed back; a panic
// in the hook is recovered and logged.
func (a *App) leaveDeadLetterUnsettled(ctx context.Context, msg *ReceivedMessage, err error, subject string) {
	a.leaveUnsettled(ctx, msg, err,
		"babelqueue: dead-lettering "+subject+" failed; it was not acknowledged and will be redelivered",
		"babelqueue: rewinding an unsettled dead-letter candidate failed",
		a.reportDeadLetterError)
}

// leaveRepublishUnsettled is the retry/release counterpart of leaveDeadLetterUnsettled: the
// re-publish that was to replace msg failed, so acknowledging it would lose the message. It is
// not acknowledged, handed back to the broker and the failure is reported to the
// [WithReleaseErrorHandler] hook.
func (a *App) leaveRepublishUnsettled(ctx context.Context, msg *ReceivedMessage, err error, subject string) {
	a.leaveUnsettled(ctx, msg, err,
		"babelqueue: re-publishing "+subject+" failed; it was not acknowledged and will be redelivered",
		"babelqueue: rewinding an unsettled re-publish candidate failed",
		a.reportReleaseError)
}

// leaveUnsettled logs err, hands the unacknowledged msg back to the broker ([Rewinder] when the
// transport has it; otherwise it stays reserved and is redelivered when the reservation lapses)
// and then reports err (and a failed rewind) through report.
func (a *App) leaveUnsettled(ctx context.Context, msg *ReceivedMessage, err error, failMsg, rewindFailMsg string,
	report func(context.Context, *ReceivedMessage, error)) {
	slog.ErrorContext(ctx, failMsg, "queue", msg.Queue, "route", msg.RouteURN, "error", err)
	var rewindErr error
	rw, canRewind := a.transport.(Rewinder)
	if canRewind {
		if rewindErr = rw.Rewind(ctx, msg); rewindErr != nil {
			slog.ErrorContext(ctx, rewindFailMsg, "queue", msg.Queue, "error", rewindErr)
		}
	}
	report(ctx, msg, err)
	if rewindErr != nil {
		report(ctx, msg, rewindErr)
		return
	}
	if canRewind && a.rewindPause > 0 {
		select {
		case <-ctx.Done():
		case <-time.After(a.rewindPause):
		}
	}
}

// reportReleaseError calls the [WithReleaseErrorHandler] hook, recovering and logging a panic so
// the consume loop carries on.
func (a *App) reportReleaseError(ctx context.Context, msg *ReceivedMessage, err error) {
	if a.onReleaseError == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "babelqueue: release error handler panicked; ignored",
				"queue", msg.Queue, "panic", fmt.Sprint(r))
		}
	}()
	a.onReleaseError(ctx, msg, err)
}

// reportDeadLetterError calls the [WithDeadLetterErrorHandler] hook, recovering and logging a
// panic so the consume loop carries on.
func (a *App) reportDeadLetterError(ctx context.Context, msg *ReceivedMessage, err error) {
	if a.onDeadLetterErr == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "babelqueue: dead-letter error handler panicked; ignored",
				"queue", msg.Queue, "panic", fmt.Sprint(r))
		}
	}()
	a.onDeadLetterErr(ctx, msg, err)
}

// routePoison handles a body that does not decode or names no URN. No handler can ever be
// registered for it, so the unknown-URN strategy applies except [StrategyRelease]: releasing
// would redeliver it forever, so it takes the bounded [StrategyFail] path instead (retries up
// to WithMaxAttempts, floored by the broker's delivery count, then dead-letter or drop).
//
// An undecodable body (decodeErr != nil) is the exception to every retrying path: when
// dead-lettering is on (WithDeadLetter, or [StrategyDeadLetter]) it is never retried — a retry
// could only re-publish an empty envelope and lose the body — and it is written to the DLQ
// wrapped with its original body preserved (see settleRawPoison). [StrategyDelete] still drops
// it. With dead-lettering off the strategy paths below apply, except on a transport with the
// [NativeSettler] capability (SQS, Azure Service Bus), where the body is settled natively on the
// first delivery instead (see below).
func (a *App) routePoison(ctx context.Context, msg *ReceivedMessage, env Envelope, decodeErr error) {
	if decodeErr != nil && a.onUnknownURN != StrategyDelete && (a.deadLetter || a.onUnknownURN == StrategyDeadLetter) {
		a.settleRawPoison(ctx, msg, decodeErr)
		return
	}
	// Dead-lettering off (and not the writable dead_letter strategy), strategy not delete: on a
	// transport with a native mechanism (SQS, Azure Service Bus: the [NativeSettler] capability) the
	// poison body is settled natively on the first delivery - no bounded retry, no empty-envelope
	// re-publish, body untouched (ADR-0037 decision 2, "Go and Python"). Every other transport
	// keeps the bounded path below.
	if a.onUnknownURN != StrategyDelete && !a.deadLetter && a.onUnknownURN != StrategyDeadLetter &&
		a.settleNative(ctx, msg, "poison", "the body does not decode into a valid envelope") {
		return
	}
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

// settleRawPoison dead-letters an undecodable body without retrying it. The DLQ record follows
// ADR-0035 (error-handling §6): a body that is a strict JSON object without a top-level
// dead_letter member is spliced byte for byte, anything else is wrapped as
// {"raw": <string>, "raw_encoding"?: "base64"}, then annotated with a dead_letter block (reason
// "poison"); see poisonDeadLetterBody. It is acknowledged only after the DLQ write succeeded;
// otherwise it is left unacknowledged and handed back to the broker, the same way a failed
// version-gate dead-letter write is (see leaveDeadLetterUnsettled).
func (a *App) settleRawPoison(ctx context.Context, msg *ReceivedMessage, decodeErr error) {
	body, err := poisonDeadLetterBody(msg, decodeErr)
	settled := false
	if err == nil {
		settled, err = a.deadLetterRaw(ctx, msg, string(body))
	}
	if err != nil {
		a.leaveDeadLetterUnsettled(ctx, msg, err, "an undecodable message")
		return
	}
	a.settle(ctx, msg, settled)
}

// poisonDeadLetterBody builds the DLQ record for an undecodable body (ADR-0035): the body is
// spliced when it is a strict JSON object, otherwise carried in "raw" (plain text when it is valid
// UTF-8, standard base64 plus "raw_encoding": "base64" when it is not — never U+FFFD substitution).
// The dead_letter block is the one [Annotate] writes, with the poison reason and the decode error
// as its cause.
func poisonDeadLetterBody(msg *ReceivedMessage, decodeErr error) ([]byte, error) {
	attempts := msg.DeliveryCount - 1
	if attempts < 0 {
		attempts = 0
	}
	var dl bytes.Buffer
	enc := json.NewEncoder(&dl)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(Annotate(Envelope{}, "poison", msg.Queue, attempts, decodeErr).DeadLetter); err != nil {
		return nil, err
	}
	return buildPoisonRecord(msg.Body, bytes.TrimRight(dl.Bytes(), "\n")), nil
}

func (a *App) routeUnknown(ctx context.Context, urn string, msg *ReceivedMessage, env Envelope) {
	switch a.onUnknownURN {
	case StrategyDelete:
		a.ack(ctx, msg)
	case StrategyRelease:
		if a.release(ctx, msg, a.unknownURNDelay) {
			return
		}
		if err := a.republish(ctx, msg, msg.Queue, msg.Body); err != nil {
			a.leaveRepublishUnsettled(ctx, msg, err, "an unknown-URN message")
			return
		}
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
		if a.release(ctx, msg, retryDelay(a.retryBackoff, a.retryMultiplier, a.retryBackoffMax, a.retryJitter, env.Attempts, rand.Float64)) {
			return
		}
		body, err := env.Encode()
		if err == nil {
			err = a.republish(ctx, msg, msg.Queue, string(body))
		}
		if err != nil {
			a.leaveRepublishUnsettled(ctx, msg, err, "a message for retry")
			return
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

// republish puts body on queue on behalf of msg: a retry or release (queue is msg's own) or the
// dead-letter record (queue is the DLQ). When the delivery was routed on a URN taken from
// the transport's routing property ([ReceivedMessage.RouteURN]) and the transport implements
// [RoutedPublisher], that URN is carried onto the re-published message, so the route stays
// constant across retries; a transport implementing [KeyedRepublisher] (Kafka: the record key)
// takes over the whole re-publish instead; otherwise it is a plain Publish.
func (a *App) republish(ctx context.Context, msg *ReceivedMessage, queue, body string) error {
	if kr, ok := a.transport.(KeyedRepublisher); ok {
		return kr.PublishFrom(ctx, queue, body, msg)
	}
	return publishKeepingRoute(ctx, a.transport, queue, body, msg.RouteURN)
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
	settled, err := a.writeDeadLetter(ctx, msg, env, reason, cause)
	if err != nil {
		// Never ack a message whose dead-letter record was not written.
		subject := "a failed message"
		if reason == "unknown_urn" {
			subject = "an unknown-URN message"
		}
		a.leaveDeadLetterUnsettled(ctx, msg, err, subject)
		return
	}
	a.settle(ctx, msg, settled)
}

// deadLetterTarget is the queue msg's dead-letter record is written to.
func (a *App) deadLetterTarget(msg *ReceivedMessage) string {
	if a.deadLetterQueue != "" {
		return a.deadLetterQueue
	}
	if n, ok := a.transport.(DeadLetterNamer); ok {
		suffix := ""
		if a.deadLetterSuffixSet {
			suffix = a.deadLetterSuffix
		}
		return n.DeadLetterQueueName(msg.Queue, suffix)
	}
	return msg.Queue + a.deadLetterSuffix
}

// writeDeadLetter encodes the annotated dead-letter record and writes it to the DLQ
// ([App.deadLetterRaw]). settled reports that the write also settled msg; otherwise the caller
// acks. The caller decides what a failure means.
func (a *App) writeDeadLetter(ctx context.Context, msg *ReceivedMessage, env Envelope, reason string, cause error) (settled bool, err error) {
	originalQueue := msg.Queue
	if env.Meta.Queue != "" {
		originalQueue = env.Meta.Queue
	}
	annotated := Annotate(env, reason, originalQueue, env.Attempts, cause)

	body, err := annotated.Encode()
	if err != nil {
		return false, err
	}
	return a.deadLetterRaw(ctx, msg, string(body))
}
