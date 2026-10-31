// Package otel adds optional OpenTelemetry tracing and metrics to a babelqueue producer or
// consumer. It lives in its own module so the zero-dependency core never imports
// OpenTelemetry — wiring a TracerProvider (and a MeterProvider) is opt-in.
//
// Span shape (ADR-0038, semantic conventions v1.44.0): a PRODUCER span "send <queue>" and a
// CONSUMER span "process <queue>", with messaging.system set by [WithSystem] (the broker, e.g.
// "kafka"), messaging.operation.type / .name, the babelqueue.* identity attributes, and
// error.type on a failure. The URN is an attribute, never part of the span name.
//
// Metrics (ADR-0039): pass a meter with [WithMeter] and the wrappers record
// messaging.process.duration, messaging.client.sent.messages,
// messaging.client.consumed.messages and babelqueue.retries; [NewDeadLetterObserver] adds
// babelqueue.dlq.messages. Without a meter nothing is recorded.
//
// Cross-hop trace propagation works at two levels, layered:
//
//   - trace_id ↔ TraceID (ADR-0025, v0.1): the envelope's trace_id maps 1:1 to an OTel
//     TraceID, so every hop that shares a trace_id shares one trace — correlation and per-hop
//     timing with zero wire/transport change.
//   - W3C traceparent (ADR-0028, v0.2): the producer also injects the active span context as a
//     traceparent transport header (beside the frozen envelope, never in it), so the consumer
//     starts its span as a true child of the producer span — real cross-hop parent-child
//     linkage. This rides the out-of-band HeaderPublisher / ReceivedMessage.Headers seam
//     (ADR-0027) and is available on any transport that carries headers. When no traceparent
//     header is present it falls back to the v0.1 trace_id behaviour — a strict, backward-
//     compatible upgrade.
package otel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	// SemconvVersion is the OpenTelemetry semantic-conventions release this module targets
	// (ADR-0038 section 1).
	SemconvVersion = "1.44.0"

	// SchemaURL is the schema URL of [SemconvVersion]. This module does not create the tracer or
	// the meter, so create them with it:
	//
	//	tp.Tracer("my-app", trace.WithSchemaURL(otel.SchemaURL))
	//	mp.Meter("my-app", metric.WithSchemaURL(otel.SchemaURL))
	SchemaURL = "https://opentelemetry.io/schemas/" + SemconvVersion

	// defaultSystem is the messaging.system written when [WithSystem] is not given. It is
	// deprecated: pass the broker name (ADR-0038 section 5).
	defaultSystem = "babelqueue"

	// asciiWhitespace is the closed whitespace set of the conformance suite: a blank queue is
	// empty or only these (the core's rule, ADR-0032 section 5).
	asciiWhitespace = " \t\n\v\f\r"
)

// Option configures [WrapHandler], [NewPublisher] and [NewDeadLetterObserver].
type Option func(*config)

type config struct {
	system        string
	consumerGroup string
	meter         metric.Meter
}

// WithSystem sets messaging.system to the broker name (ADR-0038 section 3): "redis", "rabbitmq",
// "aws_sqs", "servicebus", "pulsar", "kafka" or "activemq". Any other value is written as given,
// unvalidated. An empty value counts as absent, which keeps the deprecated default "babelqueue".
func WithSystem(system string) Option { return func(c *config) { c.system = system } }

// WithConsumerGroup sets messaging.consumer.group.name on the consumer span (ADR-0038 section 4),
// intended for Kafka. It is written whenever given and non-empty; it is never a metric attribute.
func WithConsumerGroup(group string) Option { return func(c *config) { c.consumerGroup = group } }

// WithMeter turns on the standard metrics (ADR-0039): the instruments are created from m once
// per wrapper. Without it the wrappers record no metric. Create the meter with
// metric.WithSchemaURL([SchemaURL]).
func WithMeter(m metric.Meter) Option { return func(c *config) { c.meter = m } }

// warnDefaultSystem logs the deprecation of the implicit "babelqueue" system once per process.
var warnDefaultSystem sync.Once

func newConfig(opts []Option) *config {
	c := &config{}
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	if c.system == "" {
		c.system = defaultSystem
		warnDefaultSystem.Do(func() {
			slog.Warn("babelqueue otel: messaging.system defaults to \"babelqueue\"; this default is deprecated, " +
				"pass otel.WithSystem with the broker name (ADR-0038 section 5)")
		})
	}
	return c
}

// isBlank reports whether s is empty or only ASCII whitespace.
func isBlank(s string) bool { return strings.Trim(s, asciiWhitespace) == "" }

// errorType is the error.type value for err: its Go type name (ADR-0038 section 6), "_OTHER" when
// none can be obtained.
func errorType(err error) string {
	if t := fmt.Sprintf("%T", err); t != "" && t != "<nil>" {
		return t
	}
	return "_OTHER"
}

// recordFailure records err on span the ADR-0038 section 6 way: the error event, status Error and
// error.type.
func recordFailure(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	span.SetAttributes(attribute.String("error.type", errorType(err)))
}

// TraceIDOf maps an envelope trace_id to a deterministic OTel TraceID: a UUID maps to its
// 16 raw bytes; any other string is hashed (SHA-256, first 16 bytes). The inverse of
// [UUIDOf] for the UUID case.
func TraceIDOf(traceID string) trace.TraceID {
	t, _ := mapTraceID(traceID)
	return t
}

// mapTraceID is [TraceIDOf] that also reports whether the value had to be hashed (it is not a
// UUID, or it is the all-zero value, which is not a valid TraceID).
func mapTraceID(traceID string) (t trace.TraceID, hashed bool) {
	if raw, ok := uuidBytes(traceID); ok {
		copy(t[:], raw)
		if t.IsValid() {
			return t, false
		}
	}
	sum := sha256.Sum256([]byte(traceID))
	copy(t[:], sum[:])
	return t, true
}

// UUIDOf formats an OTel TraceID (16 bytes) as a canonical UUID string — the form a producer
// stamps into the message's trace_id so a consumer can recover the same TraceID.
func UUIDOf(t trace.TraceID) string {
	b := t[:]
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func uuidBytes(s string) ([]byte, bool) {
	h := strings.ReplaceAll(s, "-", "")
	if len(h) != 32 {
		return nil, false
	}
	raw, err := hex.DecodeString(h)
	if err != nil {
		return nil, false
	}
	return raw, true
}

// spanIDOf derives a deterministic, non-zero SpanID from the trace_id so the remote parent
// SpanContext is valid (a span needs a valid parent to inherit a specific trace).
func spanIDOf(traceID string) trace.SpanID {
	sum := sha256.Sum256([]byte("babelqueue-span:" + traceID))
	var s trace.SpanID
	copy(s[:], sum[:8])
	if !s.IsValid() {
		s[7] = 1
	}
	return s
}

// parentOf returns ctx carrying a remote parent SpanContext in the trace_id-derived trace,
// so a span started from it lands in that trace (cross-hop correlation).
func parentOf(ctx context.Context, traceID string) context.Context {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    TraceIDOf(traceID),
		SpanID:     spanIDOf(traceID),
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

// WrapHandler returns handler decorated to emit a CONSUMER span "process <queue>" per message
// (ADR-0038), recording the handler's error/status, and, when [WithMeter] is given, the consumer
// metrics (ADR-0039). Register it like any handler:
// app.Handle(urn, otel.WrapHandler(tracer, handler, otel.WithSystem("kafka"))).
//
// The span name and messaging.destination.name come from the delivered envelope's meta.queue;
// when it is absent or blank the span is just "process" and carries no destination. The options
// are additive: WrapHandler(tracer, handler) keeps working and writes the deprecated default
// messaging.system "babelqueue".
//
// Parent selection (ADR-0028 / ADR-0025 Option 2): when the producer carried a W3C
// traceparent on the transport (surfaced by the runtime via
// [babelqueue.HeadersFromContext]), the span is started as a true child of the producer's
// span — real cross-hop parent→child linkage with per-hop span timing. When no traceparent
// header is present, it falls back to the v0.1 behaviour: a remote parent derived from the
// envelope's trace_id (ADR-0025 Option 1), which shares the trace but not exact span linkage.
// So enabling traceparent propagation is a strict, backward-compatible upgrade — no regression
// for messages produced without it. A trace_id that is not a UUID is preserved untouched and
// mapped to a TraceID by hashing; that is logged as a warning, once per distinct value.
func WrapHandler(tracer trace.Tracer, handler babelqueue.Handler, opts ...Option) babelqueue.Handler {
	cfg := newConfig(opts)
	inst := newInstruments(cfg.meter)
	warned := &hashedTraceIDWarner{}
	return func(ctx context.Context, env babelqueue.Envelope) error {
		if remote, ok := remoteParentFromHeaders(ctx); ok {
			ctx = remote
		} else {
			warned.check(ctx, env.TraceID)
			ctx = parentOf(ctx, env.TraceID)
		}
		name := "process"
		if !isBlank(env.Meta.Queue) {
			name = "process " + env.Meta.Queue
		}
		ctx, span := tracer.Start(ctx, name,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(consumeAttrs(cfg, env)...),
		)
		defer span.End()

		inst.delivered(ctx, cfg.system, env)
		start := time.Now()
		err := handler(ctx, env)
		failure := ""
		if err != nil {
			recordFailure(span, err)
			failure = errorType(err)
		}
		inst.processed(ctx, cfg.system, env.Meta.Queue, time.Since(start), failure)
		return err
	}
}

// Publisher publishes through a [babelqueue.App] under a PRODUCER span "send <queue>"
// (ADR-0038) and, when [WithMeter] is given, counts the attempt (ADR-0039). Build it once and
// reuse it: the metric instruments are created per Publisher, not per message.
type Publisher struct {
	tracer trace.Tracer
	app    *babelqueue.App
	cfg    *config
	inst   *instruments
}

// NewPublisher returns a [Publisher] over app. The options are those of [WrapHandler]; the
// consumer-only [WithConsumerGroup] is accepted and ignored.
func NewPublisher(tracer trace.Tracer, app *babelqueue.App, opts ...Option) *Publisher {
	cfg := newConfig(opts)
	return &Publisher{tracer: tracer, app: app, cfg: cfg, inst: newInstruments(cfg.meter)}
}

// Publish starts a PRODUCER span "send <queue>" and publishes via app, propagating the trace
// to the downstream consumer two ways:
//
//   - It injects the active span context as a W3C traceparent (and tracestate) onto the
//     outgoing transport headers, so a consumer can start its span as a true child of this
//     producer span — real cross-hop parent→child linkage (ADR-0028 / ADR-0025 Option 2). The
//     header rides beside the frozen envelope, never in it (GR-1), via
//     [babelqueue.App.PublishEnvelopeWithHeaders]; if the transport can't carry headers it degrades to a
//     plain publish (the traceparent is simply not propagated — no error).
//   - It also carries the active trace's TraceID into the message's trace_id (the v0.1
//     behaviour, ADR-0025 Option 1), so even a consumer that ignores the header — or a transport
//     that drops it — still recovers the same trace (correlation without exact span linkage).
//
// The span starts as the bare "send": the queue is only known from the envelope the App builds
// (a per-call [babelqueue.WithQueue] or the app's default queue), so once the envelope has been
// handed to the transport the span is renamed "send <queue>" and gets
// messaging.destination.name, messaging.message.id and messaging.message.conversation_id.
// When no envelope is ever handed to the transport (the build or the encode failed) the span
// stays "send" and carries none of those, never a guessed destination.
//
// It otherwise behaves like [babelqueue.App.Publish], returning the message id.
func (p *Publisher) Publish(ctx context.Context, urn string, data map[string]any, opts ...babelqueue.Option) (string, error) {
	// Make trims the URN with the ASCII whitespace set; the span carries the same value as the
	// envelope's job, so the producer and consumer spans of one message group together.
	urn = strings.Trim(urn, asciiWhitespace)
	ctx, span := p.tracer.Start(ctx, "send",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", p.cfg.system),
			attribute.String("messaging.operation.type", "send"),
			attribute.String("messaging.operation.name", "send"),
			attribute.String("babelqueue.urn", urn),
			attribute.Int("babelqueue.schema_version", babelqueue.SchemaVersion),
			// Deprecated transition key, dual-emitted for one MINOR (ADR-0038 section 5).
			attribute.String("messaging.operation", "publish"),
		),
	)
	defer span.End()

	headers := injectTraceparent(ctx, nil)
	traceID := UUIDOf(span.SpanContext().TraceID())
	callOpts := make([]babelqueue.Option, 0, len(opts)+1)
	callOpts = append(callOpts, opts...)
	callOpts = append(callOpts, babelqueue.WithTraceID(traceID))
	env, err := p.app.PublishEnvelopeWithHeaders(ctx, urn, data, headers, callOpts...)

	destination := ""
	if env.Meta.ID != "" { // the envelope was handed to the transport
		if !isBlank(env.Meta.Queue) {
			destination = env.Meta.Queue
			span.SetName("send " + destination)
			span.SetAttributes(attribute.String("messaging.destination.name", destination))
		}
		span.SetAttributes(
			attribute.String("messaging.message.id", env.Meta.ID),
			attribute.String("messaging.message.conversation_id", env.TraceID),
		)
	}
	failure := ""
	if err != nil {
		recordFailure(span, err)
		failure = errorType(err)
	}
	p.inst.sent(ctx, p.cfg.system, destination, failure)
	if err != nil {
		return "", err
	}
	return env.Meta.ID, nil
}

// Publish is [Publisher.Publish] with the default options: the deprecated messaging.system
// "babelqueue" and no metrics. Use [NewPublisher] to pass [WithSystem] or [WithMeter].
func Publish(
	ctx context.Context,
	tracer trace.Tracer,
	app *babelqueue.App,
	urn string,
	data map[string]any,
	opts ...babelqueue.Option,
) (string, error) {
	return NewPublisher(tracer, app).Publish(ctx, urn, data, opts...)
}

func consumeAttrs(cfg *config, env babelqueue.Envelope) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", cfg.system),
		attribute.String("messaging.operation.type", "process"),
		attribute.String("messaging.operation.name", "process"),
		attribute.String("messaging.message.conversation_id", env.TraceID),
		attribute.String("babelqueue.urn", env.URN()),
		attribute.Int("babelqueue.schema_version", env.Meta.SchemaVersion),
		attribute.Int("babelqueue.attempts", env.Attempts),
		// Deprecated transition keys, dual-emitted for one MINOR (ADR-0038 section 5).
		attribute.String("messaging.operation", "process"),
		attribute.Int("messaging.babelqueue.attempts", env.Attempts),
	}
	if !isBlank(env.Meta.Queue) {
		attrs = append(attrs, attribute.String("messaging.destination.name", env.Meta.Queue))
	}
	if env.Meta.ID != "" {
		attrs = append(attrs, attribute.String("messaging.message.id", env.Meta.ID))
	}
	if cfg.consumerGroup != "" {
		attrs = append(attrs, attribute.String("messaging.consumer.group.name", cfg.consumerGroup))
	}
	return attrs
}

// hashedTraceIDWarner logs the ADR-0038 section 7 warning when a trace_id that is not a UUID is
// mapped to a TraceID by hashing. It warns once per distinct value and remembers at most
// maxWarnedTraceIDs values, so a stream of unique ids cannot grow it without bound.
type hashedTraceIDWarner struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

const (
	maxWarnedTraceIDs = 1024
	maxLoggedTraceID  = 64
)

func (w *hashedTraceIDWarner) check(ctx context.Context, traceID string) {
	if _, hashed := mapTraceID(traceID); !hashed {
		return
	}
	w.mu.Lock()
	if _, dup := w.seen[traceID]; dup {
		w.mu.Unlock()
		return
	}
	if w.seen == nil || len(w.seen) >= maxWarnedTraceIDs {
		w.seen = make(map[string]struct{})
	}
	w.seen[traceID] = struct{}{}
	w.mu.Unlock()

	logged := traceID
	if r := []rune(logged); len(r) > maxLoggedTraceID {
		logged = string(r[:maxLoggedTraceID]) + "..."
	}
	slog.WarnContext(ctx, "babelqueue otel: trace_id is not a UUID; it is kept as is and mapped to an OTel TraceID by hashing (ADR-0038 section 7)",
		"trace_id", logged)
}
