// Package otel adds optional OpenTelemetry tracing to a babelqueue producer or consumer. It
// lives in its own module so the zero-dependency core never imports OpenTelemetry — wiring a
// TracerProvider is opt-in.
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
	"strings"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const system = "babelqueue"

// TraceIDOf maps an envelope trace_id to a deterministic OTel TraceID: a UUID maps to its
// 16 raw bytes; any other string is hashed (SHA-256, first 16 bytes). The inverse of
// [UUIDOf] for the UUID case.
func TraceIDOf(traceID string) trace.TraceID {
	if raw, ok := uuidBytes(traceID); ok {
		var t trace.TraceID
		copy(t[:], raw)
		if t.IsValid() {
			return t
		}
	}
	sum := sha256.Sum256([]byte(traceID))
	var t trace.TraceID
	copy(t[:], sum[:])
	return t
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

// WrapHandler returns handler decorated to emit a CONSUMER span per message, recording the
// handler's error/status. Register it like any handler:
// app.Handle(urn, otel.WrapHandler(tracer, handler)).
//
// Parent selection (ADR-0028 / ADR-0025 Option 2): when the producer carried a W3C
// traceparent on the transport (surfaced by the runtime via
// [babelqueue.HeadersFromContext]), the span is started as a true child of the producer's
// span — real cross-hop parent→child linkage with per-hop span timing. When no traceparent
// header is present, it falls back to the v0.1 behaviour: a remote parent derived from the
// envelope's trace_id (ADR-0025 Option 1), which shares the trace but not exact span linkage.
// So enabling traceparent propagation is a strict, backward-compatible upgrade — no regression
// for messages produced without it.
func WrapHandler(tracer trace.Tracer, handler babelqueue.Handler) babelqueue.Handler {
	return func(ctx context.Context, env babelqueue.Envelope) error {
		if remote, ok := remoteParentFromHeaders(ctx); ok {
			ctx = remote
		} else {
			ctx = parentOf(ctx, env.TraceID)
		}
		ctx, span := tracer.Start(ctx, "process "+env.URN(),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(consumeAttrs(env)...),
		)
		defer span.End()

		err := handler(ctx, env)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
}

// Publish starts a PRODUCER span "publish <urn>" and publishes via app, propagating the trace
// to the downstream consumer two ways:
//
//   - It injects the active span context as a W3C traceparent (and tracestate) onto the
//     outgoing transport headers, so a consumer can start its span as a true child of this
//     producer span — real cross-hop parent→child linkage (ADR-0028 / ADR-0025 Option 2). The
//     header rides beside the frozen envelope, never in it (GR-1), via
//     [babelqueue.App.PublishWithHeaders]; if the transport can't carry headers it degrades to a
//     plain publish (the traceparent is simply not propagated — no error).
//   - It also carries the active trace's TraceID into the message's trace_id (the v0.1
//     behaviour, ADR-0025 Option 1), so even a consumer that ignores the header — or a transport
//     that drops it — still recovers the same trace (correlation without exact span linkage).
//
// It otherwise behaves like [babelqueue.App.Publish], returning the message id.
func Publish(
	ctx context.Context,
	tracer trace.Tracer,
	app *babelqueue.App,
	urn string,
	data map[string]any,
	opts ...babelqueue.Option,
) (string, error) {
	ctx, span := tracer.Start(ctx, "publish "+urn,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", system),
			attribute.String("messaging.operation", "publish"),
			attribute.String("messaging.destination.name", urn),
		),
	)
	defer span.End()

	headers := injectTraceparent(ctx, nil)
	traceID := UUIDOf(span.SpanContext().TraceID())
	id, err := app.PublishWithHeaders(ctx, urn, data, headers, append(opts, babelqueue.WithTraceID(traceID))...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	span.SetAttributes(attribute.String("messaging.message.id", id))
	return id, nil
}

func consumeAttrs(env babelqueue.Envelope) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", system),
		attribute.String("messaging.operation", "process"),
		attribute.String("messaging.destination.name", env.Meta.Queue),
		attribute.String("messaging.message.id", env.Meta.ID),
		attribute.String("messaging.message.conversation_id", env.TraceID),
		attribute.Int("messaging.babelqueue.attempts", env.Attempts),
	}
}
