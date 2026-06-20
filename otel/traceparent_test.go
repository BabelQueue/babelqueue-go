package otel

import (
	"context"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestCrossHopParentChildLinkage proves the core of ADR-0028: a consumer span started by
// WrapHandler is a true child of the producer span across a simulated hop. The producer injects
// a W3C traceparent into transport headers; those headers ride to the consumer (as the runtime
// surfaces them on the handler context); the consumer span's parent must be exactly the
// producer span's span context — same trace, and the producer's span id as the parent span id.
func TestCrossHopParentChildLinkage(t *testing.T) {
	tracer, sr := recorder(t)

	// PRODUCER: start a span and inject its context as a traceparent header — exactly what the
	// otel Publish wrapper hands to App.PublishWithHeaders.
	prodCtx, producer := tracer.Start(context.Background(), "publish urn:babel:orders:created",
		trace.WithSpanKind(trace.SpanKindProducer))
	headers := injectTraceparent(prodCtx, nil)
	producer.End()

	if headers[HeaderTraceparent] == "" {
		t.Fatal("producer did not inject a traceparent header")
	}

	// HOP: the header travels on the transport. On the consume side the runtime surfaces the
	// delivered message's headers onto the handler context (babelqueue.dispatch -> withHeaders).
	consumeCtx := contextWithDeliveredHeaders(headers)

	// CONSUMER: WrapHandler must start its span as a child of the producer's span.
	var childSC trace.SpanContext
	h := WrapHandler(tracer, func(ctx context.Context, _ babelqueue.Envelope) error {
		childSC = trace.SpanContextFromContext(ctx)
		return nil
	})
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1})
	if err := h(consumeCtx, env); err != nil {
		t.Fatal(err)
	}

	consumer := endedByName(t, sr, "process "+env.URN())
	prodSC := producer.SpanContext()

	// Same trace across the hop.
	if consumer.SpanContext().TraceID() != prodSC.TraceID() {
		t.Errorf("consumer trace %s != producer trace %s",
			consumer.SpanContext().TraceID(), prodSC.TraceID())
	}
	// True parent-child: the consumer span's PARENT is the producer span (its span id), not a
	// trace_id-derived synthetic parent.
	if consumer.Parent().SpanID() != prodSC.SpanID() {
		t.Errorf("consumer parent span id %s != producer span id %s",
			consumer.Parent().SpanID(), prodSC.SpanID())
	}
	if !consumer.Parent().IsRemote() {
		t.Error("the producer must be recorded as a remote parent")
	}
	// And the handler's own active span is a fresh child of that parent.
	if childSC.SpanID() == prodSC.SpanID() {
		t.Error("the consumer span must be a new child, not the producer span itself")
	}
}

// TestNoHeaderFallsBackToTraceID proves backward compatibility: with no traceparent header,
// WrapHandler falls back to the v0.1 behaviour — the span lands in the trace_id-derived trace
// (ADR-0025 Option 1), so a message produced by a pre-0028 producer is not regressed.
func TestNoHeaderFallsBackToTraceID(t *testing.T) {
	tracer, sr := recorder(t)

	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil })
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1})

	// No headers on the context — the consume path of a producer that never injected traceparent.
	if err := h(context.Background(), env); err != nil {
		t.Fatal(err)
	}

	consumer := endedByName(t, sr, "process "+env.URN())
	if consumer.SpanContext().TraceID() != TraceIDOf(env.TraceID) {
		t.Error("fallback span must be in the trace_id-derived trace (v0.1 behaviour)")
	}
	// The fallback parent is the deterministic trace_id-derived span id, not a real remote span.
	if consumer.Parent().SpanID() != spanIDOf(env.TraceID) {
		t.Error("fallback parent must be the trace_id-derived synthetic parent")
	}
}

// TestRemoteParentFromHeaders_IgnoresMalformed proves a malformed/empty traceparent does not
// hijack the trace: extraction yields no valid remote parent, so the caller falls back.
func TestRemoteParentFromHeaders_IgnoresMalformed(t *testing.T) {
	// Empty headers -> no remote parent.
	if _, ok := remoteParentFromHeaders(context.Background()); ok {
		t.Error("a header-less context must yield no remote parent")
	}
	// Present but malformed traceparent -> no valid remote parent (W3C propagator rejects it).
	ctx := contextWithDeliveredHeaders(map[string]string{HeaderTraceparent: "garbage"})
	if _, ok := remoteParentFromHeaders(ctx); ok {
		t.Error("a malformed traceparent must not produce a valid remote parent")
	}
}

// TestPublish_InjectsTraceparentHeader proves the producer wrapper actually puts a traceparent
// on the wire (via PublishWithHeaders) AND keeps stamping trace_id for the v0.1 fallback.
func TestPublish_InjectsTraceparentHeader(t *testing.T) {
	tracer, sr := recorder(t)
	transport := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(transport)

	if _, err := Publish(context.Background(), tracer, app, "urn:babel:orders:created",
		map[string]any{"order_id": 7}); err != nil {
		t.Fatal(err)
	}
	span := sr.Ended()[0]

	msg, err := transport.Pop(context.Background(), "default", 0)
	if err != nil || msg == nil {
		t.Fatalf("pop: msg=%v err=%v", msg, err)
	}
	tp := msg.Headers[HeaderTraceparent]
	if tp == "" {
		t.Fatal("Publish did not carry a traceparent transport header")
	}
	// The injected traceparent must encode the producer span (extract -> same trace + span id).
	extracted := propagator.Extract(context.Background(), mapCarrier(msg.Headers))
	rsc := trace.SpanContextFromContext(extracted)
	if rsc.TraceID() != span.SpanContext().TraceID() || rsc.SpanID() != span.SpanContext().SpanID() {
		t.Error("the traceparent header does not encode the producer span context")
	}
	// v0.1 belt-and-braces: trace_id still encodes the same trace for header-blind consumers.
	env, err := babelqueue.Decode([]byte(msg.Body))
	if err != nil {
		t.Fatal(err)
	}
	if TraceIDOf(env.TraceID) != span.SpanContext().TraceID() {
		t.Error("trace_id no longer encodes the producer trace (v0.1 fallback broken)")
	}
}

// TestEndToEndProducerToConsumer wires the real producer (Publish over InMemoryTransport) to the
// real consumer (WrapHandler dispatched by the App runtime), proving parent-child linkage across
// the full path — header inject by Publish, carried by the transport, surfaced by the runtime,
// extracted by WrapHandler.
func TestEndToEndProducerToConsumer(t *testing.T) {
	tracer, sr := recorder(t)
	transport := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(transport)

	app.Handle("urn:babel:orders:created", WrapHandler(tracer,
		func(context.Context, babelqueue.Envelope) error { return nil }))

	if _, err := Publish(context.Background(), tracer, app, "urn:babel:orders:created",
		map[string]any{"order_id": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "default", 1); err != nil {
		t.Fatal(err)
	}

	producer := endedByKind(t, sr, trace.SpanKindProducer)
	consumer := endedByKind(t, sr, trace.SpanKindConsumer)
	if consumer.Parent().SpanID() != producer.SpanContext().SpanID() {
		t.Fatalf("end-to-end: consumer parent %s != producer span %s",
			consumer.Parent().SpanID(), producer.SpanContext().SpanID())
	}
	if consumer.SpanContext().TraceID() != producer.SpanContext().TraceID() {
		t.Error("end-to-end: consumer and producer are in different traces")
	}
}

// contextWithDeliveredHeaders simulates the runtime surfacing a delivered message's transport
// headers onto the handler context — the same thing babelqueue.dispatch does via withHeaders.
// It round-trips through the public InMemoryTransport + App.Drain seam so the test exercises the
// real surfacing path rather than reaching into core internals.
func contextWithDeliveredHeaders(headers map[string]string) context.Context {
	tr := babelqueue.NewInMemoryTransport()
	body, _ := mustMake().Encode()
	_ = tr.PublishWithHeaders(context.Background(), "default", string(body), headers)

	captured := make(chan context.Context, 1)
	app := babelqueue.NewApp(tr)
	app.Handle("urn:babel:headers:probe", func(ctx context.Context, _ babelqueue.Envelope) error {
		captured <- ctx
		return nil
	})
	_, _ = app.Drain(context.Background(), "default", 1)
	return <-captured
}

func mustMake() babelqueue.Envelope {
	env, _ := babelqueue.Make("urn:babel:headers:probe", map[string]any{"k": "v"})
	return env
}

func endedByName(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range sr.Ended() {
		if s.Name() == name {
			return s
		}
	}
	t.Fatalf("no ended span named %q (got %d spans)", name, len(sr.Ended()))
	return nil
}

func endedByKind(t *testing.T, sr *tracetest.SpanRecorder, kind trace.SpanKind) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range sr.Ended() {
		if s.SpanKind() == kind {
			return s
		}
	}
	t.Fatalf("no ended span of kind %v", kind)
	return nil
}
