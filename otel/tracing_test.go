package otel

import (
	"context"
	"errors"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// failTransport is a Transport whose Publish always errors, to exercise the producer span's
// error path.
type failTransport struct{}

func (failTransport) Publish(context.Context, string, string) error {
	return errors.New("transport publish failed")
}

func (failTransport) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	return nil, nil
}

func (failTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error { return nil }

func recorder(t *testing.T) (trace.Tracer, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	return tp.Tracer("test"), sr
}

func TestTraceIDMappingRoundTrip(t *testing.T) {
	uuid := "7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b"
	tid := TraceIDOf(uuid)
	if !tid.IsValid() {
		t.Fatal("derived TraceID is invalid")
	}
	if got := UUIDOf(tid); got != uuid {
		t.Fatalf("round-trip: got %q, want %q", got, uuid)
	}
	a, b := TraceIDOf("not-a-uuid"), TraceIDOf("not-a-uuid")
	if a != b || !a.IsValid() {
		t.Fatal("a non-uuid trace_id must map deterministically to a valid TraceID")
	}
	if a == tid {
		t.Fatal("different inputs should map to different traces")
	}
}

func TestWrapHandler_SpanInTraceIDTraceWithAttrs(t *testing.T) {
	tracer, sr := recorder(t)
	called := false
	h := WrapHandler(tracer, func(ctx context.Context, _ babelqueue.Envelope) error {
		called = true
		if !trace.SpanContextFromContext(ctx).IsValid() {
			t.Error("handler ctx carries no active span")
		}
		return nil
	})
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1})

	if err := h(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("handler not called")
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	s := spans[0]
	if s.Name() != "process "+env.URN() {
		t.Errorf("name = %q", s.Name())
	}
	if s.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("kind = %v, want consumer", s.SpanKind())
	}
	if s.SpanContext().TraceID() != TraceIDOf(env.TraceID) {
		t.Error("the span is not in the trace_id-derived trace")
	}
	if !hasStringAttr(s.Attributes(), "messaging.message.conversation_id", env.TraceID) {
		t.Error("missing conversation_id == trace_id attribute")
	}
}

func TestWrapHandler_RecordsError(t *testing.T) {
	tracer, sr := recorder(t)
	boom := errors.New("boom")
	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return boom })
	env, _ := babelqueue.Make("urn:babel:orders:created", nil)

	if err := h(context.Background(), env); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	s := sr.Ended()[0]
	if s.Status().Code != codes.Error {
		t.Errorf("status = %v, want Error", s.Status().Code)
	}
	if len(s.Events()) == 0 {
		t.Error("expected a recorded error event")
	}
}

func TestPublish_StampsTraceIDFromSpan(t *testing.T) {
	tracer, sr := recorder(t)
	transport := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(transport)

	id, err := Publish(context.Background(), tracer, app, "urn:babel:orders:created", map[string]any{"order_id": 7})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("empty message id")
	}

	spans := sr.Ended()
	if len(spans) != 1 || spans[0].SpanKind() != trace.SpanKindProducer {
		t.Fatalf("want 1 producer span, got %+v", spans)
	}

	msg, err := transport.Pop(context.Background(), "default", 0)
	if err != nil || msg == nil {
		t.Fatalf("pop: msg=%v err=%v", msg, err)
	}
	env, err := babelqueue.Decode([]byte(msg.Body))
	if err != nil {
		t.Fatal(err)
	}
	// the published message's trace_id encodes the producer span's trace, so a consumer
	// recovers the same trace.
	if env.TraceID != UUIDOf(spans[0].SpanContext().TraceID()) {
		t.Errorf("message trace_id %q does not encode the producer span's trace", env.TraceID)
	}
	if TraceIDOf(env.TraceID) != spans[0].SpanContext().TraceID() {
		t.Error("a consumer would not recover the producer's trace from this trace_id")
	}
}

func TestPublish_RecordsError(t *testing.T) {
	tracer, sr := recorder(t)
	app := babelqueue.NewApp(failTransport{})

	if _, err := Publish(context.Background(), tracer, app, "urn:babel:orders:created", nil); err == nil {
		t.Fatal("expected a publish error")
	}
	s := sr.Ended()[0]
	if s.Status().Code != codes.Error {
		t.Errorf("status = %v, want Error", s.Status().Code)
	}
}

func hasStringAttr(attrs []attribute.KeyValue, key, val string) bool {
	for _, a := range attrs {
		if string(a.Key) == key && a.Value.AsString() == val {
			return true
		}
	}
	return false
}
