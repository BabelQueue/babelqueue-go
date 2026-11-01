package otel

import (
	"context"
	"errors"
	"log/slog"
	"strings"
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
	if s.Name() != "process default" {
		t.Errorf("name = %q, want the queue in the name, not the URN", s.Name())
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
	if v, ok := attrMap(s.Attributes())["error.type"]; !ok || v.AsString() == "" {
		t.Errorf("error.type = %v (present %v), want a non-empty type name", v, ok)
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
	if v, ok := attrMap(s.Attributes())["error.type"]; !ok || v.AsString() == "" {
		t.Errorf("error.type = %v (present %v), want a non-empty type name", v, ok)
	}
	if len(s.Events()) == 0 {
		t.Error("expected a recorded error event")
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

// attrMap indexes a span's attributes by key.
func attrMap(attrs []attribute.KeyValue) map[string]attribute.Value {
	m := make(map[string]attribute.Value, len(attrs))
	for _, a := range attrs {
		m[string(a.Key)] = a.Value
	}
	return m
}

// wantAttrs asserts that attrs holds exactly the wanted keys with the wanted values (string or
// int) and nothing else.
func wantAttrs(t *testing.T, attrs []attribute.KeyValue, want map[string]any) {
	t.Helper()
	got := attrMap(attrs)
	for k, w := range want {
		v, ok := got[k]
		if !ok {
			t.Errorf("attribute %q missing", k)
			continue
		}
		switch w := w.(type) {
		case string:
			if v.Type() != attribute.STRING || v.AsString() != w {
				t.Errorf("attribute %q = %v, want %q", k, v.Emit(), w)
			}
		case int:
			if v.Type() != attribute.INT64 || v.AsInt64() != int64(w) {
				t.Errorf("attribute %q = %v, want %d", k, v.Emit(), w)
			}
		default:
			t.Fatalf("unsupported want type %T", w)
		}
	}
	for k, v := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected attribute %q = %v", k, v.Emit())
		}
	}
}

func TestSemconvPin(t *testing.T) {
	if SemconvVersion != "1.44.0" || SchemaURL != "https://opentelemetry.io/schemas/1.44.0" {
		t.Fatalf("pin = %q / %q", SemconvVersion, SchemaURL)
	}
}

func TestWrapHandler_FullAttributeSet(t *testing.T) {
	tracer, sr := recorder(t)
	env, _ := babelqueue.Make("urn:acme:order.created", map[string]any{"n": 1}, babelqueue.WithQueue("orders.events"),
		babelqueue.WithTraceID("order-42"))
	env.Attempts = 2

	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil },
		WithSystem("kafka"), WithConsumerGroup("billing-workers"))
	if err := h(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	s := sr.Ended()[0]
	if s.Name() != "process orders.events" || s.SpanKind() != trace.SpanKindConsumer || s.Status().Code != codes.Unset {
		t.Errorf("span = %q kind %v status %v", s.Name(), s.SpanKind(), s.Status().Code)
	}
	wantAttrs(t, s.Attributes(), map[string]any{
		"messaging.system":                  "kafka",
		"messaging.operation.type":          "process",
		"messaging.operation.name":          "process",
		"messaging.destination.name":        "orders.events",
		"messaging.message.id":              env.Meta.ID,
		"messaging.message.conversation_id": "order-42", // non-UUID, byte for byte
		"messaging.consumer.group.name":     "billing-workers",
		"babelqueue.urn":                    "urn:acme:order.created",
		"babelqueue.schema_version":         1,
		"babelqueue.attempts":               2,
		// transition MINOR: dual-emitted deprecated keys (ADR-0038 section 5)
		"messaging.operation":           "process",
		"messaging.babelqueue.attempts": 2,
	})
}

func TestWrapHandler_DefaultSystemAndNoOptionalAttrs(t *testing.T) {
	tracer, sr := recorder(t)
	env, _ := babelqueue.Make("urn:acme:order.created", nil, babelqueue.WithQueue("   "))
	env.Meta.ID = ""

	if err := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil })(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	s := sr.Ended()[0]
	if s.Name() != "process" {
		t.Errorf("name = %q, want the bare process (blank queue, ADR-0038 section 4)", s.Name())
	}
	got := attrMap(s.Attributes())
	if got["messaging.system"].AsString() != "babelqueue" {
		t.Errorf("messaging.system = %v, want the deprecated default", got["messaging.system"].Emit())
	}
	for _, k := range []string{"messaging.destination.name", "messaging.message.id", "messaging.consumer.group.name", "error.type"} {
		if _, ok := got[k]; ok {
			t.Errorf("attribute %q must be absent", k)
		}
	}
}

func TestWrapHandler_EmptySystemCountsAsAbsent(t *testing.T) {
	tracer, sr := recorder(t)
	env, _ := babelqueue.Make("urn:acme:order.created", nil)
	_ = WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil }, WithSystem(""))(context.Background(), env)
	if v := attrMap(sr.Ended()[0].Attributes())["messaging.system"]; v.AsString() != "babelqueue" {
		t.Errorf("messaging.system = %v", v.Emit())
	}
}

func publishRecorder(t *testing.T, tr babelqueue.Transport, appOpts ...babelqueue.AppOption) (*Publisher, *tracetest.SpanRecorder) {
	t.Helper()
	tracer, sr := recorder(t)
	return NewPublisher(tracer, babelqueue.NewApp(tr, appOpts...), WithSystem("rabbitmq")), sr
}

func TestPublisher_FullAttributeSet(t *testing.T) {
	pub, sr := publishRecorder(t, babelqueue.NewInMemoryTransport(), babelqueue.WithDefaultQueue("billing"))
	id, err := pub.Publish(context.Background(), "urn:acme:order.created", map[string]any{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	s := sr.Ended()[0]
	if s.Name() != "send billing" || s.SpanKind() != trace.SpanKindProducer || s.Status().Code != codes.Unset {
		t.Errorf("span = %q kind %v status %v", s.Name(), s.SpanKind(), s.Status().Code)
	}
	wantAttrs(t, s.Attributes(), map[string]any{
		"messaging.system":                  "rabbitmq",
		"messaging.operation.type":          "send",
		"messaging.operation.name":          "send",
		"messaging.destination.name":        "billing",
		"messaging.message.id":              id,
		"messaging.message.conversation_id": UUIDOf(s.SpanContext().TraceID()),
		"babelqueue.urn":                    "urn:acme:order.created",
		"babelqueue.schema_version":         1,
		"messaging.operation":               "publish",
	})
}

func TestPublisher_DestinationIsTheQueueNotTheURN(t *testing.T) {
	pub, sr := publishRecorder(t, babelqueue.NewInMemoryTransport())
	if _, err := pub.Publish(context.Background(), "urn:acme:order.created", nil, babelqueue.WithQueue("orders")); err != nil {
		t.Fatal(err)
	}
	s := sr.Ended()[0]
	if s.Name() != "send orders" {
		t.Errorf("name = %q", s.Name())
	}
	if v := attrMap(s.Attributes())["messaging.destination.name"]; v.AsString() != "orders" {
		t.Errorf("destination = %v, want the queue", v.Emit())
	}
}

// A per-call WithQueue the otel module cannot read still ends up in the span name, because it
// comes from the envelope the App built, not from the app's default queue.
func TestPublisher_FailureAfterEnvelopeBuiltUsesThePerCallQueue(t *testing.T) {
	pub, sr := publishRecorder(t, failTransport{}, babelqueue.WithDefaultQueue("billing"))
	_, err := pub.Publish(context.Background(), "urn:acme:order.created", nil, babelqueue.WithQueue("orders"))
	if err == nil {
		t.Fatal("expected a publish error")
	}
	s := sr.Ended()[0]
	if s.Name() != "send orders" {
		t.Errorf("name = %q, want send orders (never send billing)", s.Name())
	}
	got := attrMap(s.Attributes())
	if got["messaging.destination.name"].AsString() != "orders" {
		t.Errorf("destination = %v", got["messaging.destination.name"].Emit())
	}
	if got["messaging.message.id"].AsString() == "" || got["messaging.message.conversation_id"].AsString() == "" {
		t.Error("the envelope was handed to the transport, so message.id and conversation_id must be written")
	}
	if s.Status().Code != codes.Error || got["error.type"].AsString() == "" {
		t.Errorf("status = %v error.type = %v", s.Status().Code, got["error.type"].Emit())
	}
}

// Encode fails after a successful Make: no envelope reaches the transport, so the span stays the
// bare "send" with no destination, no message id and no conversation id.
func TestPublisher_FailureBeforeEnvelopeBuiltKeepsBareSend(t *testing.T) {
	pub, sr := publishRecorder(t, babelqueue.NewInMemoryTransport(), babelqueue.WithDefaultQueue("billing"))
	_, err := pub.Publish(context.Background(), "urn:acme:order.created", map[string]any{"bad": make(chan int)},
		babelqueue.WithQueue("orders"))
	if err == nil {
		t.Fatal("expected an encode error")
	}
	s := sr.Ended()[0]
	if s.Name() != "send" {
		t.Errorf("name = %q, want the bare send (no guessed destination)", s.Name())
	}
	got := attrMap(s.Attributes())
	for _, k := range []string{"messaging.destination.name", "messaging.message.id", "messaging.message.conversation_id"} {
		if _, ok := got[k]; ok {
			t.Errorf("attribute %q must be absent when no envelope was built", k)
		}
	}
	if s.Status().Code != codes.Error || got["error.type"].AsString() == "" {
		t.Errorf("status = %v error.type = %v", s.Status().Code, got["error.type"].Emit())
	}
	wantAttrs(t, s.Attributes(), map[string]any{
		"messaging.system": "rabbitmq", "messaging.operation.type": "send", "messaging.operation.name": "send",
		"babelqueue.urn": "urn:acme:order.created", "babelqueue.schema_version": 1, "messaging.operation": "publish",
		"error.type": got["error.type"].AsString(),
	})
}

func TestPublisher_BlankProducedQueueMeansNoDestination(t *testing.T) {
	pub, sr := publishRecorder(t, babelqueue.NewInMemoryTransport())
	if _, err := pub.Publish(context.Background(), "urn:acme:order.created", nil, babelqueue.WithQueue("")); err != nil {
		t.Fatal(err)
	}
	s := sr.Ended()[0]
	if s.Name() != "send" {
		t.Errorf("name = %q, want send", s.Name())
	}
	if _, ok := attrMap(s.Attributes())["messaging.destination.name"]; ok {
		t.Error("a blank produced queue must not be written as a destination")
	}
}

func TestPublish_FreeFunctionKeepsItsSignature(t *testing.T) {
	tracer, sr := recorder(t)
	app := babelqueue.NewApp(babelqueue.NewInMemoryTransport())
	opts := []babelqueue.Option{babelqueue.WithQueue("orders")}
	if _, err := Publish(context.Background(), tracer, app, "urn:acme:a", nil, opts...); err != nil {
		t.Fatal(err)
	}
	if len(opts) != 1 {
		t.Error("Publish must not append to the caller's option slice")
	}
	s := sr.Ended()[0]
	if s.Name() != "send orders" || attrMap(s.Attributes())["messaging.system"].AsString() != "babelqueue" {
		t.Errorf("span = %q system %v", s.Name(), attrMap(s.Attributes())["messaging.system"].Emit())
	}
}

// ADR-0038 section 7: a trace_id that is not a UUID is kept as is and hashed, with a warning once
// per distinct value; the mapping itself is unchanged.
func TestWrapHandler_WarnsOncePerHashedTraceID(t *testing.T) {
	var buf strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	tracer, sr := recorder(t)
	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil }, WithSystem("redis"))
	deliver := func(traceID string) {
		env, _ := babelqueue.Make("urn:acme:a", nil, babelqueue.WithTraceID("x"))
		env.TraceID = traceID
		if err := h(context.Background(), env); err != nil {
			t.Fatal(err)
		}
	}
	deliver("order-42")
	deliver("order-42")
	deliver("7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b") // a UUID: no warning
	deliver("order-43")

	if n := strings.Count(buf.String(), "trace_id is not a UUID"); n != 2 {
		t.Errorf("warnings = %d, want 2 (one per distinct hashed value):\n%s", n, buf.String())
	}
	spans := sr.Ended()
	if len(spans) != 4 || spans[0].SpanContext().TraceID() != TraceIDOf("order-42") {
		t.Error("the hashed mapping must be unchanged")
	}
}

// Make trims the URN; the producer span carries the same trimmed value as the envelope's job, so it
// matches the consumer span of the same message.
func TestPublisher_URNIsTrimmedLikeTheEnvelopeJob(t *testing.T) {
	tr := &captureTransport{}
	pub, sr := publishRecorder(t, tr)
	if _, err := pub.Publish(context.Background(), " \turn:acme:order.created\n ", nil); err != nil {
		t.Fatal(err)
	}
	env, err := babelqueue.Decode([]byte(tr.bodies[0]))
	if err != nil {
		t.Fatal(err)
	}
	if got := attrMap(sr.Ended()[0].Attributes())["babelqueue.urn"].AsString(); got != "urn:acme:order.created" || got != env.URN() {
		t.Errorf("babelqueue.urn = %q, envelope job = %q", got, env.URN())
	}
}
