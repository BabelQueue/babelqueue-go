package kafka

import (
	"context"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	kafkago "github.com/segmentio/kafka-go"
)

const urn = "urn:babel:orders:created"

type fakeWriter struct{ sent []kafkago.Message }

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.sent = append(f.sent, msgs...)
	return nil
}

type fakeReader struct {
	messages  []kafkago.Message
	committed []kafkago.Message
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if len(f.messages) == 0 {
		<-ctx.Done() // emulate a blocking fetch that ends when the poll deadline fires
		return kafkago.Message{}, ctx.Err()
	}
	m := f.messages[0]
	f.messages = f.messages[1:]
	return m, nil
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.committed = append(f.committed, msgs...)
	return nil
}

func sampleBody(t *testing.T, attempts int) string {
	t.Helper()
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	env.Attempts = attempts
	b, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func headerMap(hs []kafkago.Header) map[string]string {
	m := make(map[string]string, len(hs))
	for _, h := range hs {
		m[h.Key] = string(h.Value)
	}
	return m
}

func newTransport(t *testing.T, fw *fakeWriter, fr *fakeReader) *Transport {
	t.Helper()
	tr, err := New(WithWriter(fw), WithReaderFactory(func(string) Reader { return fr }))
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestPublishProjectsHeadersValueTimestamp(t *testing.T) {
	fw := &fakeWriter{}
	tr := newTransport(t, fw, &fakeReader{})
	body := sampleBody(t, 0)

	if err := tr.Publish(context.Background(), "orders", body); err != nil {
		t.Fatal(err)
	}
	if len(fw.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(fw.sent))
	}
	m := fw.sent[0]
	if m.Topic != "orders" {
		t.Errorf("topic = %q, want orders", m.Topic)
	}
	if string(m.Value) != body {
		t.Errorf("value mismatch")
	}
	h := headerMap(m.Headers)
	if h["bq-job"] != urn {
		t.Errorf("bq-job = %q", h["bq-job"])
	}
	if h["bq-schema-version"] != "1" {
		t.Errorf("bq-schema-version = %q", h["bq-schema-version"])
	}
	if h["bq-attempts"] != "0" {
		t.Errorf("bq-attempts = %q", h["bq-attempts"])
	}
	env, _ := babelqueue.Decode([]byte(body))
	if m.Time.UnixMilli() != env.Meta.CreatedAt {
		t.Errorf("record time = %d, want created_at %d", m.Time.UnixMilli(), env.Meta.CreatedAt)
	}
	if h["bq-message-id"] != env.Meta.ID {
		t.Errorf("bq-message-id = %q, want %q", h["bq-message-id"], env.Meta.ID)
	}
}

func TestPopReconcilesAttemptsAndAcks(t *testing.T) {
	body := sampleBody(t, 0)
	rec := kafkago.Message{
		Topic:   "orders",
		Value:   []byte(body),
		Headers: []kafkago.Header{{Key: "bq-attempts", Value: []byte("2")}},
	}
	fr := &fakeReader{messages: []kafkago.Message{rec}}
	tr := newTransport(t, &fakeWriter{}, fr)

	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if msg == nil {
		t.Fatal("Pop returned nil")
	}
	env, _ := babelqueue.Decode([]byte(msg.Body))
	if env.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (from bq-attempts header)", env.Attempts)
	}

	if err := tr.Ack(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(fr.committed) != 1 {
		t.Errorf("committed %d, want 1", len(fr.committed))
	}
}

func TestPopEmptyReturnsNil(t *testing.T) {
	tr := newTransport(t, &fakeWriter{}, &fakeReader{})
	msg, err := tr.Pop(context.Background(), "orders", 20*time.Millisecond)
	if err != nil || msg != nil {
		t.Errorf("Pop = (%v, %v), want (nil, nil)", msg, err)
	}
}

func TestAckNoopWithoutHandle(t *testing.T) {
	tr := newTransport(t, &fakeWriter{}, &fakeReader{})
	if err := tr.Ack(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders", Handle: nil}); err != nil {
		t.Errorf("Ack with no handle returned %v", err)
	}
}

func TestReconcileHeaderAuthoritativeAndFallback(t *testing.T) {
	body := sampleBody(t, 3)

	// Header present → authoritative (overrides the body's own count).
	out := reconcileAttempts(body, []kafkago.Header{{Key: "bq-attempts", Value: []byte("1")}})
	if env, _ := babelqueue.Decode([]byte(out)); env.Attempts != 1 {
		t.Errorf("header-present attempts = %d, want 1", env.Attempts)
	}
	// Header absent → fall back to the body's attempts.
	if reconcileAttempts(body, nil) != body {
		t.Error("header-absent should leave the body unchanged (fallback to body attempts)")
	}
	// Garbage header → fall back to the body.
	if reconcileAttempts(body, []kafkago.Header{{Key: "bq-attempts", Value: []byte("x")}}) != body {
		t.Error("garbage bq-attempts should fall back to the body")
	}
}

func TestMessageWithGarbageBodyHasNoHeaders(t *testing.T) {
	m := message("orders", "not-json")
	if string(m.Value) != "not-json" {
		t.Errorf("value = %q", string(m.Value))
	}
	if len(m.Headers) != 0 {
		t.Errorf("headers = %v, want none for a non-envelope body", m.Headers)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(); err == nil {
		t.Error("New() with no config should error")
	}
	if _, err := New(WithBrokers("localhost:9092")); err == nil {
		t.Error("New(WithBrokers) without a group id should error")
	}
	if _, err := New(WithBrokers("localhost:9092"), WithGroupID("g"), WithMaxWaitTime(time.Second)); err != nil {
		t.Errorf("New with brokers + group id should succeed, got %v", err)
	}
}
