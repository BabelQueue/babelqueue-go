package artemis

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const urn = "urn:babel:orders:created"

type fakeSender struct{ sent []*amqp.Message }

func (f *fakeSender) Send(_ context.Context, m *amqp.Message, _ *amqp.SendOptions) error {
	f.sent = append(f.sent, m)
	return nil
}

type fakeReceiver struct {
	messages []*amqp.Message
	accepted []*amqp.Message
	released []*amqp.Message
	err      error
}

func (f *fakeReceiver) Receive(ctx context.Context, _ *amqp.ReceiveOptions) (*amqp.Message, error) {
	if f.err != nil {
		return nil, f.err
	}
	if len(f.messages) == 0 {
		<-ctx.Done() // emulate a blocking receive that ends when the poll deadline fires
		return nil, ctx.Err()
	}
	m := f.messages[0]
	f.messages = f.messages[1:]
	return m, nil
}

func (f *fakeReceiver) AcceptMessage(_ context.Context, m *amqp.Message) error {
	f.accepted = append(f.accepted, m)
	return nil
}

func (f *fakeReceiver) ReleaseMessage(_ context.Context, m *amqp.Message) error {
	f.released = append(f.released, m)
	return nil
}

type fakeClient struct {
	sender   *fakeSender
	receiver *fakeReceiver
}

func (f *fakeClient) Sender(context.Context, string) (Sender, error)     { return f.sender, nil }
func (f *fakeClient) Receiver(context.Context, string) (Receiver, error) { return f.receiver, nil }

func newTestTransport(t *testing.T, c *fakeClient) *Transport {
	t.Helper()
	tr, err := New(context.Background(), WithClient(c))
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func sampleBody(t *testing.T, attempts int) string {
	t.Helper()
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042},
		babelqueue.WithQueue("orders"), babelqueue.WithTraceID("trace-1"))
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

func incoming(body, jmsType string, deliveryCount uint32) *amqp.Message {
	m := &amqp.Message{Value: body}
	if deliveryCount > 0 {
		m.Header = &amqp.MessageHeader{DeliveryCount: deliveryCount}
	}
	if jmsType != "" {
		m.Annotations = amqp.Annotations{jmsTypeKey: jmsType}
	}
	return m
}

func TestPublishProjectsBodyCorrelationAnnotationAndProperties(t *testing.T) {
	sender := &fakeSender{}
	tr := newTestTransport(t, &fakeClient{sender: sender})
	body := sampleBody(t, 2)

	if err := tr.Publish(context.Background(), "orders", body); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("want 1 sent, got %d", len(sender.sent))
	}
	m := sender.sent[0]
	if got, _ := m.Value.(string); got != body {
		t.Errorf("body: want envelope JSON, got %q", got)
	}
	if m.Properties == nil || m.Properties.CorrelationID != "trace-1" {
		t.Errorf("correlation-id: want trace-1, got %#v", m.Properties)
	}
	if m.Annotations[jmsTypeKey] != urn {
		t.Errorf("x-opt-jms-type: want %q, got %#v", urn, m.Annotations[jmsTypeKey])
	}
	if m.ApplicationProperties["bq_app_id"] != "babelqueue" {
		t.Errorf("bq_app_id: want babelqueue, got %#v", m.ApplicationProperties["bq_app_id"])
	}
	if m.ApplicationProperties["bq_schema_version"] != "1" {
		t.Errorf("bq_schema_version: want \"1\", got %#v", m.ApplicationProperties["bq_schema_version"])
	}
	if m.ApplicationProperties["bq_attempts"] != "2" {
		t.Errorf("bq_attempts: want \"2\", got %#v", m.ApplicationProperties["bq_attempts"])
	}
	if m.ApplicationProperties["bq_source_lang"] == nil {
		t.Error("bq_source_lang: want present")
	}
}

func TestPopReconcilesAttempts(t *testing.T) {
	cases := []struct {
		name          string
		bodyAttempts  int
		deliveryCount uint32
		want          int
	}{
		{"delivery-count wins, no minus one", 0, 3, 3},
		{"body never lowered", 5, 2, 5},
		{"first delivery keeps body", 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			receiver := &fakeReceiver{messages: []*amqp.Message{incoming(sampleBody(t, tc.bodyAttempts), urn, tc.deliveryCount)}}
			tr := newTestTransport(t, &fakeClient{receiver: receiver})

			msg, err := tr.Pop(context.Background(), "orders", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				t.Fatal("want a message, got nil")
			}
			env, err := babelqueue.Decode([]byte(msg.Body))
			if err != nil {
				t.Fatal(err)
			}
			if env.Attempts != tc.want {
				t.Errorf("attempts: want %d, got %d", tc.want, env.Attempts)
			}
			if _, ok := msg.Handle.(*amqp.Message); !ok {
				t.Error("handle: want the *amqp.Message")
			}
		})
	}
}

func TestPopReturnsNilOnTimeout(t *testing.T) {
	tr := newTestTransport(t, &fakeClient{receiver: &fakeReceiver{}})
	msg, err := tr.Pop(context.Background(), "orders", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("want nil error, got %v", err)
	}
	if msg != nil {
		t.Errorf("want nil message on timeout, got %#v", msg)
	}
}

func TestPopPropagatesReceiveError(t *testing.T) {
	tr := newTestTransport(t, &fakeClient{receiver: &fakeReceiver{err: errors.New("link detached")}})
	if _, err := tr.Pop(context.Background(), "orders", time.Second); err == nil {
		t.Error("want an error from Receive")
	}
}

func TestAckAcceptsTheMessage(t *testing.T) {
	receiver := &fakeReceiver{messages: []*amqp.Message{incoming(sampleBody(t, 0), urn, 0)}}
	tr := newTestTransport(t, &fakeClient{receiver: receiver})

	msg, err := tr.Pop(context.Background(), "orders", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Ack(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(receiver.accepted) != 1 {
		t.Errorf("want 1 accepted, got %d", len(receiver.accepted))
	}
}

func TestAckIgnoresAForeignHandle(t *testing.T) {
	tr := newTestTransport(t, &fakeClient{receiver: &fakeReceiver{}})
	err := tr.Ack(context.Background(), &babelqueue.ReceivedMessage{Body: "{}", Queue: "orders", Handle: "not-amqp"})
	if err != nil {
		t.Errorf("want nil for a foreign handle, got %v", err)
	}
}

func TestMessageBodyVariants(t *testing.T) {
	if got := messageBody(&amqp.Message{Value: "hi"}); got != "hi" {
		t.Errorf("string value: got %q", got)
	}
	if got := messageBody(&amqp.Message{Value: []byte("bytes")}); got != "bytes" {
		t.Errorf("byte value: got %q", got)
	}
	if got := messageBody(&amqp.Message{Data: [][]byte{[]byte("data")}}); got != "data" {
		t.Errorf("data section: got %q", got)
	}
	if got := messageBody(&amqp.Message{}); got != "" {
		t.Errorf("empty: got %q", got)
	}
}

func TestDeliveryCountIsZeroWhenHeaderAbsent(t *testing.T) {
	if got := deliveryCount(&amqp.Message{}); got != 0 {
		t.Errorf("want 0, got %d", got)
	}
}

func TestNewRequiresURLOrClient(t *testing.T) {
	if _, err := New(context.Background()); err == nil {
		t.Error("want an error when neither WithURL nor WithClient is given")
	}
}

func TestMessageOfNonConformantBodyStillCarriesTheBody(t *testing.T) {
	// A body that does not decode still rides as the AMQP value (the body stays authoritative).
	m := message("not-json")
	if got, _ := m.Value.(string); got != "not-json" {
		t.Errorf("want raw body preserved, got %q", got)
	}
	if m.Properties != nil {
		t.Error("want no projection for an undecodable body")
	}
}

var _ babelqueue.Transport = (*Transport)(nil)

// popGated receives one message carrying the given bq_schema_version application property
// (none when set is false) with the given AMQP delivery-count.
func popGated(t *testing.T, body string, version any, set bool, count uint32, opts ...Option) (*babelqueue.ReceivedMessage, *fakeReceiver) {
	t.Helper()
	m := incoming(body, "", count)
	if set {
		m.ApplicationProperties = map[string]any{"bq_schema_version": version}
	}
	receiver := &fakeReceiver{messages: []*amqp.Message{m}}
	opts = append([]Option{WithClient(&fakeClient{sender: &fakeSender{}, receiver: receiver})}, opts...)
	tr, err := New(context.Background(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil {
		t.Fatalf("Pop: msg=%v err=%v", msg, err)
	}
	return msg, receiver
}

func TestPopSchemaGateDecodesWhenMissingBlankOrSupported(t *testing.T) {
	cases := map[string]struct {
		version string
		set     bool
	}{
		"missing":   {"", false},
		"empty":     {"", true},
		"blank":     {" \t\n\v\f\r", true},
		"supported": {"1", true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			called := false
			msg, _ := popGated(t, sampleBody(t, 0), c.version, c.set, 2,
				WithOnSchemaVersionRejected(func(string, string) { called = true }))
			if msg.RouteURN != "" || called {
				t.Errorf("RouteURN = %q, hook called = %v; want neither", msg.RouteURN, called)
			}
			env, err := babelqueue.Decode([]byte(msg.Body))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if env.Attempts != 2 {
				t.Errorf("attempts = %d, want 2 (reconciled from delivery-count)", env.Attempts)
			}
		})
	}
}

func TestPopSchemaGateRejectsUnsupportedWithoutDecoding(t *testing.T) {
	cases := map[string]struct {
		version any
		want    string
	}{
		"2":      {"2", "2"},
		"x":      {"x", "x"},
		"01":     {"01", "01"},
		"padded": {" 1", " 1"},
		"nbsp":   {"\u00a0", "\u00a0"},
		"bom":    {"\ufeff", "\ufeff"},
		"int 1":  {1, "1"},
		"bool":   {true, "true"},
		"nil":    {nil, "<nil>"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var gotVersion, gotQueue string
			// A body that is not JSON proves the gate runs before any decode / reconcile.
			msg, _ := popGated(t, "not json", c.version, true, 2,
				WithOnSchemaVersionRejected(func(v, q string) { gotVersion, gotQueue = v, q }))
			if want := unsupportedSchemaRoutePrefix + url.QueryEscape(c.want); msg.RouteURN != want {
				t.Errorf("RouteURN = %q, want %q", msg.RouteURN, want)
			}
			if msg.Body != "not json" {
				t.Errorf("Body = %q, want it handed over untouched", msg.Body)
			}
			if msg.DeliveryCount != 3 {
				t.Errorf("DeliveryCount = %d, want 3 (0-based delivery-count 2, plus one)", msg.DeliveryCount)
			}
			if gotVersion != c.want || gotQueue != "orders" {
				t.Errorf("hook got (%q,%q), want (%q,%q)", gotVersion, gotQueue, c.want, "orders")
			}
		})
	}
}

func TestPopSchemaGateHookPanicIsIgnoredAndAckStillWorks(t *testing.T) {
	msg, receiver := popGated(t, sampleBody(t, 0), "2", true, 0,
		WithOnSchemaVersionRejected(func(string, string) { panic("boom") }))
	if msg.RouteURN == "" {
		t.Fatal("message was not gated")
	}
	tr := newTestTransport(t, &fakeClient{sender: &fakeSender{}, receiver: receiver})
	if err := tr.Ack(context.Background(), msg); err != nil || len(receiver.accepted) != 1 {
		t.Errorf("Ack: err=%v accepted=%d, want the gated message accepted once", err, len(receiver.accepted))
	}
}

// A hostile bq_schema_version is bounded to 64 characters plus "..." in the hook value and the
// route URN; exactly 64 characters is left alone.
func TestPopSchemaGateTruncatesLongDeclaredVersion(t *testing.T) {
	long := strings.Repeat("9", 5000)
	var hooked string
	msg, _ := popGated(t, "not json", long, true, 0,
		WithOnSchemaVersionRejected(func(v, _ string) { hooked = v }))
	want := strings.Repeat("9", 64) + "..."
	if hooked != want {
		t.Errorf("hook value length = %d, want %d", len(hooked), len(want))
	}
	if route := unsupportedSchemaRoutePrefix + url.QueryEscape(want); msg.RouteURN != route {
		t.Errorf("RouteURN length = %d, want %d", len(msg.RouteURN), len(route))
	}
	exact := strings.Repeat("9", 64)
	msg, _ = popGated(t, "not json", exact, true, 0)
	if msg.RouteURN != unsupportedSchemaRoutePrefix+exact {
		t.Errorf("a 64-character value must not be truncated, got %q", msg.RouteURN)
	}
}

// Rewind hands the delivery back to the broker (AMQP released) instead of settling it.
func TestRewindReleasesTheDelivery(t *testing.T) {
	msg, receiver := popGated(t, "not json", "2", true, 0)
	tr, err := New(context.Background(), WithClient(&fakeClient{sender: &fakeSender{}, receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Rewind(context.Background(), msg); err != nil {
		t.Fatalf("Rewind: %v", err)
	}
	if len(receiver.released) != 1 || len(receiver.accepted) != 0 {
		t.Fatalf("released = %d, accepted = %d; want 1 and 0", len(receiver.released), len(receiver.accepted))
	}
}
