package artemis

import (
	"context"
	"errors"
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
