package azureservicebus

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const urn = "urn:babel:orders:created"

type fakeSender struct{ sent []*azservicebus.Message }

func (f *fakeSender) SendMessage(_ context.Context, m *azservicebus.Message, _ *azservicebus.SendMessageOptions) error {
	f.sent = append(f.sent, m)
	return nil
}

type fakeReceiver struct {
	messages  []*azservicebus.ReceivedMessage
	completed []*azservicebus.ReceivedMessage
}

func (f *fakeReceiver) ReceiveMessages(_ context.Context, max int, _ *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	if len(f.messages) == 0 {
		return nil, nil
	}
	n := max
	if n > len(f.messages) {
		n = len(f.messages)
	}
	out := f.messages[:n]
	f.messages = f.messages[n:]
	return out, nil
}

func (f *fakeReceiver) CompleteMessage(_ context.Context, m *azservicebus.ReceivedMessage, _ *azservicebus.CompleteMessageOptions) error {
	f.completed = append(f.completed, m)
	return nil
}

type fakeClient struct {
	sender   *fakeSender
	receiver *fakeReceiver
}

func (f *fakeClient) NewSender(string) (Sender, error)     { return f.sender, nil }
func (f *fakeClient) NewReceiver(string) (Receiver, error) { return f.receiver, nil }

func sampleBody(t *testing.T) string {
	t.Helper()
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func TestMessageProjection(t *testing.T) {
	body := sampleBody(t)
	env, _ := babelqueue.Decode([]byte(body))
	m := message(body)

	if got := derefStr(m.Subject); got != urn {
		t.Errorf("Subject = %q, want %q", got, urn)
	}
	if got := derefStr(m.CorrelationID); got != env.TraceID {
		t.Errorf("CorrelationID = %q, want %q", got, env.TraceID)
	}
	if got := derefStr(m.MessageID); got != env.Meta.ID {
		t.Errorf("MessageID = %q, want %q", got, env.Meta.ID)
	}
	if got := derefStr(m.ContentType); got != "application/json" {
		t.Errorf("ContentType = %q", got)
	}
	if m.ApplicationProperties["bq-schema-version"] != env.Meta.SchemaVersion {
		t.Errorf("bq-schema-version = %v", m.ApplicationProperties["bq-schema-version"])
	}
	if m.ApplicationProperties["bq-source-lang"] != env.Meta.Lang {
		t.Errorf("bq-source-lang = %v", m.ApplicationProperties["bq-source-lang"])
	}
	if m.ApplicationProperties["bq-created-at"] != env.Meta.CreatedAt {
		t.Errorf("bq-created-at = %v", m.ApplicationProperties["bq-created-at"])
	}
}

func TestPublishSendsProjectedMessage(t *testing.T) {
	sender := &fakeSender{}
	tr, err := New(WithClient(&fakeClient{sender: sender, receiver: &fakeReceiver{}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Publish(context.Background(), "orders", sampleBody(t)); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(sender.sent))
	}
	if got := derefStr(sender.sent[0].Subject); got != urn {
		t.Errorf("Subject = %q, want %q", got, urn)
	}
}

func TestPopReconcilesAttemptsAndAcks(t *testing.T) {
	rm := &azservicebus.ReceivedMessage{Body: []byte(sampleBody(t)), DeliveryCount: 3}
	receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{rm}}
	tr, _ := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: receiver}))

	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if msg == nil {
		t.Fatal("Pop returned nil")
	}
	env, _ := babelqueue.Decode([]byte(msg.Body))
	if env.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (DeliveryCount-1)", env.Attempts)
	}

	if err := tr.Ack(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(receiver.completed) != 1 {
		t.Errorf("completed %d, want 1", len(receiver.completed))
	}
}

func TestPopEmptyReturnsNil(t *testing.T) {
	tr, _ := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: &fakeReceiver{}}))
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg != nil {
		t.Errorf("Pop = (%v, %v), want (nil, nil)", msg, err)
	}
}

func TestReconcileNeverLowersRuntimeCount(t *testing.T) {
	env, err := babelqueue.Make(urn, map[string]any{}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	env.Attempts = 5
	b, _ := env.Encode()

	out := reconcileAttempts(string(b), 2) // native-1 = 1 < body 5 → body wins
	decoded, _ := babelqueue.Decode([]byte(out))
	if decoded.Attempts != 5 {
		t.Errorf("attempts = %d, want 5 (runtime count not lowered)", decoded.Attempts)
	}
}

func TestReconcileFirstDeliveryUnchanged(t *testing.T) {
	body := sampleBody(t)
	if reconcileAttempts(body, 1) != body {
		t.Error("first delivery (DeliveryCount=1) should leave the body unchanged")
	}
}

func TestAckNoopWithoutHandle(t *testing.T) {
	tr, _ := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: &fakeReceiver{}}))
	if err := tr.Ack(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders", Handle: nil}); err != nil {
		t.Errorf("Ack with no handle returned %v", err)
	}
}

func TestNewRequiresClientOrConnString(t *testing.T) {
	if _, err := New(); err == nil {
		t.Error("New() with no client/connection-string should error")
	}
}
