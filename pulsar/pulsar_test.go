package pulsar

import (
	"context"
	"testing"
	"time"

	pulsarsdk "github.com/apache/pulsar-client-go/pulsar"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const urn = "urn:babel:orders:created"

type fakeProducer struct {
	sent []*pulsarsdk.ProducerMessage
}

func (f *fakeProducer) Send(_ context.Context, m *pulsarsdk.ProducerMessage) (pulsarsdk.MessageID, error) {
	f.sent = append(f.sent, m)
	return nil, nil
}

// fakeMessage embeds the pulsar.Message interface so it satisfies it; only the two methods
// the transport calls are implemented (the rest would panic, but are never invoked).
type fakeMessage struct {
	pulsarsdk.Message
	payload         []byte
	redeliveryCount uint32
}

func (m fakeMessage) Payload() []byte         { return m.payload }
func (m fakeMessage) RedeliveryCount() uint32 { return m.redeliveryCount }

type fakeConsumer struct {
	messages []pulsarsdk.Message
	acked    []pulsarsdk.Message
	nacked   []pulsarsdk.Message
}

func (f *fakeConsumer) Receive(ctx context.Context) (pulsarsdk.Message, error) {
	if len(f.messages) == 0 {
		<-ctx.Done() // emulate a blocking receive that ends when the poll deadline fires
		return nil, ctx.Err()
	}
	m := f.messages[0]
	f.messages = f.messages[1:]
	return m, nil
}

func (f *fakeConsumer) Ack(m pulsarsdk.Message) error { f.acked = append(f.acked, m); return nil }
func (f *fakeConsumer) Nack(m pulsarsdk.Message)      { f.nacked = append(f.nacked, m) }

type fakeClient struct {
	producer *fakeProducer
	consumer *fakeConsumer
}

func (f *fakeClient) CreateProducer(string) (Producer, error) { return f.producer, nil }
func (f *fakeClient) Subscribe(string) (Consumer, error)      { return f.consumer, nil }

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

func TestPropertiesProjection(t *testing.T) {
	body := sampleBody(t)
	env, _ := babelqueue.Decode([]byte(body))
	props := properties(env)

	if props["bq-job"] != urn {
		t.Errorf("bq-job = %q, want %q", props["bq-job"], urn)
	}
	if props["bq-trace-id"] != env.TraceID {
		t.Errorf("bq-trace-id = %q, want %q", props["bq-trace-id"], env.TraceID)
	}
	if props["bq-message-id"] != env.Meta.ID {
		t.Errorf("bq-message-id = %q, want %q", props["bq-message-id"], env.Meta.ID)
	}
	if props["bq-schema-version"] != "1" {
		t.Errorf("bq-schema-version = %q, want \"1\"", props["bq-schema-version"])
	}
	if props["bq-source-lang"] != env.Meta.Lang {
		t.Errorf("bq-source-lang = %q, want %q", props["bq-source-lang"], env.Meta.Lang)
	}
	if props["bq-attempts"] != "0" {
		t.Errorf("bq-attempts = %q, want \"0\"", props["bq-attempts"])
	}
}

func TestPublishSendsProjectedMessage(t *testing.T) {
	producer := &fakeProducer{}
	tr, err := New(WithClient(&fakeClient{producer: producer, consumer: &fakeConsumer{}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Publish(context.Background(), "orders", sampleBody(t)); err != nil {
		t.Fatal(err)
	}
	if len(producer.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(producer.sent))
	}
	sent := producer.sent[0]
	if sent.Properties["bq-job"] != urn {
		t.Errorf("bq-job = %q, want %q", sent.Properties["bq-job"], urn)
	}
	if env, _ := babelqueue.Decode(sent.Payload); env.Job != urn {
		t.Errorf("payload job = %q, want %q", env.Job, urn)
	}
}

func TestPopReconcilesAttemptsAndAcks(t *testing.T) {
	msg := fakeMessage{payload: []byte(sampleBody(t)), redeliveryCount: 2}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{msg}}
	tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: consumer}))

	got, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("Pop returned nil")
	}
	env, _ := babelqueue.Decode([]byte(got.Body))
	if env.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (RedeliveryCount, no -1)", env.Attempts)
	}

	if err := tr.Ack(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if len(consumer.acked) != 1 {
		t.Errorf("acked %d, want 1", len(consumer.acked))
	}
}

func TestPopFirstDeliveryZeroAttempts(t *testing.T) {
	msg := fakeMessage{payload: []byte(sampleBody(t)), redeliveryCount: 0}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{msg}}
	tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: consumer}))

	got, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || got == nil {
		t.Fatalf("Pop = (%v, %v)", got, err)
	}
	env, _ := babelqueue.Decode([]byte(got.Body))
	if env.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", env.Attempts)
	}
}

func TestPopEmptyReturnsNil(t *testing.T) {
	tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: &fakeConsumer{}}))
	msg, err := tr.Pop(context.Background(), "orders", 20*time.Millisecond)
	if err != nil || msg != nil {
		t.Errorf("Pop = (%v, %v), want (nil, nil)", msg, err)
	}
}

func TestReconcileNeverLowersRuntimeCount(t *testing.T) {
	// Republish-driven retry carried attempts=5 in the body; redelivery count is only 1.
	env, err := babelqueue.Make(urn, map[string]any{}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	env.Attempts = 5
	b, _ := env.Encode()

	out := reconcileAttempts(string(b), 1)
	decoded, _ := babelqueue.Decode([]byte(out))
	if decoded.Attempts != 5 {
		t.Errorf("attempts = %d, want 5 (runtime count not lowered)", decoded.Attempts)
	}
}

func TestReconcileFirstDeliveryUnchanged(t *testing.T) {
	body := sampleBody(t)
	if reconcileAttempts(body, 0) != body {
		t.Error("first delivery (RedeliveryCount=0) should leave the body unchanged")
	}
}

func TestAckNoopWithoutHandle(t *testing.T) {
	tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: &fakeConsumer{}}))
	if err := tr.Ack(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders", Handle: nil}); err != nil {
		t.Errorf("Ack with no handle returned %v", err)
	}
}

func TestNewRequiresClientOrURL(t *testing.T) {
	if _, err := New(); err == nil {
		t.Error("New() with no client/URL should error")
	}
}

func TestOptionsConfigureTransport(t *testing.T) {
	tr := newTransport(
		WithURL("pulsar://host:6650"),
		WithSubscription("orders-sub"),
		WithSubscriptionType(pulsarsdk.Exclusive),
		WithTopicPrefix("persistent://public/default/"),
		WithMaxWaitTime(2*time.Second),
	)
	if tr.url != "pulsar://host:6650" {
		t.Errorf("url = %q", tr.url)
	}
	if tr.subscription != "orders-sub" {
		t.Errorf("subscription = %q", tr.subscription)
	}
	if tr.consumerType != pulsarsdk.Exclusive {
		t.Errorf("consumerType = %v", tr.consumerType)
	}
	if tr.topicPrefix != "persistent://public/default/" {
		t.Errorf("topicPrefix = %q", tr.topicPrefix)
	}
	if tr.maxWait != 2*time.Second {
		t.Errorf("maxWait = %v", tr.maxWait)
	}
}

func TestDefaultsAreSharedSubscription(t *testing.T) {
	tr := newTransport()
	if tr.subscription != "babelqueue" {
		t.Errorf("default subscription = %q, want %q", tr.subscription, "babelqueue")
	}
	if tr.consumerType != pulsarsdk.Shared {
		t.Errorf("default consumerType = %v, want Shared", tr.consumerType)
	}
}

func TestTopicPrefixApplied(t *testing.T) {
	c := pulsarClient{topicPrefix: "persistent://public/default/"}
	if got := c.topic("orders"); got != "persistent://public/default/orders" {
		t.Errorf("topic = %q", got)
	}
	bare := pulsarClient{}
	if got := bare.topic("orders"); got != "orders" {
		t.Errorf("bare topic = %q, want %q", got, "orders")
	}
}

func TestMessageWithGarbageBodyOmitsProperties(t *testing.T) {
	m := message("not-json")
	if string(m.Payload) != "not-json" {
		t.Errorf("payload = %q", string(m.Payload))
	}
	if m.Properties != nil {
		t.Errorf("properties = %v, want nil for a non-envelope body", m.Properties)
	}
}

func TestProducerAndConsumerAreCached(t *testing.T) {
	tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: &fakeConsumer{
		messages: []pulsarsdk.Message{
			fakeMessage{payload: []byte(sampleBody(t))},
			fakeMessage{payload: []byte(sampleBody(t))},
		},
	}}))
	ctx := context.Background()

	_ = tr.Publish(ctx, "orders", sampleBody(t))
	_ = tr.Publish(ctx, "orders", sampleBody(t))
	if len(tr.producers) != 1 {
		t.Errorf("producers cached = %d, want 1", len(tr.producers))
	}

	_, _ = tr.Pop(ctx, "orders", 0)
	_, _ = tr.Pop(ctx, "orders", 0)
	if len(tr.consumers) != 1 {
		t.Errorf("consumers cached = %d, want 1", len(tr.consumers))
	}
}
