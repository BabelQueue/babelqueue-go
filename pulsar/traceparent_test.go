package pulsar

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pulsarsdk "github.com/apache/pulsar-client-go/pulsar"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const (
	testTraceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	testTracestate  = "vendor=value"
)

func newHeaderTransport(t *testing.T, p *fakeProducer, c *fakeConsumer) *Transport {
	t.Helper()
	tr, err := New(WithClient(&fakeClient{producer: p, consumer: c}))
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// PublishWithHeaders sends the unchanged envelope as the payload and carries traceparent /
// tracestate as message properties beside the bq- projection (ADR-0028, broker-bindings.md §5).
func TestPublishWithHeadersCarriesTraceContext(t *testing.T) {
	producer := &fakeProducer{}
	tr := newHeaderTransport(t, producer, &fakeConsumer{})
	body := sampleBody(t)

	if err := tr.PublishWithHeaders(context.Background(), "orders", body, map[string]string{
		"traceparent": testTraceparent,
		"tracestate":  testTracestate,
	}); err != nil {
		t.Fatal(err)
	}
	if len(producer.sent) != 1 {
		t.Fatalf("sent %d, want 1", len(producer.sent))
	}
	m := producer.sent[0]
	if string(m.Payload) != body {
		t.Error("payload must be the unchanged envelope")
	}
	if strings.Contains(string(m.Payload), "traceparent") {
		t.Error("traceparent must not appear in the envelope body")
	}
	if m.Properties["traceparent"] != testTraceparent || m.Properties["tracestate"] != testTracestate {
		t.Errorf("trace properties = %v", m.Properties)
	}
	if m.Properties["bq-job"] != urn || m.Properties["bq-attempts"] != "0" || m.Properties["bq-schema-version"] != "1" {
		t.Errorf("bq- projection lost: %v", m.Properties)
	}

	plain := &fakeProducer{}
	if err := newHeaderTransport(t, plain, &fakeConsumer{}).Publish(context.Background(), "orders", body); err != nil {
		t.Fatal(err)
	}
	if string(plain.sent[0].Payload) != string(m.Payload) {
		t.Error("PublishWithHeaders and Publish must send byte-identical payloads")
	}
}

// A bq- contract property wins a key collision; blank keys/values are skipped; an empty map
// equals Publish; an undecodable body still carries the out-of-band headers.
func TestPublishWithHeadersContractPropertiesWin(t *testing.T) {
	producer := &fakeProducer{}
	tr := newHeaderTransport(t, producer, &fakeConsumer{})
	body := sampleBody(t)

	if err := tr.PublishWithHeaders(context.Background(), "orders", body, map[string]string{
		"bq-job":            "urn:evil",
		"bq-attempts":       "99",
		"bq-schema-version": "2",
		"":                  "no-key",
		"blank":             "",
		"traceparent":       testTraceparent,
		"bq-replay-bypass":  "1",
	}); err != nil {
		t.Fatal(err)
	}
	p := producer.sent[0].Properties
	if p["bq-job"] != urn || p["bq-attempts"] != "0" || p["bq-schema-version"] != "1" {
		t.Errorf("a contract property was clobbered: %v", p)
	}
	if _, ok := p[""]; ok {
		t.Error("blank key written")
	}
	if _, ok := p["blank"]; ok {
		t.Error("blank value written")
	}
	if p["traceparent"] != testTraceparent || p["bq-replay-bypass"] != "1" {
		t.Errorf("out-of-band properties missing: %v", p)
	}

	for _, hs := range []map[string]string{nil, {}} {
		pa, pb := &fakeProducer{}, &fakeProducer{}
		if err := newHeaderTransport(t, pa, &fakeConsumer{}).PublishWithHeaders(context.Background(), "orders", body, hs); err != nil {
			t.Fatal(err)
		}
		if err := newHeaderTransport(t, pb, &fakeConsumer{}).Publish(context.Background(), "orders", body); err != nil {
			t.Fatal(err)
		}
		if len(pa.sent[0].Properties) != len(pb.sent[0].Properties) {
			t.Errorf("empty headers must behave like Publish: %v vs %v", pa.sent[0].Properties, pb.sent[0].Properties)
		}
	}

	bad := &fakeProducer{}
	if err := newHeaderTransport(t, bad, &fakeConsumer{}).PublishWithHeaders(context.Background(), "orders", "not json", map[string]string{"traceparent": testTraceparent}); err != nil {
		t.Fatal(err)
	}
	if got := bad.sent[0].Properties; len(got) != 1 || got["traceparent"] != testTraceparent {
		t.Errorf("undecodable body properties = %v", got)
	}
}

// inject -> publish -> receive: the handler reads the same traceparent through
// HeadersFromContext and no bq- contract property leaks into the headers map.
func TestTraceparentRoundTripReachesHandler(t *testing.T) {
	producer := &fakeProducer{}
	consumer := &fakeConsumer{}
	tr := newHeaderTransport(t, producer, consumer)
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))

	var got map[string]string
	app.Handle(urn, func(ctx context.Context, _ babelqueue.Envelope) error {
		got = babelqueue.HeadersFromContext(ctx)
		return nil
	})
	if _, err := app.PublishWithHeaders(context.Background(), urn, map[string]any{"order_id": 1042},
		map[string]string{"traceparent": testTraceparent, "tracestate": testTracestate}); err != nil {
		t.Fatal(err)
	}
	sent := producer.sent[0]
	consumer.messages = []pulsarsdk.Message{fakeMessage{payload: sent.Payload, properties: sent.Properties}}

	n, err := app.Drain(context.Background(), "orders", 1)
	if err != nil || n != 1 {
		t.Fatalf("drain = %d, %v", n, err)
	}
	if got["traceparent"] != testTraceparent || got["tracestate"] != testTracestate {
		t.Errorf("handler headers = %v", got)
	}
	for k := range got {
		if strings.HasPrefix(k, "bq-") {
			t.Errorf("contract property %q leaked into the headers map", k)
		}
	}
	if len(consumer.acked) != 1 {
		t.Errorf("message not acked")
	}
}

// Pop surfaces the out-of-band properties (the replay-bypass marker included), stays header-less
// when there are none, and also surfaces them on a version-gated delivery.
func TestPopSurfacesOutOfBandHeaders(t *testing.T) {
	body := sampleBody(t)
	msg := fakeMessage{payload: []byte(body), properties: map[string]string{
		"bq-job": urn, "bq-attempts": "0",
		"traceparent": testTraceparent, "bq-replay-bypass": "1", "empty": "",
	}}
	tr := newHeaderTransport(t, &fakeProducer{}, &fakeConsumer{messages: []pulsarsdk.Message{msg}})
	got, err := tr.Pop(context.Background(), "orders", time.Second)
	if err != nil || got == nil {
		t.Fatalf("pop = %v, %v", got, err)
	}
	if len(got.Headers) != 2 || got.Headers["traceparent"] != testTraceparent || got.Headers["bq-replay-bypass"] != "1" {
		t.Errorf("headers = %v", got.Headers)
	}

	plain := fakeMessage{payload: []byte(body), properties: map[string]string{"bq-job": urn}}
	tr2 := newHeaderTransport(t, &fakeProducer{}, &fakeConsumer{messages: []pulsarsdk.Message{plain}})
	got2, _ := tr2.Pop(context.Background(), "orders", time.Second)
	if got2 == nil || got2.Headers != nil {
		t.Errorf("a property-less delivery must surface nil Headers, got %v", got2)
	}

	gated := fakeMessage{payload: []byte(body), properties: map[string]string{"bq-schema-version": "2", "traceparent": testTraceparent}}
	tr3 := newHeaderTransport(t, &fakeProducer{}, &fakeConsumer{messages: []pulsarsdk.Message{gated}})
	got3, _ := tr3.Pop(context.Background(), "orders", time.Second)
	if got3 == nil || len(got3.Headers) != 1 || got3.Headers["traceparent"] != testTraceparent {
		t.Errorf("gated delivery headers = %v", got3)
	}
}

// dlqEnvelope is a dead-lettered envelope for Redrive (job = urn, original queue "orders").
func dlqEnvelope(t *testing.T) string {
	t.Helper()
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := babelqueue.Annotate(env, "failed", "orders", 3, errors.New("boom")).Encode()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func redriveBypass(t *testing.T, props map[string]string) (babelqueue.RedriveResult, *fakeProducer) {
	t.Helper()
	producer := &fakeProducer{}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{fakeMessage{payload: []byte(dlqEnvelope(t)), properties: props}}}
	tr := newHeaderTransport(t, producer, consumer)
	res, err := babelqueue.Redrive(context.Background(), tr, "orders.dlq",
		babelqueue.RedriveOptions{Bypass: true, Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 || len(res.Items) != 1 || !res.Items[0].Bypassed {
		t.Fatalf("want one bypassed redrive, got %+v", res)
	}
	if len(producer.sent) != 1 {
		t.Fatalf("sent %d, want 1", len(producer.sent))
	}
	return res, producer
}

// Redrive{Bypass: true} keeps the authoritative routing property: a DLQ record routed on a bq-job
// that differs from the body job is redriven with that same bq-job (as a plain redrive does via
// PublishRouted), and carries the bypass marker.
func TestRedriveBypassKeepsRoutingProperty(t *testing.T) {
	const routed = "urn:babel:orders:created:v2"
	_, producer := redriveBypass(t, map[string]string{"bq-job": routed, "bq-attempts": "3"})
	p := producer.sent[0].Properties
	if p["bq-job"] != routed {
		t.Errorf("bq-job = %q, want the pinned route %q", p["bq-job"], routed)
	}
	if p["bq-replay-bypass"] != "1" {
		t.Errorf("bq-replay-bypass missing: %v", p)
	}
	if p["bq-attempts"] != "0" {
		t.Errorf("bq-attempts = %q, want the reset 0", p["bq-attempts"])
	}
}

// Redrive{Bypass: true} keeps a version-gated record gated: the original bq-schema-version (2)
// and bq-job are written back, not the body's 1, so the consumer's gate (§5.7) still rejects it.
func TestRedriveBypassKeepsVersionGate(t *testing.T) {
	_, producer := redriveBypass(t, map[string]string{"bq-schema-version": "2", "bq-job": urn})
	p := producer.sent[0].Properties
	if p["bq-schema-version"] != "2" {
		t.Errorf("bq-schema-version = %q, the version gate was bypassed", p["bq-schema-version"])
	}
	if p["bq-job"] != urn || p["bq-replay-bypass"] != "1" {
		t.Errorf("properties = %v", p)
	}
}

// PublishFromWithHeaders without a from (or without a route) derives bq-job from the body.
func TestPublishFromWithHeadersWithoutRoute(t *testing.T) {
	producer := &fakeProducer{}
	tr := newHeaderTransport(t, producer, &fakeConsumer{})
	if err := tr.PublishFromWithHeaders(context.Background(), "orders", sampleBody(t), nil,
		map[string]string{"traceparent": testTraceparent, "bq-job": "urn:evil"}); err != nil {
		t.Fatal(err)
	}
	p := producer.sent[0].Properties
	if p["bq-job"] != urn || p["traceparent"] != testTraceparent {
		t.Errorf("properties = %v", p)
	}
}

// The optional bq-delay mirror (§5.4) is a contract property: a caller cannot write it, and a
// message from another SDK carrying it does not leak it into the headers map.
func TestBqDelayIsNeitherWrittenNorSurfaced(t *testing.T) {
	producer := &fakeProducer{}
	tr := newHeaderTransport(t, producer, &fakeConsumer{})
	if err := tr.PublishWithHeaders(context.Background(), "orders", sampleBody(t), map[string]string{"bq-delay": "5000", "traceparent": testTraceparent}); err != nil {
		t.Fatal(err)
	}
	if _, ok := producer.sent[0].Properties["bq-delay"]; ok {
		t.Error("bq-delay written from caller headers")
	}
	got := outOfBandFromProperties(map[string]string{"bq-delay": "5000", "bq-attempts": "1", "traceparent": testTraceparent})
	if len(got) != 1 || got["traceparent"] != testTraceparent {
		t.Errorf("surfaced headers = %v", got)
	}
}
