package artemis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const (
	testTraceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	testTracestate  = "vendor=value"
)

func bodyOf(m *amqp.Message) string {
	s, _ := m.Value.(string)
	return s
}

// PublishWithHeaders sends the unchanged envelope as the AMQP value and carries traceparent /
// tracestate as string application properties beside the bq_ projection (ADR-0028, §7).
func TestPublishWithHeadersCarriesTraceContext(t *testing.T) {
	sender := &fakeSender{}
	tr := newTestTransport(t, &fakeClient{sender: sender})
	body := sampleBody(t, 0)

	if err := tr.PublishWithHeaders(context.Background(), "orders", body, map[string]string{
		"traceparent": testTraceparent,
		"tracestate":  testTracestate,
	}); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent %d, want 1", len(sender.sent))
	}
	m := sender.sent[0]
	if bodyOf(m) != body {
		t.Error("message body must be the unchanged envelope")
	}
	if strings.Contains(bodyOf(m), "traceparent") {
		t.Error("traceparent must not appear in the envelope body")
	}
	p := m.ApplicationProperties
	if p["traceparent"] != testTraceparent || p["tracestate"] != testTracestate {
		t.Errorf("trace properties = %v", p)
	}
	if p["bq_app_id"] != "babelqueue" || p["bq_attempts"] != "0" || p["bq_schema_version"] != "1" {
		t.Errorf("bq_ projection lost: %v", p)
	}
	if m.Annotations[jmsTypeKey] != urn || m.Properties == nil || m.Properties.CorrelationID != "trace-1" {
		t.Errorf("native projection lost: %#v %#v", m.Annotations, m.Properties)
	}

	plain := &fakeSender{}
	if err := newTestTransport(t, &fakeClient{sender: plain}).Publish(context.Background(), "orders", body); err != nil {
		t.Fatal(err)
	}
	if bodyOf(plain.sent[0]) != bodyOf(m) {
		t.Error("PublishWithHeaders and Publish must send byte-identical bodies")
	}
}

// A bq_ contract property wins a key collision; blank keys/values are skipped; an empty map
// equals Publish; an undecodable body still carries the out-of-band headers.
func TestPublishWithHeadersContractPropertiesWin(t *testing.T) {
	sender := &fakeSender{}
	tr := newTestTransport(t, &fakeClient{sender: sender})
	body := sampleBody(t, 0)

	if err := tr.PublishWithHeaders(context.Background(), "orders", body, map[string]string{
		"bq_attempts":       "99",
		"bq_schema_version": "2",
		"bq_app_id":         "evil",
		"":                  "no-key",
		"blank":             "",
		"traceparent":       testTraceparent,
	}); err != nil {
		t.Fatal(err)
	}
	p := sender.sent[0].ApplicationProperties
	if p["bq_attempts"] != "0" || p["bq_schema_version"] != "1" || p["bq_app_id"] != "babelqueue" {
		t.Errorf("a contract property was clobbered: %v", p)
	}
	if _, ok := p[""]; ok {
		t.Error("blank key written")
	}
	if _, ok := p["blank"]; ok {
		t.Error("blank value written")
	}
	if p["traceparent"] != testTraceparent {
		t.Errorf("traceparent missing: %v", p)
	}

	for _, hs := range []map[string]string{nil, {}} {
		sa, sb := &fakeSender{}, &fakeSender{}
		if err := newTestTransport(t, &fakeClient{sender: sa}).PublishWithHeaders(context.Background(), "orders", body, hs); err != nil {
			t.Fatal(err)
		}
		if err := newTestTransport(t, &fakeClient{sender: sb}).Publish(context.Background(), "orders", body); err != nil {
			t.Fatal(err)
		}
		if len(sa.sent[0].ApplicationProperties) != len(sb.sent[0].ApplicationProperties) {
			t.Errorf("empty headers must behave like Publish: %v vs %v", sa.sent[0].ApplicationProperties, sb.sent[0].ApplicationProperties)
		}
	}

	bad := &fakeSender{}
	if err := newTestTransport(t, &fakeClient{sender: bad}).PublishWithHeaders(context.Background(), "orders", "not json", map[string]string{"traceparent": testTraceparent}); err != nil {
		t.Fatal(err)
	}
	if got := bad.sent[0].ApplicationProperties; len(got) != 1 || got["traceparent"] != testTraceparent {
		t.Errorf("undecodable body properties = %v", got)
	}
}

// inject -> publish -> receive: the handler reads the same traceparent through
// HeadersFromContext and no bq_ contract property leaks into the headers map.
func TestTraceparentRoundTripReachesHandler(t *testing.T) {
	sender := &fakeSender{}
	receiver := &fakeReceiver{}
	tr := newTestTransport(t, &fakeClient{sender: sender, receiver: receiver})
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
	receiver.messages = []*amqp.Message{sender.sent[0]}

	n, err := app.Drain(context.Background(), "orders", 1)
	if err != nil || n != 1 {
		t.Fatalf("drain = %d, %v", n, err)
	}
	if got["traceparent"] != testTraceparent || got["tracestate"] != testTracestate {
		t.Errorf("handler headers = %v", got)
	}
	for k := range got {
		if strings.HasPrefix(k, "bq_") {
			t.Errorf("contract property %q leaked into the headers map", k)
		}
	}
	if len(receiver.accepted) != 1 {
		t.Error("message not accepted")
	}
}

// Pop surfaces string out-of-band properties, skips non-text ones, stays header-less when there
// are none, and also surfaces them on a version-gated delivery.
func TestPopSurfacesOutOfBandHeaders(t *testing.T) {
	body := sampleBody(t, 0)
	m := incoming(body, urn, 0)
	m.ApplicationProperties = map[string]any{
		"bq_attempts": "0", "bq_app_id": "babelqueue",
		"traceparent": testTraceparent, "bq-replay-bypass": "1", "count": int64(3), "empty": "",
	}
	receiver := &fakeReceiver{messages: []*amqp.Message{m}}
	tr := newTestTransport(t, &fakeClient{receiver: receiver})
	got, err := tr.Pop(context.Background(), "orders", time.Second)
	if err != nil || got == nil {
		t.Fatalf("pop = %v, %v", got, err)
	}
	if len(got.Headers) != 2 || got.Headers["traceparent"] != testTraceparent || got.Headers["bq-replay-bypass"] != "1" {
		t.Errorf("headers = %v", got.Headers)
	}

	plain := incoming(body, urn, 0)
	plain.ApplicationProperties = map[string]any{"bq_attempts": "0"}
	tr2 := newTestTransport(t, &fakeClient{receiver: &fakeReceiver{messages: []*amqp.Message{plain}}})
	got2, _ := tr2.Pop(context.Background(), "orders", time.Second)
	if got2 == nil || got2.Headers != nil {
		t.Errorf("a property-less delivery must surface nil Headers, got %v", got2)
	}

	gated := incoming(body, urn, 0)
	gated.ApplicationProperties = map[string]any{"bq_schema_version": "2", "traceparent": testTraceparent}
	tr3 := newTestTransport(t, &fakeClient{receiver: &fakeReceiver{messages: []*amqp.Message{gated}}})
	got3, _ := tr3.Pop(context.Background(), "orders", time.Second)
	if got3 == nil || len(got3.Headers) != 1 || got3.Headers["traceparent"] != testTraceparent {
		t.Errorf("gated delivery headers = %v", got3)
	}
}

// Redrive{Bypass: true} over Artemis stamps bq-replay-bypass on the redriven message and reports
// Bypassed; the §7 projection is re-derived from the reset body and the body is the reset envelope.
func TestRedriveBypassStampsMarker(t *testing.T) {
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"), babelqueue.WithTraceID("trace-1"))
	if err != nil {
		t.Fatal(err)
	}
	dead, err := babelqueue.Annotate(env, "failed", "orders", 3, errors.New("boom")).Encode()
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{}
	receiver := &fakeReceiver{messages: []*amqp.Message{incoming(string(dead), urn, 0)}}
	tr := newTestTransport(t, &fakeClient{sender: sender, receiver: receiver})

	res, err := babelqueue.Redrive(context.Background(), tr, "orders.dlq",
		babelqueue.RedriveOptions{Bypass: true, Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 || len(res.Items) != 1 || !res.Items[0].Bypassed {
		t.Fatalf("want one bypassed redrive, got %+v", res)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent %d, want 1", len(sender.sent))
	}
	p := sender.sent[0].ApplicationProperties
	if p["bq-replay-bypass"] != "1" || p["bq_attempts"] != "0" || p["bq_app_id"] != "babelqueue" {
		t.Errorf("application properties = %v", p)
	}
	redriven, err := babelqueue.Decode([]byte(bodyOf(sender.sent[0])))
	if err != nil || redriven.DeadLetter != nil || redriven.Attempts != 0 {
		t.Errorf("redriven body = %v, %v", redriven, err)
	}
	if len(receiver.accepted) != 1 {
		t.Errorf("DLQ message not accepted after a successful redrive")
	}
}

// The optional bq_delay mirror (§7.4) is a contract property: a caller cannot write it, and a
// message from another SDK carrying it does not leak it into the headers map.
func TestBqDelayIsNeitherWrittenNorSurfaced(t *testing.T) {
	sender := &fakeSender{}
	tr := newTestTransport(t, &fakeClient{sender: sender})
	if err := tr.PublishWithHeaders(context.Background(), "orders", sampleBody(t, 0), map[string]string{"bq_delay": "5000", "traceparent": testTraceparent}); err != nil {
		t.Fatal(err)
	}
	if _, ok := sender.sent[0].ApplicationProperties["bq_delay"]; ok {
		t.Error("bq_delay written from caller headers")
	}
	got := outOfBandFromProperties(map[string]any{"bq_delay": "5000", "traceparent": testTraceparent})
	if len(got) != 1 || got["traceparent"] != testTraceparent {
		t.Errorf("surfaced headers = %v", got)
	}
}
