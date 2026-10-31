package azureservicebus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const (
	testTraceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	testTracestate  = "vendor=value"
)

// PublishWithHeaders sends the unchanged envelope and carries traceparent / tracestate as string
// application properties beside the bq- projection (ADR-0028, broker-bindings.md §4).
func TestPublishWithHeadersCarriesTraceContext(t *testing.T) {
	sender := &fakeSender{}
	tr, err := New(WithClient(&fakeClient{sender: sender, receiver: &fakeReceiver{}}))
	if err != nil {
		t.Fatal(err)
	}
	body := sampleBody(t)

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
	if string(m.Body) != body {
		t.Error("message body must be the unchanged envelope")
	}
	if strings.Contains(string(m.Body), "traceparent") {
		t.Error("traceparent must not appear in the envelope body")
	}
	p := m.ApplicationProperties
	if p["traceparent"] != testTraceparent || p["tracestate"] != testTracestate {
		t.Errorf("trace properties = %v", p)
	}
	if p["bq-schema-version"] != 1 || p["bq-source-lang"] == nil || p["bq-created-at"] == nil {
		t.Errorf("bq- projection lost: %v", p)
	}
	if m.Subject == nil || *m.Subject != urn {
		t.Errorf("native projection lost: subject = %v", m.Subject)
	}

	plain := &fakeSender{}
	tr2, _ := New(WithClient(&fakeClient{sender: plain, receiver: &fakeReceiver{}}))
	if err := tr2.Publish(context.Background(), "orders", body); err != nil {
		t.Fatal(err)
	}
	if string(plain.sent[0].Body) != string(m.Body) {
		t.Error("PublishWithHeaders and Publish must send byte-identical bodies")
	}
}

// A bq- contract property wins a key collision; blank keys/values are skipped; an empty map
// equals Publish; an undecodable body still carries the out-of-band headers.
func TestPublishWithHeadersContractPropertiesWin(t *testing.T) {
	newTr := func(s *fakeSender) *Transport {
		tr, err := New(WithClient(&fakeClient{sender: s, receiver: &fakeReceiver{}}))
		if err != nil {
			t.Fatal(err)
		}
		return tr
	}
	sender := &fakeSender{}
	body := sampleBody(t)

	if err := newTr(sender).PublishWithHeaders(context.Background(), "orders", body, map[string]string{
		"bq-schema-version": "2",
		"bq-source-lang":    "evil",
		"bq-created-at":     "0",
		"":                  "no-key",
		"blank":             "",
		"traceparent":       testTraceparent,
	}); err != nil {
		t.Fatal(err)
	}
	p := sender.sent[0].ApplicationProperties
	if p["bq-schema-version"] != 1 || p["bq-source-lang"] == "evil" || p["bq-created-at"] == "0" {
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
		if err := newTr(sa).PublishWithHeaders(context.Background(), "orders", body, hs); err != nil {
			t.Fatal(err)
		}
		if err := newTr(sb).Publish(context.Background(), "orders", body); err != nil {
			t.Fatal(err)
		}
		if len(sa.sent[0].ApplicationProperties) != len(sb.sent[0].ApplicationProperties) {
			t.Errorf("empty headers must behave like Publish: %v vs %v", sa.sent[0].ApplicationProperties, sb.sent[0].ApplicationProperties)
		}
	}

	bad := &fakeSender{}
	if err := newTr(bad).PublishWithHeaders(context.Background(), "orders", "not json", map[string]string{"traceparent": testTraceparent}); err != nil {
		t.Fatal(err)
	}
	if got := bad.sent[0].ApplicationProperties; len(got) != 1 || got["traceparent"] != testTraceparent {
		t.Errorf("undecodable body properties = %v", got)
	}
}

// inject -> publish -> receive: the handler reads the same traceparent through
// HeadersFromContext and no bq- contract property leaks into the headers map.
func TestTraceparentRoundTripReachesHandler(t *testing.T) {
	sender := &fakeSender{}
	receiver := &fakeReceiver{}
	tr, err := New(WithClient(&fakeClient{sender: sender, receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}
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
	sent := sender.sent[0]
	receiver.messages = []*azservicebus.ReceivedMessage{{Body: sent.Body, ApplicationProperties: sent.ApplicationProperties, DeliveryCount: 1}}

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
	if len(receiver.completed) != 1 {
		t.Error("message not completed")
	}
}

// Pop surfaces string out-of-band properties, skips non-text ones, stays header-less when there
// are none, and also surfaces them on a version-gated delivery.
func TestPopSurfacesOutOfBandHeaders(t *testing.T) {
	pop := func(rm *azservicebus.ReceivedMessage) *babelqueue.ReceivedMessage {
		t.Helper()
		tr, err := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: &fakeReceiver{messages: []*azservicebus.ReceivedMessage{rm}}}))
		if err != nil {
			t.Fatal(err)
		}
		msg, err := tr.Pop(context.Background(), "orders", time.Second)
		if err != nil || msg == nil {
			t.Fatalf("pop = %v, %v", msg, err)
		}
		return msg
	}
	body := sampleBody(t)

	got := pop(&azservicebus.ReceivedMessage{Body: []byte(body), ApplicationProperties: map[string]any{
		"bq-schema-version": "1", "bq-source-lang": "go", "bq-created-at": int64(5),
		"traceparent": testTraceparent, "bq-replay-bypass": "1", "count": int64(3), "empty": "",
	}})
	if len(got.Headers) != 2 || got.Headers["traceparent"] != testTraceparent || got.Headers["bq-replay-bypass"] != "1" {
		t.Errorf("headers = %v", got.Headers)
	}

	if plain := pop(&azservicebus.ReceivedMessage{Body: []byte(body), ApplicationProperties: map[string]any{"bq-schema-version": "1"}}); plain.Headers != nil {
		t.Errorf("a property-less delivery must surface nil Headers, got %v", plain.Headers)
	}
	if none := pop(&azservicebus.ReceivedMessage{Body: []byte(body)}); none.Headers != nil {
		t.Errorf("a property-less delivery must surface nil Headers, got %v", none.Headers)
	}

	gated := pop(&azservicebus.ReceivedMessage{Body: []byte(body), ApplicationProperties: map[string]any{
		"bq-schema-version": "2", "traceparent": testTraceparent,
	}})
	if len(gated.Headers) != 1 || gated.Headers["traceparent"] != testTraceparent {
		t.Errorf("gated delivery headers = %v", gated.Headers)
	}
}

// nativeDeadLetterSettler is the optional native-settle variant of ADR-0039 §5.1, as the core App
// looks for it structurally (the core needs no import of this module).
type nativeDeadLetterSettler interface {
	SettleNativeDeadLetter(ctx context.Context, msg *babelqueue.ReceivedMessage, reason, description string) (deadLettered bool, err error)
}

var _ nativeDeadLetterSettler = (*Transport)(nil)

// With a receiver that has DeadLetterMessage, SettleNativeDeadLetter dead-letters natively and
// reports true; the reason and description reach the broker, the body is untouched.
func TestSettleNativeDeadLetterReportsTrueWhenDeadLettered(t *testing.T) {
	receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage("not json", "2", true)}}
	tr, err := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil {
		t.Fatalf("Pop: msg=%v err=%v", msg, err)
	}
	dl, err := tr.SettleNativeDeadLetter(context.Background(), msg, "unsupported schema version", "declares version 2")
	if err != nil || !dl {
		t.Fatalf("deadLettered = %v, err = %v; want true, nil", dl, err)
	}
	if len(receiver.deadLettered) != 1 || len(receiver.completed) != 0 || len(receiver.abandoned) != 0 {
		t.Fatalf("dead-lettered = %d, completed = %d, abandoned = %d; want 1, 0, 0",
			len(receiver.deadLettered), len(receiver.completed), len(receiver.abandoned))
	}
	if receiver.dlReasons[0] != "unsupported schema version" || receiver.dlDetails[0] != "declares version 2" {
		t.Errorf("reason/description = %q/%q", receiver.dlReasons[0], receiver.dlDetails[0])
	}
}

// A receiver without DeadLetterMessage falls back to Abandon: the call settles exactly like
// SettleNative did, and reports false (the SDK did not dead-letter it).
func TestSettleNativeDeadLetterReportsFalseOnAbandonFallback(t *testing.T) {
	receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage("not json", "2", true)}}
	tr, err := New(WithClient(&abandonOnlyClient{sender: &fakeSender{}, receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := tr.Pop(context.Background(), "orders", 0)
	dl, err := tr.SettleNativeDeadLetter(context.Background(), msg, "poison", "x")
	if err != nil || dl {
		t.Fatalf("deadLettered = %v, err = %v; want false, nil", dl, err)
	}
	if len(receiver.abandoned) != 1 || len(receiver.completed) != 0 || len(receiver.deadLettered) != 0 {
		t.Fatalf("abandoned = %d, completed = %d, dead-lettered = %d; want 1, 0, 0",
			len(receiver.abandoned), len(receiver.completed), len(receiver.deadLettered))
	}
}

// failingDeadLetterReceiver's DeadLetterMessage fails.
type failingDeadLetterReceiver struct{ *fakeReceiver }

func (failingDeadLetterReceiver) DeadLetterMessage(context.Context, *azservicebus.ReceivedMessage, *azservicebus.DeadLetterOptions) error {
	return errors.New("service bus unavailable")
}

type failingDeadLetterClient struct{ receiver *fakeReceiver }

func (c *failingDeadLetterClient) NewSender(string) (Sender, error) { return &fakeSender{}, nil }
func (c *failingDeadLetterClient) NewReceiver(string) (Receiver, error) {
	return failingDeadLetterReceiver{c.receiver}, nil
}

// A failed DeadLetterMessage is not a dead-letter: false with the error, and SettleNative
// (which delegates) returns the same error.
func TestSettleNativeDeadLetterReportsFalseOnError(t *testing.T) {
	receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage("not json", "2", true)}}
	tr, err := New(WithClient(&failingDeadLetterClient{receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := tr.Pop(context.Background(), "orders", 0)
	dl, err := tr.SettleNativeDeadLetter(context.Background(), msg, "poison", "x")
	if err == nil || dl {
		t.Fatalf("deadLettered = %v, err = %v; want false, error", dl, err)
	}
	if err := tr.SettleNative(context.Background(), msg, "poison", "x"); err == nil {
		t.Error("SettleNative must return the same error")
	}
}

// A handle that is not a Service Bus message is a no-op: false, nil.
func TestSettleNativeDeadLetterNoopWithoutHandle(t *testing.T) {
	tr, _ := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: &fakeReceiver{}}))
	dl, err := tr.SettleNativeDeadLetter(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders"}, "poison", "x")
	if err != nil || dl {
		t.Fatalf("deadLettered = %v, err = %v; want false, nil", dl, err)
	}
}

// Redrive{Bypass: true} over Azure Service Bus stamps bq-replay-bypass on the redriven message and
// reports Bypassed; the body is the reset envelope.
func TestRedriveBypassStampsMarker(t *testing.T) {
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	dead, err := babelqueue.Annotate(env, "failed", "orders", 3, errors.New("boom")).Encode()
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{}
	receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{{Body: dead, DeliveryCount: 1}}}
	tr, err := New(WithClient(&fakeClient{sender: sender, receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}

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
	if p["bq-replay-bypass"] != "1" || p["bq-schema-version"] != 1 {
		t.Errorf("application properties = %v", p)
	}
	redriven, err := babelqueue.Decode(sender.sent[0].Body)
	if err != nil || redriven.DeadLetter != nil || redriven.Attempts != 0 {
		t.Errorf("redriven body = %v, %v", redriven, err)
	}
	if len(receiver.completed) != 1 {
		t.Errorf("DLQ message not completed after a successful redrive")
	}
}

// The optional bq-attempts (§4.2) and bq-delay (§4.4) mirrors are contract properties: a caller
// cannot write them, and a message from another SDK carrying them does not leak them into the
// headers map.
func TestBqAttemptsAndDelayAreNeitherWrittenNorSurfaced(t *testing.T) {
	sender := &fakeSender{}
	tr, err := New(WithClient(&fakeClient{sender: sender, receiver: &fakeReceiver{}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.PublishWithHeaders(context.Background(), "orders", sampleBody(t),
		map[string]string{"bq-attempts": "7", "bq-delay": "5000", "traceparent": testTraceparent}); err != nil {
		t.Fatal(err)
	}
	p := sender.sent[0].ApplicationProperties
	if _, ok := p["bq-attempts"]; ok {
		t.Error("bq-attempts written from caller headers")
	}
	if _, ok := p["bq-delay"]; ok {
		t.Error("bq-delay written from caller headers")
	}
	got := outOfBandFromProperties(map[string]any{"bq-attempts": "1", "bq-delay": "5000", "traceparent": testTraceparent})
	if len(got) != 1 || got["traceparent"] != testTraceparent {
		t.Errorf("surfaced headers = %v", got)
	}
}
