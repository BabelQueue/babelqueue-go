package pulsar

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	pulsarsdk "github.com/apache/pulsar-client-go/pulsar"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const urn = "urn:babel:orders:created"

type fakeProducer struct {
	sent    []*pulsarsdk.ProducerMessage
	sendErr error // when set, Send fails without recording the message
}

func (f *fakeProducer) Send(_ context.Context, m *pulsarsdk.ProducerMessage) (pulsarsdk.MessageID, error) {
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	f.sent = append(f.sent, m)
	return nil, nil
}

// fakeMessage embeds the pulsar.Message interface so it satisfies it; only the two methods
// the transport calls are implemented (the rest would panic, but are never invoked).
type fakeMessage struct {
	pulsarsdk.Message
	payload         []byte
	redeliveryCount uint32
	properties      map[string]string
}

func (m fakeMessage) Payload() []byte               { return m.payload }
func (m fakeMessage) RedeliveryCount() uint32       { return m.redeliveryCount }
func (m fakeMessage) Properties() map[string]string { return m.properties }

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

	out, _ := reconcile(string(b), nil, 1, nil)
	decoded, _ := babelqueue.Decode([]byte(out))
	if decoded.Attempts != 5 {
		t.Errorf("attempts = %d, want 5 (runtime count not lowered)", decoded.Attempts)
	}
}

func TestReconcileFirstDeliveryUnchanged(t *testing.T) {
	body := sampleBody(t)
	if out, route := reconcile(body, nil, 0, nil); out != body || route != "" {
		t.Error("first delivery (RedeliveryCount=0) should leave the body unchanged")
	}
}

// popBody delivers one fake message and returns the decoded envelope the App would see.
func popBody(t *testing.T, payload string, props map[string]string, redelivery uint32) babelqueue.Envelope {
	t.Helper()
	env, _ := popMessage(t, payload, props, redelivery)
	return env
}

// popMessage is popBody plus the received message, so a test can read RouteURN and the raw body.
func popMessage(t *testing.T, payload string, props map[string]string, redelivery uint32) (babelqueue.Envelope, *babelqueue.ReceivedMessage) {
	t.Helper()
	msg := fakeMessage{payload: []byte(payload), redeliveryCount: redelivery, properties: props}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{msg}}
	tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: consumer}))
	got, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || got == nil {
		t.Fatalf("Pop = (%v, %v)", got, err)
	}
	env, err := babelqueue.Decode([]byte(got.Body))
	if err != nil {
		t.Fatal(err)
	}
	return env, got
}

func bodyWith(t *testing.T, job string, attempts int) string {
	t.Helper()
	env, err := babelqueue.Make(job, map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	env.Attempts = attempts
	b, _ := env.Encode()
	return string(b)
}

func TestPopRoutesOnBqJobPropertyWithoutRewritingTheBody(t *testing.T) {
	const routed = "urn:babel:orders:refunded"
	body := bodyWith(t, urn, 0)
	env, got := popMessage(t, body, map[string]string{"bq-job": routed}, 0)
	if got.RouteURN != routed {
		t.Errorf("RouteURN = %q, want %q (bq-job wins over the body's job)", got.RouteURN, routed)
	}
	if got.Body != body || env.URN() != urn {
		t.Errorf("the body must reach the App untouched; job = %q, body changed = %v", env.URN(), got.Body != body)
	}
}

func TestPopKeepsUrnAliasInTheBody(t *testing.T) {
	const routed = "urn:babel:orders:refunded"
	body := `{"urn":"` + urn + `","trace_id":"t-1","data":{"x":1},"meta":{"schema_version":1,"id":"m-1"},"attempts":0}`
	_, got := popMessage(t, body, map[string]string{"bq-job": routed}, 0)
	if got.Body != body || got.RouteURN != routed {
		t.Errorf("urn-alias body must be delivered as is with RouteURN %q; got body %q route %q", routed, got.Body, got.RouteURN)
	}
}

// End to end: the App routes on bq-job, and a failing handler's retry carries the producer's
// original body (not one rewritten to bq-job) AND the original bq-job property, so the second
// delivery reaches the same handler (§5.2: the route is constant across retries).
func TestAppRetryKeepsTheOriginalBodyAndRoute(t *testing.T) {
	const routed = "urn:babel:orders:refunded"
	producer := &fakeProducer{}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{
		fakeMessage{payload: []byte(bodyWith(t, urn, 0)), properties: map[string]string{"bq-job": routed}},
	}}
	tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: consumer}))
	var routedSeen, bodySeen int
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3))
	app.Handle(routed, func(_ context.Context, env babelqueue.Envelope) error {
		routedSeen++
		if env.URN() != urn {
			t.Errorf("handler saw job %q, want the producer's %q", env.URN(), urn)
		}
		return context.DeadlineExceeded
	})
	app.Handle(urn, func(context.Context, babelqueue.Envelope) error {
		bodySeen++
		return nil
	})
	if n, err := app.Drain(context.Background(), "orders", 1); err != nil || n != 1 {
		t.Fatalf("Drain = (%d, %v)", n, err)
	}
	if len(producer.sent) != 1 {
		t.Fatalf("retry published %d messages, want 1", len(producer.sent))
	}
	retried, _ := babelqueue.Decode(producer.sent[0].Payload)
	if retried.Job != urn || retried.Attempts != 1 {
		t.Errorf("retry body job=%q attempts=%d, want job %q attempts 1", retried.Job, retried.Attempts, urn)
	}
	if producer.sent[0].Properties["bq-job"] != routed {
		t.Errorf("retry bq-job property = %q, want the original %q", producer.sent[0].Properties["bq-job"], routed)
	}
	if producer.sent[0].Properties["bq-attempts"] != "1" {
		t.Errorf("retry bq-attempts = %q, want 1", producer.sent[0].Properties["bq-attempts"])
	}

	// The broker hands the re-published message back: it must reach the same handler.
	consumer.messages = append(consumer.messages, fakeMessage{
		payload: producer.sent[0].Payload, properties: producer.sent[0].Properties,
	})
	if n, err := app.Drain(context.Background(), "orders", 1); err != nil || n != 1 {
		t.Fatalf("second Drain = (%d, %v)", n, err)
	}
	if routedSeen != 2 || bodySeen != 0 {
		t.Errorf("routed handler ran %d times, body-URN handler %d times; want 2 and 0", routedSeen, bodySeen)
	}
}

// Dead-lettering keeps the producer's original body, not one rewritten to bq-job.
func TestAppDeadLetterKeepsTheOriginalBody(t *testing.T) {
	const routed = "urn:babel:orders:refunded"
	producer := &fakeProducer{}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{
		fakeMessage{payload: []byte(bodyWith(t, urn, 0)), properties: map[string]string{"bq-job": routed}},
	}}
	tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: consumer}))
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true))
	app.Handle(routed, func(context.Context, babelqueue.Envelope) error { return context.DeadlineExceeded })
	if n, err := app.Drain(context.Background(), "orders", 1); err != nil || n != 1 {
		t.Fatalf("Drain = (%d, %v)", n, err)
	}
	if len(producer.sent) != 1 {
		t.Fatalf("dead-letter published %d messages, want 1", len(producer.sent))
	}
	dead, _ := babelqueue.Decode(producer.sent[0].Payload)
	if dead.Job != urn {
		t.Errorf("dead-letter body job = %q, want the producer's %q", dead.Job, urn)
	}
	// The DLQ record carries the original routing property, not one derived from the body.
	if got := producer.sent[0].Properties["bq-job"]; got != routed {
		t.Errorf("dead-letter bq-job = %q, want the original %q", got, routed)
	}
}

// StrategyRelease re-publishes an unknown-URN message; the re-published copy keeps the original
// bq-job so the next delivery is routed (and found unknown) the same way, not flipped to the
// body URN, whose handler exists.
func TestAppUnknownURNReleaseKeepsTheOriginalRoute(t *testing.T) {
	const routed = "urn:babel:orders:unregistered"
	body := bodyWith(t, urn, 0)
	producer := &fakeProducer{}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{
		fakeMessage{payload: []byte(body), properties: map[string]string{"bq-job": routed}},
	}}
	tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: consumer}))
	var bodySeen int
	app := babelqueue.NewApp(tr, babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease))
	app.Handle(urn, func(context.Context, babelqueue.Envelope) error {
		bodySeen++
		return nil
	})
	if n, err := app.Drain(context.Background(), "orders", 1); err != nil || n != 1 {
		t.Fatalf("Drain = (%d, %v)", n, err)
	}
	if len(producer.sent) != 1 {
		t.Fatalf("release published %d messages, want 1", len(producer.sent))
	}
	if string(producer.sent[0].Payload) != body {
		t.Errorf("released body changed: %q", producer.sent[0].Payload)
	}
	if producer.sent[0].Properties["bq-job"] != routed {
		t.Errorf("released bq-job = %q, want the original %q", producer.sent[0].Properties["bq-job"], routed)
	}

	consumer.messages = append(consumer.messages, fakeMessage{
		payload: producer.sent[0].Payload, properties: producer.sent[0].Properties,
	})
	if n, err := app.Drain(context.Background(), "orders", 1); err != nil || n != 1 {
		t.Fatalf("second Drain = (%d, %v)", n, err)
	}
	if bodySeen != 0 {
		t.Errorf("body-URN handler ran %d times; the released message must stay routed on %q", bodySeen, routed)
	}
	if len(producer.sent) != 2 || producer.sent[1].Properties["bq-job"] != routed {
		t.Errorf("second release did not keep the route: %d sends", len(producer.sent))
	}
}

// A blank route URN (no bq-job property) derives bq-job from the body, as before.
func TestPublishRoutedBlankRouteDerivesFromBody(t *testing.T) {
	producer := &fakeProducer{}
	tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: &fakeConsumer{}}))
	if err := tr.PublishRouted(context.Background(), "orders", bodyWith(t, urn, 0), ""); err != nil {
		t.Fatal(err)
	}
	if got := producer.sent[0].Properties["bq-job"]; got != urn {
		t.Errorf("bq-job = %q, want body URN %q", got, urn)
	}
}

type mismatchCall struct {
	propertyURN, bodyURN string
	env                  babelqueue.Envelope
}

// popWithHook delivers one message through a transport carrying hook, then acks it, and
// returns the consumer so the test can inspect acks and nacks.
func popWithHook(t *testing.T, payload string, props map[string]string, hook OnRoutingMismatch) (*fakeConsumer, babelqueue.Envelope) {
	t.Helper()
	msg := fakeMessage{payload: []byte(payload), properties: props}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{msg}}
	tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: consumer}), WithOnRoutingMismatch(hook))
	got, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || got == nil {
		t.Fatalf("Pop = (%v, %v)", got, err)
	}
	if err := tr.Ack(context.Background(), got); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	env, err := babelqueue.Decode([]byte(got.Body))
	if err != nil {
		t.Fatal(err)
	}
	return consumer, env
}

func TestRoutingMismatchHookIsCalledWithBothURNsAndTheBodyEnvelope(t *testing.T) {
	const routed = "urn:babel:orders:refunded"
	var calls []mismatchCall
	consumer, env := popWithHook(t, bodyWith(t, urn, 0), map[string]string{"bq-job": " " + routed + " "},
		func(p, b string, e *babelqueue.Envelope) { calls = append(calls, mismatchCall{p, b, *e}) })
	if len(calls) != 1 {
		t.Fatalf("hook called %d times, want 1", len(calls))
	}
	if calls[0].propertyURN != routed || calls[0].bodyURN != urn || calls[0].env.Job != urn {
		t.Errorf("hook args = (%q, %q, job %q), want (%q, %q, job %q)",
			calls[0].propertyURN, calls[0].bodyURN, calls[0].env.Job, routed, urn, urn)
	}
	if env.URN() != urn {
		t.Errorf("body job = %q, want the untouched %q", env.URN(), urn)
	}
	if len(consumer.acked) != 1 || len(consumer.nacked) != 0 {
		t.Errorf("acked=%d nacked=%d, want 1/0", len(consumer.acked), len(consumer.nacked))
	}
}

func TestRoutingMismatchHookSeesReconciledAttempts(t *testing.T) {
	var got []int
	popWithHook(t, bodyWith(t, urn, 0), map[string]string{"bq-job": "urn:babel:orders:refunded", "bq-attempts": "4"},
		func(_, _ string, e *babelqueue.Envelope) { got = append(got, e.Attempts) })
	if len(got) != 1 || got[0] != 4 {
		t.Errorf("hook saw attempts %v, want [4] (reconciled from bq-attempts)", got)
	}
}

func TestRoutingMismatchHookNotCalledForRejectedEnvelope(t *testing.T) {
	// schema_version 2 is not accepted by this consumer; the mismatch must not be reported.
	body := `{"job":"` + urn + `","trace_id":"t-1","data":{"x":1},"meta":{"schema_version":2,"id":"m-1"},"attempts":0}`
	called := false
	popWithHook(t, body, map[string]string{"bq-job": "urn:babel:orders:refunded"},
		func(string, string, *babelqueue.Envelope) { called = true })
	if called {
		t.Error("hook must not be called for an envelope Accepts() rejects")
	}
}

func TestRoutingMismatchHookNotCalledWithoutMismatch(t *testing.T) {
	noJob := `{"trace_id":"t-1","data":{"x":1},"meta":{"schema_version":1,"id":"m-1"},"attempts":0}`
	for name, c := range map[string]struct {
		payload string
		props   map[string]string
	}{
		"same urn":     {bodyWith(t, urn, 0), map[string]string{"bq-job": urn}},
		"trimmed same": {bodyWith(t, urn, 0), map[string]string{"bq-job": "  " + urn + "  "}},
		"no property":  {bodyWith(t, urn, 0), nil},
		"blank":        {bodyWith(t, urn, 0), map[string]string{"bq-job": "  "}},
		"body w/o job": {noJob, map[string]string{"bq-job": urn}},
		"undecodable":  {"not json", map[string]string{"bq-job": urn}},
	} {
		t.Run(name, func(t *testing.T) {
			called := false
			msg := fakeMessage{payload: []byte(c.payload), properties: c.props}
			consumer := &fakeConsumer{messages: []pulsarsdk.Message{msg}}
			tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: consumer}),
				WithOnRoutingMismatch(func(string, string, *babelqueue.Envelope) { called = true }))
			if got, err := tr.Pop(context.Background(), "orders", 0); err != nil || got == nil {
				t.Fatalf("Pop = (%v, %v)", got, err)
			}
			if called {
				t.Error("hook must not be called")
			}
		})
	}
}

func TestPanickingRoutingMismatchHookStillDeliversAndAcks(t *testing.T) {
	const routed = "urn:babel:orders:refunded"
	consumer, env := popWithHook(t, bodyWith(t, urn, 0), map[string]string{"bq-job": routed},
		func(string, string, *babelqueue.Envelope) { panic("hook failure") })
	if env.URN() != urn {
		t.Errorf("body job = %q, want the untouched %q", env.URN(), urn)
	}
	if len(consumer.acked) != 1 || len(consumer.nacked) != 0 {
		t.Errorf("acked=%d nacked=%d, want 1/0", len(consumer.acked), len(consumer.nacked))
	}
}

func TestPopFallsBackToBodyJobWhenPropertyMissingOrBlank(t *testing.T) {
	for name, props := range map[string]map[string]string{
		"nil":   nil,
		"empty": {},
		"blank": {"bq-job": "   "},
	} {
		t.Run(name, func(t *testing.T) {
			env, got := popMessage(t, bodyWith(t, urn, 0), props, 0)
			if env.URN() != urn || got.RouteURN != "" {
				t.Errorf("job = %q RouteURN = %q, want body job %q and no override", env.URN(), got.RouteURN, urn)
			}
		})
	}
}

func TestPopBodyWithoutJobIsRejectedDespiteBqJob(t *testing.T) {
	body := `{"trace_id":"t-1","data":{"x":1},"meta":{"schema_version":1,"id":"m-1"},"attempts":0}`
	env, got := popMessage(t, body, map[string]string{"bq-job": urn}, 0)
	if env.URN() != "" || env.Accepts() || got.RouteURN != "" {
		t.Errorf("a body with no job must stay non-conformant; got URN %q accepts=%v RouteURN=%q", env.URN(), env.Accepts(), got.RouteURN)
	}
}

func TestPopBqAttemptsIsAuthoritative(t *testing.T) {
	cases := []struct {
		name       string
		body       int
		prop       string
		redelivery uint32
		want       int
	}{
		{"property replaces body", 5, "3", 0, 3},
		{"property raises body", 1, "4", 0, 4},
		{"redelivery raises property", 0, "2", 6, 6},
		{"property above redelivery", 0, "7", 2, 7},
		{"leading zeros are digits", 0, "007", 0, 7},
		{"zero property lowers body", 5, "0", 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := popBody(t, bodyWith(t, urn, c.body), map[string]string{"bq-attempts": c.prop}, c.redelivery)
			if env.Attempts != c.want {
				t.Errorf("attempts = %d, want %d", env.Attempts, c.want)
			}
		})
	}
}

func TestPopInvalidBqAttemptsFallsBackToBody(t *testing.T) {
	for _, bad := range []string{"", " ", "abc", "-1", "+3", "-0", "3.5", "1e2", " 3", "3 ", "3\n", " 3x", "٣", "2147483648", "99999999999999999999"} {
		t.Run(bad, func(t *testing.T) {
			env := popBody(t, bodyWith(t, urn, 5), map[string]string{"bq-attempts": bad}, 0)
			if env.Attempts != 5 {
				t.Errorf("attempts = %d, want the body's 5 for invalid property %q", env.Attempts, bad)
			}
		})
	}
	t.Run("redelivery still applies", func(t *testing.T) {
		env := popBody(t, bodyWith(t, urn, 5), map[string]string{"bq-attempts": "nope"}, 9)
		if env.Attempts != 9 {
			t.Errorf("attempts = %d, want 9", env.Attempts)
		}
	})
	t.Run("missing property", func(t *testing.T) {
		if env := popBody(t, bodyWith(t, urn, 5), nil, 0); env.Attempts != 5 {
			t.Errorf("attempts = %d, want 5", env.Attempts)
		}
	})
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
	m := message("not-json", "")
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

// --- bq-schema-version gate (§5.7 step 3) ---

func TestPopSchemaVersionMissingOrBlankDecodesNormally(t *testing.T) {
	for name, props := range map[string]map[string]string{
		"missing": {},
		"nil":     nil,
		"blank":   {"bq-schema-version": "  "},
		"empty":   {"bq-schema-version": ""},
		// blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r), identical in every SDK (GR-5)
		"vertical tab": {"bq-schema-version": "\u000B"},
		"form feed":    {"bq-schema-version": "\f"},
		"all ASCII ws": {"bq-schema-version": " \t\n\v\f\r"},
	} {
		t.Run(name, func(t *testing.T) {
			env, got := popMessage(t, bodyWith(t, urn, 0), props, 0)
			if env.URN() != urn || got.RouteURN != "" {
				t.Errorf("job = %q route = %q; want a normal decode with no route override", env.URN(), got.RouteURN)
			}
		})
	}
}

func TestPopSchemaVersionSupportedIsDelivered(t *testing.T) {
	env, got := popMessage(t, bodyWith(t, urn, 0), map[string]string{"bq-schema-version": "1", "bq-attempts": "3"}, 0)
	if got.RouteURN != "" || env.URN() != urn || env.Attempts != 3 {
		t.Errorf("version 1 must follow the normal path: route = %q job = %q attempts = %d", got.RouteURN, env.URN(), env.Attempts)
	}
}

// An unsupported version is rejected before the body is decoded: a body that is not JSON at all
// still yields the version-gate route (a decode error would leave RouteURN empty), and the
// payload reaches the App untouched.
func TestPopUnsupportedSchemaVersionGatesBeforeDecode(t *testing.T) {
	const garbage = "{not json"
	for _, v := range []string{"2", "x", "01", " 1", "1.0", "-1", "\u001C", "\u0085", "\uFEFF", "\u00A0", "\u3000"} {
		t.Run(fmt.Sprintf("%q", v), func(t *testing.T) {
			msg := fakeMessage{payload: []byte(garbage), properties: map[string]string{"bq-schema-version": v, "bq-job": urn}}
			consumer := &fakeConsumer{messages: []pulsarsdk.Message{msg}}
			tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: consumer}))
			got, err := tr.Pop(context.Background(), "orders", 0)
			if err != nil || got == nil {
				t.Fatalf("Pop = (%v, %v)", got, err)
			}
			if want := gateRoute(v, map[string]string{"bq-job": urn}); got.RouteURN != want {
				t.Errorf("RouteURN = %q, want %q", got.RouteURN, want)
			}
			if got.Body != garbage {
				t.Errorf("body must be handed over untouched, got %q", got.Body)
			}
		})
	}
}

func TestRoutingMismatchHookNotCalledForGatedMessage(t *testing.T) {
	called := false
	hook := func(string, string, *babelqueue.Envelope) { called = true }
	msg := fakeMessage{payload: []byte(bodyWith(t, urn, 0)), properties: map[string]string{
		"bq-schema-version": "2", "bq-job": "urn:babel:orders:refunded",
	}}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{msg}}
	tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: consumer}), WithOnRoutingMismatch(hook))
	if got, err := tr.Pop(context.Background(), "orders", 0); err != nil || got == nil {
		t.Fatalf("Pop = (%v, %v)", got, err)
	}
	if called {
		t.Error("the mismatch hook must not run for a version-gated message")
	}
}

// End to end: a gated message never reaches a handler (neither the body URN's nor the bq-job's)
// and is quarantined to the DLQ as the raw, unchanged body (never decoded, no dead_letter block).
func TestAppQuarantinesUnsupportedSchemaVersion(t *testing.T) {
	body := bodyWith(t, urn, 0)
	for _, v := range []string{"2", "x"} {
		t.Run(v, func(t *testing.T) {
			producer := &fakeProducer{}
			consumer := &fakeConsumer{messages: []pulsarsdk.Message{
				fakeMessage{payload: []byte(body), properties: map[string]string{"bq-schema-version": v, "bq-job": urn}},
			}}
			tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: consumer}))
			app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true))
			handled := 0
			app.Handle(urn, func(context.Context, babelqueue.Envelope) error { handled++; return nil })
			if n, err := app.Drain(context.Background(), "orders", 1); err != nil || n != 1 {
				t.Fatalf("Drain = (%d, %v)", n, err)
			}
			if handled != 0 {
				t.Fatalf("handler ran %d times for a version-gated message", handled)
			}
			if len(consumer.acked) != 1 || len(producer.sent) != 1 {
				t.Fatalf("acked %d, dead-lettered %d; want 1 and 1", len(consumer.acked), len(producer.sent))
			}
			dead, _ := babelqueue.Decode(producer.sent[0].Payload)
			if dead.Job != urn {
				t.Errorf("dead-letter body job = %q, want the producer's %q", dead.Job, urn)
			}
			if string(producer.sent[0].Payload) != body {
				t.Errorf("dead-letter body is not byte-identical to the incoming body:\n got: %s\nwant: %s", producer.sent[0].Payload, body)
			}
			if strings.Contains(string(producer.sent[0].Payload), "dead_letter") {
				t.Errorf("dead-letter record carries a dead_letter block: %s", producer.sent[0].Payload)
			}
		})
	}
}

// The sentinel route is an in-process marker: the dead-letter record the App writes for a gated
// message (it never retries one) keeps the delivery's original bq-job and bq-schema-version
// and never carries the reserved URN, so a redrive is gated the same way.
func TestGatedRepublishKeepsOriginalRoutingProperties(t *testing.T) {
	body := bodyWith(t, urn, 0)
	cases := map[string]struct {
		props   map[string]string
		wantJob string
		hasJob  bool
	}{
		"with bq-job":    {map[string]string{"bq-schema-version": "2", "bq-job": "urn:babel:orders:legacy"}, "urn:babel:orders:legacy", true},
		"without bq-job": {map[string]string{"bq-schema-version": "x"}, urn, true}, // derived from the body, never the sentinel,
		"escaped chars":  {map[string]string{"bq-schema-version": "2&x=?bq-job=%", "bq-job": "a?bq-job=b&c d"}, "a?bq-job=b&c d", true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Dead-lettering off: the App drops (acks) the gated message, publishing nothing.
			{
				producer := &fakeProducer{}
				consumer := &fakeConsumer{messages: []pulsarsdk.Message{fakeMessage{payload: []byte(body), properties: tc.props}}}
				tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: consumer}))
				app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5))
				if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
					t.Fatalf("Drain: %v", err)
				}
				if len(producer.sent) != 0 || len(consumer.acked) != 1 {
					t.Fatalf("dead-letter off: published %d, acked %d; want 0 and 1 (no retry copy)", len(producer.sent), len(consumer.acked))
				}
			}
			// Dead-lettering on: exactly one dead-letter record, with no retry copy before it.
			for _, deadLetter := range []bool{true} {
				producer := &fakeProducer{}
				consumer := &fakeConsumer{messages: []pulsarsdk.Message{fakeMessage{payload: []byte(body), properties: tc.props}}}
				tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: consumer}))
				app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5), babelqueue.WithDeadLetter(deadLetter))
				if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
					t.Fatalf("Drain: %v", err)
				}
				if len(producer.sent) != 1 {
					t.Fatalf("deadLetter=%v: published %d messages, want 1", deadLetter, len(producer.sent))
				}
				got := producer.sent[0].Properties
				if strings.Contains(got["bq-job"], "babelqueue:unsupported-schema-version") {
					t.Errorf("deadLetter=%v: the sentinel leaked into bq-job: %q", deadLetter, got["bq-job"])
				}
				if job, ok := got["bq-job"]; ok != tc.hasJob || job != tc.wantJob {
					t.Errorf("deadLetter=%v: bq-job = (%q, %v), want (%q, %v)", deadLetter, job, ok, tc.wantJob, tc.hasJob)
				}
				if got["bq-schema-version"] != tc.props["bq-schema-version"] {
					t.Errorf("deadLetter=%v: bq-schema-version = %q, want the original %q", deadLetter, got["bq-schema-version"], tc.props["bq-schema-version"])
				}
				if !strings.Contains(string(producer.sent[0].Payload), `"job":"`+urn+`"`) {
					t.Errorf("deadLetter=%v: body job was rewritten: %s", deadLetter, producer.sent[0].Payload)
				}
			}
		})
	}
}

// A dead-lettered gated message is gated again when it is consumed (the redrive meets the same gate).
func TestGatedRepublishIsGatedAgain(t *testing.T) {
	producer := &fakeProducer{}
	first := &fakeConsumer{messages: []pulsarsdk.Message{
		fakeMessage{payload: []byte(bodyWith(t, urn, 0)), properties: map[string]string{"bq-schema-version": "2", "bq-job": urn}},
	}}
	tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: first}))
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5), babelqueue.WithDeadLetter(true))
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil || len(producer.sent) != 1 {
		t.Fatalf("Drain: %v, sent %d", err, len(producer.sent))
	}
	sent := producer.sent[0]
	second := &fakeConsumer{messages: []pulsarsdk.Message{fakeMessage{payload: sent.Payload, properties: sent.Properties}}}
	tr2, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: second}))
	got, err := tr2.Pop(context.Background(), "orders", 0)
	if err != nil || got == nil {
		t.Fatalf("Pop = (%v, %v)", got, err)
	}
	if !strings.HasPrefix(got.RouteURN, unsupportedSchemaRoutePrefix) {
		t.Errorf("the re-published message must be gated again, RouteURN = %q", got.RouteURN)
	}
}

// A body that is not JSON plus an unsupported version: the transport names the real reason through
// the OnSchemaVersionRejected hook (the App cannot, it only applies the route to a decoded body),
// and the message is still quarantined with its original payload, never handed to a handler.
func TestRejectedHookNamesVersionForInvalidBody(t *testing.T) {
	const garbage = "{not json"
	var gotVersion, gotQueue string
	hook := func(version, queue string) { gotVersion, gotQueue = version, queue }
	producer := &fakeProducer{}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{
		fakeMessage{payload: []byte(garbage), properties: map[string]string{"bq-schema-version": "2"}},
	}}
	tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: consumer}), WithOnSchemaVersionRejected(hook))
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true))
	handled := 0
	app.Handle(urn, func(context.Context, babelqueue.Envelope) error { handled++; return nil })
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if gotVersion != "2" || gotQueue != "orders" {
		t.Errorf("hook = (%q, %q), want (\"2\", \"orders\")", gotVersion, gotQueue)
	}
	if handled != 0 {
		t.Errorf("handler ran %d times", handled)
	}
	for _, m := range producer.sent {
		if strings.Contains(m.Properties["bq-job"], "babelqueue:unsupported-schema-version") {
			t.Errorf("the sentinel leaked into bq-job: %q", m.Properties["bq-job"])
		}
	}
}

func TestRejectedHookIsNotCalledForSupportedVersionAndPanicIsIgnored(t *testing.T) {
	called := false
	popMessageWithOptions := func(props map[string]string, fn OnSchemaVersionRejected) {
		consumer := &fakeConsumer{messages: []pulsarsdk.Message{fakeMessage{payload: []byte(bodyWith(t, urn, 0)), properties: props}}}
		tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: consumer}), WithOnSchemaVersionRejected(fn))
		if got, err := tr.Pop(context.Background(), "orders", 0); err != nil || got == nil {
			t.Fatalf("Pop = (%v, %v)", got, err)
		}
	}
	popMessageWithOptions(map[string]string{"bq-schema-version": "1"}, func(string, string) { called = true })
	popMessageWithOptions(map[string]string{}, func(string, string) { called = true })
	if called {
		t.Error("the hook must not run for a supported or missing version")
	}
	popMessageWithOptions(map[string]string{"bq-schema-version": "2"}, func(string, string) { panic("boom") })
}

// A gated message goes straight to the dead-letter queue, with no retry, even when the App is
// configured with StrategyRelease and several attempts: neither a release nor a retry can make a
// future-version message handleable, so the handler is never reached and exactly one record (the
// raw dead-letter copy, with the original properties) is written, after which the original is acked.
func TestGatedMessageIsDeadLetteredDirectlyUnderStrategyRelease(t *testing.T) {
	body := bodyWith(t, urn, 0)
	props := map[string]string{"bq-schema-version": "2", "bq-job": urn}
	producer := &fakeProducer{}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{fakeMessage{payload: []byte(body), properties: props}}}
	tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: consumer}))
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3), babelqueue.WithDeadLetter(true),
		babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease))
	handled := 0
	app.Handle(urn, func(context.Context, babelqueue.Envelope) error { handled++; return nil })
	if n, err := app.Drain(context.Background(), "orders", 1); err != nil || n != 1 {
		t.Fatalf("Drain = (%d, %v)", n, err)
	}
	if handled != 0 {
		t.Fatalf("handler ran %d times for a version-gated message", handled)
	}
	if len(producer.sent) != 1 || len(consumer.acked) != 1 {
		t.Fatalf("published %d, acked %d; want exactly one dead-letter record and one ack (no retry)", len(producer.sent), len(consumer.acked))
	}
	sent := producer.sent[0]
	if sent.Properties["bq-schema-version"] != "2" || sent.Properties["bq-job"] != urn {
		t.Errorf("dead-letter properties = %v, want the originals", sent.Properties)
	}
	if string(sent.Payload) != body {
		t.Errorf("dead-letter body is not byte-identical to the incoming body:\n got: %s\nwant: %s", sent.Payload, body)
	}
	if strings.Contains(string(sent.Payload), "dead_letter") {
		t.Errorf("dead-letter record carries a dead_letter block: %s", sent.Payload)
	}
}

// When the dead-letter write for a version-gated message fails, the App must not ack it: the
// transport hands it back with Nack so the broker redelivers it, and the handler never runs.
func TestGatedMessageIsNackedNotAckedWhenDeadLetterWriteFails(t *testing.T) {
	body := bodyWith(t, urn, 0)
	props := map[string]string{"bq-schema-version": "2", "bq-job": urn}
	producer := &fakeProducer{sendErr: errors.New("broker down")}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{fakeMessage{payload: []byte(body), properties: props}}}
	tr, _ := New(WithClient(&fakeClient{producer: producer, consumer: consumer}))
	var hookErr error
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3), babelqueue.WithDeadLetter(true),
		babelqueue.WithRewindPause(0),
		babelqueue.WithDeadLetterErrorHandler(func(_ context.Context, _ *babelqueue.ReceivedMessage, err error) { hookErr = err }))
	handled := 0
	app.Handle(urn, func(context.Context, babelqueue.Envelope) error { handled++; return nil })
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if handled != 0 {
		t.Fatalf("handler ran %d times for a version-gated message", handled)
	}
	if len(consumer.acked) != 0 {
		t.Fatalf("acked %d messages; a message whose dead-letter write failed must not be acked", len(consumer.acked))
	}
	if len(consumer.nacked) != 1 {
		t.Fatalf("nacked %d messages, want 1", len(consumer.nacked))
	}
	if len(producer.sent) != 0 {
		t.Fatalf("published %d messages, want none (the write failed)", len(producer.sent))
	}
	if hookErr == nil {
		t.Error("the dead-letter error handler was not called")
	}
}

func TestRewindNacksThePulsarMessageAndIgnoresForeignHandles(t *testing.T) {
	consumer := &fakeConsumer{}
	tr, _ := New(WithClient(&fakeClient{producer: &fakeProducer{}, consumer: consumer}))
	var _ babelqueue.Rewinder = tr

	msg := fakeMessage{payload: []byte("{}")}
	if err := tr.Rewind(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders", Handle: msg}); err != nil {
		t.Fatalf("Rewind: %v", err)
	}
	if len(consumer.nacked) != 1 || len(consumer.acked) != 0 {
		t.Fatalf("nacked %d, acked %d; want 1 and 0", len(consumer.nacked), len(consumer.acked))
	}
	if err := tr.Rewind(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders", Handle: "not-a-message"}); err != nil {
		t.Fatalf("Rewind on a foreign handle: %v", err)
	}
	if len(consumer.nacked) != 1 {
		t.Fatalf("a foreign handle was nacked; nacked = %d", len(consumer.nacked))
	}
}
