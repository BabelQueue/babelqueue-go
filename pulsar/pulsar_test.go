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
