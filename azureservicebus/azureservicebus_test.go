package azureservicebus

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const urn = "urn:babel:orders:created"

type fakeSender struct {
	sent []*azservicebus.Message
	err  error // when set, SendMessage fails
}

func (f *fakeSender) SendMessage(_ context.Context, m *azservicebus.Message, _ *azservicebus.SendMessageOptions) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, m)
	return nil
}

type fakeReceiver struct {
	messages     []*azservicebus.ReceivedMessage
	completed    []*azservicebus.ReceivedMessage
	abandoned    []*azservicebus.ReceivedMessage
	deadLettered []*azservicebus.ReceivedMessage
	dlReasons    []string // DeadLetterReason of every DeadLetterMessage call
	dlDetails    []string // DeadLetterErrorDescription of every call
}

func (f *fakeReceiver) DeadLetterMessage(_ context.Context, m *azservicebus.ReceivedMessage, o *azservicebus.DeadLetterOptions) error {
	f.deadLettered = append(f.deadLettered, m)
	if o != nil {
		f.dlReasons = append(f.dlReasons, derefStr(o.Reason))
		f.dlDetails = append(f.dlDetails, derefStr(o.ErrorDescription))
	}
	return nil
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

func (f *fakeReceiver) AbandonMessage(_ context.Context, m *azservicebus.ReceivedMessage, _ *azservicebus.AbandonMessageOptions) error {
	f.abandoned = append(f.abandoned, m)
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

// gatedMessage builds a received message carrying the given bq-schema-version application
// property (no property at all when set is false).
func gatedMessage(body string, version any, set bool) *azservicebus.ReceivedMessage {
	rm := &azservicebus.ReceivedMessage{Body: []byte(body), DeliveryCount: 2}
	if set {
		rm.ApplicationProperties = map[string]any{"bq-schema-version": version}
	}
	return rm
}

func popGated(t *testing.T, body string, version any, set bool, opts ...Option) *babelqueue.ReceivedMessage {
	t.Helper()
	receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage(body, version, set)}}
	opts = append([]Option{WithClient(&fakeClient{sender: &fakeSender{}, receiver: receiver})}, opts...)
	tr, err := New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil {
		t.Fatalf("Pop: msg=%v err=%v", msg, err)
	}
	return msg
}

func TestPopSchemaGateDecodesWhenMissingBlankOrSupported(t *testing.T) {
	cases := map[string]struct {
		version any
		set     bool
	}{
		"missing":      {nil, false},
		"blank":        {" \t", true},
		"string 1":     {"1", true},
		"int 1":        {1, true},
		"int32 1":      {int32(1), true},
		"int64 1":      {int64(1), true},
		"uint8 1":      {uint8(1), true},
		"empty string": {"", true},
		"null value":   {nil, true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			called := false
			msg := popGated(t, sampleBody(t), c.version, c.set,
				WithOnSchemaVersionRejected(func(string, string) { called = true }))
			if msg.RouteURN != "" {
				t.Errorf("RouteURN = %q, want empty", msg.RouteURN)
			}
			if called {
				t.Error("hook called for an accepted message")
			}
			env, err := babelqueue.Decode([]byte(msg.Body))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if env.Attempts != 1 {
				t.Errorf("attempts = %d, want 1 (DeliveryCount 2 - 1)", env.Attempts)
			}
		})
	}
}

func TestPopSchemaGateRejectsUnsupportedWithoutDecoding(t *testing.T) {
	cases := map[string]struct {
		version any
		want    string
	}{
		"string 2":    {"2", "2"},
		"string x":    {"x", "x"},
		"padded":      {" 1", " 1"},
		"nbsp":        {"\u00a0", "\u00a0"},
		"int 2":       {2, "2"},
		"int32 2":     {int32(2), "2"},
		"int64 0":     {int64(0), "0"},
		"float 1.5":   {1.5, "1.5"},
		"float64 1.0": {float64(1), "1"},
		"float32 1.0": {float32(1), "1"},
		"bool":        {true, "true"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var gotVersion, gotQueue string
			// A body that is not JSON proves the gate runs before any decode / reconcile.
			msg := popGated(t, "not json", c.version, true,
				WithOnSchemaVersionRejected(func(v, q string) { gotVersion, gotQueue = v, q }))
			if want := unsupportedSchemaRoutePrefix + url.QueryEscape(c.want); msg.RouteURN != want {
				t.Errorf("RouteURN = %q, want %q", msg.RouteURN, want)
			}
			if msg.Body != "not json" {
				t.Errorf("Body = %q, want it handed over untouched", msg.Body)
			}
			if msg.DeliveryCount != 2 {
				t.Errorf("DeliveryCount = %d, want 2", msg.DeliveryCount)
			}
			if gotVersion != c.want || gotQueue != "orders" {
				t.Errorf("hook got (%q,%q), want (%q,%q)", gotVersion, gotQueue, c.want, "orders")
			}
		})
	}
}

func TestPopSchemaGateHookPanicIsIgnored(t *testing.T) {
	msg := popGated(t, sampleBody(t), "2", true,
		WithOnSchemaVersionRejected(func(string, string) { panic("boom") }))
	if msg.RouteURN == "" {
		t.Error("message was not gated")
	}
}

// A hostile bq-schema-version is bounded to 64 characters plus "..." in the hook value and the
// route URN; exactly 64 characters is left alone.
func TestPopSchemaGateTruncatesLongDeclaredVersion(t *testing.T) {
	long := strings.Repeat("9", 5000)
	var hooked string
	msg := popGated(t, "not json", long, true,
		WithOnSchemaVersionRejected(func(v, _ string) { hooked = v }))
	want := strings.Repeat("9", 64) + "..."
	if hooked != want {
		t.Errorf("hook value length = %d, want %d", len(hooked), len(want))
	}
	if route := unsupportedSchemaRoutePrefix + url.QueryEscape(want); msg.RouteURN != route {
		t.Errorf("RouteURN length = %d, want %d", len(msg.RouteURN), len(route))
	}
	exact := strings.Repeat("9", 64)
	msg = popGated(t, "not json", exact, true)
	if msg.RouteURN != unsupportedSchemaRoutePrefix+exact {
		t.Errorf("a 64-character value must not be truncated, got %q", msg.RouteURN)
	}
}

// Rewind abandons the lock (the message becomes available again) instead of completing it.
func TestRewindAbandonsTheMessage(t *testing.T) {
	receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage("not json", "2", true)}}
	tr, err := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil {
		t.Fatalf("Pop: msg=%v err=%v", msg, err)
	}
	if err := tr.Rewind(context.Background(), msg); err != nil {
		t.Fatalf("Rewind: %v", err)
	}
	if len(receiver.abandoned) != 1 || len(receiver.completed) != 0 {
		t.Fatalf("abandoned = %d, completed = %d; want 1 and 0", len(receiver.abandoned), len(receiver.completed))
	}
}

// abandonOnlyReceiver is a Receiver that offers AbandonMessage but not DeadLetterMessage.
type abandonOnlyReceiver struct{ r *fakeReceiver }

func (a abandonOnlyReceiver) ReceiveMessages(ctx context.Context, n int, o *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	return a.r.ReceiveMessages(ctx, n, o)
}

func (a abandonOnlyReceiver) CompleteMessage(ctx context.Context, m *azservicebus.ReceivedMessage, o *azservicebus.CompleteMessageOptions) error {
	return a.r.CompleteMessage(ctx, m, o)
}

func (a abandonOnlyReceiver) AbandonMessage(ctx context.Context, m *azservicebus.ReceivedMessage, o *azservicebus.AbandonMessageOptions) error {
	return a.r.AbandonMessage(ctx, m, o)
}

type abandonOnlyClient struct {
	sender   *fakeSender
	receiver *fakeReceiver
}

func (c *abandonOnlyClient) NewSender(string) (Sender, error) { return c.sender, nil }
func (c *abandonOnlyClient) NewReceiver(string) (Receiver, error) {
	return abandonOnlyReceiver{c.receiver}, nil
}

// SettleNative dead-letters the message natively (reason and description set), never completing
// or abandoning it, and never touching the body.
func TestSettleNativeDeadLettersWithReasonAndDescription(t *testing.T) {
	receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage("not json", "2", true)}}
	tr, err := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil {
		t.Fatalf("Pop: msg=%v err=%v", msg, err)
	}
	if err := tr.SettleNative(context.Background(), msg, "unsupported schema version", "declares version 2"); err != nil {
		t.Fatalf("SettleNative: %v", err)
	}
	if len(receiver.deadLettered) != 1 || len(receiver.completed) != 0 || len(receiver.abandoned) != 0 {
		t.Fatalf("dead-lettered = %d, completed = %d, abandoned = %d; want 1, 0, 0",
			len(receiver.deadLettered), len(receiver.completed), len(receiver.abandoned))
	}
	if receiver.dlReasons[0] != "unsupported schema version" || receiver.dlDetails[0] != "declares version 2" {
		t.Errorf("reason/description = %q/%q", receiver.dlReasons[0], receiver.dlDetails[0])
	}
	if string(receiver.deadLettered[0].Body) != "not json" {
		t.Errorf("body = %q, want it untouched", receiver.deadLettered[0].Body)
	}
}

// A receiver without DeadLetterMessage falls back to Abandon (MaxDeliveryCount takes over): still
// a native settle, still never a Complete.
func TestSettleNativeFallsBackToAbandon(t *testing.T) {
	receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage("not json", "2", true)}}
	tr, err := New(WithClient(&abandonOnlyClient{sender: &fakeSender{}, receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := tr.Pop(context.Background(), "orders", 0)
	if err := tr.SettleNative(context.Background(), msg, "poison", "x"); err != nil {
		t.Fatal(err)
	}
	if len(receiver.abandoned) != 1 || len(receiver.completed) != 0 || len(receiver.deadLettered) != 0 {
		t.Fatalf("abandoned = %d, completed = %d, dead-lettered = %d; want 1, 0, 0",
			len(receiver.abandoned), len(receiver.completed), len(receiver.deadLettered))
	}
}

func TestSettleNativeNoopWithoutHandle(t *testing.T) {
	tr, _ := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: &fakeReceiver{}}))
	if err := tr.SettleNative(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders"}, "poison", "x"); err != nil {
		t.Errorf("SettleNative with no handle returned %v", err)
	}
}

// drainOne runs one delivery through a real App over the transport.
func drainOne(t *testing.T, receiver *fakeReceiver, sender *fakeSender, opts ...babelqueue.AppOption) (handled int) {
	t.Helper()
	tr, err := New(WithClient(&fakeClient{sender: sender, receiver: receiver}))
	if err != nil {
		t.Fatal(err)
	}
	app := babelqueue.NewApp(tr, append([]babelqueue.AppOption{babelqueue.WithMaxAttempts(5)}, opts...)...)
	app.Handle(urn, func(context.Context, babelqueue.Envelope) error { handled++; return nil })
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	return handled
}

// ADR-0037 decision 1 rule 3: DLQ off + fail (default) + a version-gated message is never
// Completed: it is dead-lettered natively (reason "unsupported schema version") on the first
// delivery. Nothing is sent to <queue>.dlq and nothing is retried.
func TestAppVersionGatedDLQOffDeadLettersNativelyWithoutComplete(t *testing.T) {
	for name, opts := range map[string][]babelqueue.AppOption{
		"default fail": nil,
		"release":      {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease)},
	} {
		t.Run(name, func(t *testing.T) {
			receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage(sampleBody(t), "2", true)}}
			sender := &fakeSender{}
			if drainOne(t, receiver, sender, opts...) != 0 {
				t.Fatal("handler ran for a version-gated delivery")
			}
			if len(receiver.completed) != 0 || len(sender.sent) != 0 || len(receiver.deadLettered) != 1 {
				t.Fatalf("completed = %d, sent = %d, dead-lettered = %d; want 0, 0, 1",
					len(receiver.completed), len(sender.sent), len(receiver.deadLettered))
			}
			if receiver.dlReasons[0] != "unsupported schema version" {
				t.Errorf("DeadLetterReason = %q", receiver.dlReasons[0])
			}
		})
	}

	t.Run("delete strategy still completes", func(t *testing.T) {
		receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage(sampleBody(t), "2", true)}}
		sender := &fakeSender{}
		drainOne(t, receiver, sender, babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete))
		if len(receiver.completed) != 1 || len(receiver.deadLettered) != 0 || len(sender.sent) != 0 {
			t.Fatalf("completed = %d, dead-lettered = %d, sent = %d; want 1, 0, 0",
				len(receiver.completed), len(receiver.deadLettered), len(sender.sent))
		}
	})

	t.Run("dlq on writes the raw body then completes", func(t *testing.T) {
		body := sampleBody(t)
		receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage(body, "2", true)}}
		sender := &fakeSender{}
		drainOne(t, receiver, sender, babelqueue.WithDeadLetter(true))
		if len(sender.sent) != 1 || string(sender.sent[0].Body) != body || len(receiver.completed) != 1 || len(receiver.deadLettered) != 0 {
			t.Fatalf("sent = %d, completed = %d, dead-lettered = %d; want the raw body sent once, 1 complete, 0 native",
				len(sender.sent), len(receiver.completed), len(receiver.deadLettered))
		}
	})
}

// ADR-0037 decision 2 ("Go and Python"): with DLQ off a poison body (invalid JSON, identity-less
// object) is dead-lettered natively on the first delivery: no Complete, no re-send, no retry. With
// DLQ on it is the ADR-0035 record then Complete; delete drops with a Complete.
func TestAppPoisonDLQOffDeadLettersNativelyWithoutComplete(t *testing.T) {
	for _, body := range []string{"not json at all", `{"data":{"a":1}}`} {
		t.Run("dlq off "+body, func(t *testing.T) {
			receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{{Body: []byte(body), DeliveryCount: 1}}}
			sender := &fakeSender{}
			drainOne(t, receiver, sender)
			if len(receiver.completed) != 0 || len(sender.sent) != 0 || len(receiver.deadLettered) != 1 {
				t.Fatalf("completed = %d, sent = %d, dead-lettered = %d; want 0, 0, 1",
					len(receiver.completed), len(sender.sent), len(receiver.deadLettered))
			}
			if receiver.dlReasons[0] != "poison" {
				t.Errorf("DeadLetterReason = %q, want poison", receiver.dlReasons[0])
			}
			if string(receiver.deadLettered[0].Body) != body {
				t.Errorf("body = %q, want it untouched", receiver.deadLettered[0].Body)
			}
		})
		t.Run("dlq on "+body, func(t *testing.T) {
			receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{{Body: []byte(body), DeliveryCount: 1}}}
			sender := &fakeSender{}
			drainOne(t, receiver, sender, babelqueue.WithDeadLetter(true))
			if len(sender.sent) != 1 || len(receiver.completed) != 1 || len(receiver.deadLettered) != 0 {
				t.Fatalf("sent = %d, completed = %d, dead-lettered = %d; want 1, 1, 0",
					len(sender.sent), len(receiver.completed), len(receiver.deadLettered))
			}
			if !strings.Contains(string(sender.sent[0].Body), `"reason":"poison"`) {
				t.Errorf("DLQ record = %s; want the ADR-0035 poison record", sender.sent[0].Body)
			}
		})
	}
	t.Run("delete strategy drops with a Complete", func(t *testing.T) {
		receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{{Body: []byte("not json"), DeliveryCount: 1}}}
		sender := &fakeSender{}
		drainOne(t, receiver, sender, babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete))
		if len(receiver.completed) != 1 || len(receiver.deadLettered) != 0 || len(sender.sent) != 0 {
			t.Fatalf("completed = %d, dead-lettered = %d, sent = %d; want 1, 0, 0",
				len(receiver.completed), len(receiver.deadLettered), len(sender.sent))
		}
	})
}
