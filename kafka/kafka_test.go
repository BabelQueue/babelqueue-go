package kafka

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	kafkago "github.com/segmentio/kafka-go"
)

const urn = "urn:babel:orders:created"

type fakeWriter struct{ sent []kafkago.Message }

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.sent = append(f.sent, msgs...)
	return nil
}

type fakeReader struct {
	messages  []kafkago.Message
	committed []kafkago.Message
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if len(f.messages) == 0 {
		<-ctx.Done() // emulate a blocking fetch that ends when the poll deadline fires
		return kafkago.Message{}, ctx.Err()
	}
	m := f.messages[0]
	f.messages = f.messages[1:]
	return m, nil
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.committed = append(f.committed, msgs...)
	return nil
}

func sampleBody(t *testing.T, attempts int) string {
	t.Helper()
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042}, babelqueue.WithQueue("orders"))
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

func headerMap(hs []kafkago.Header) map[string]string {
	m := make(map[string]string, len(hs))
	for _, h := range hs {
		m[h.Key] = string(h.Value)
	}
	return m
}

func newTransport(t *testing.T, fw *fakeWriter, fr *fakeReader) *Transport {
	t.Helper()
	tr, err := New(WithWriter(fw), WithReaderFactory(func(string) Reader { return fr }))
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestPublishProjectsHeadersValueTimestamp(t *testing.T) {
	fw := &fakeWriter{}
	tr := newTransport(t, fw, &fakeReader{})
	body := sampleBody(t, 0)

	if err := tr.Publish(context.Background(), "orders", body); err != nil {
		t.Fatal(err)
	}
	if len(fw.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(fw.sent))
	}
	m := fw.sent[0]
	if m.Topic != "orders" {
		t.Errorf("topic = %q, want orders", m.Topic)
	}
	if string(m.Value) != body {
		t.Errorf("value mismatch")
	}
	h := headerMap(m.Headers)
	if h["bq-job"] != urn {
		t.Errorf("bq-job = %q", h["bq-job"])
	}
	if h["bq-schema-version"] != "1" {
		t.Errorf("bq-schema-version = %q", h["bq-schema-version"])
	}
	if h["bq-attempts"] != "0" {
		t.Errorf("bq-attempts = %q", h["bq-attempts"])
	}
	env, _ := babelqueue.Decode([]byte(body))
	if m.Time.UnixMilli() != env.Meta.CreatedAt {
		t.Errorf("record time = %d, want created_at %d", m.Time.UnixMilli(), env.Meta.CreatedAt)
	}
	if h["bq-message-id"] != env.Meta.ID {
		t.Errorf("bq-message-id = %q, want %q", h["bq-message-id"], env.Meta.ID)
	}
}

func TestPopReconcilesAttemptsAndAcks(t *testing.T) {
	body := sampleBody(t, 0)
	rec := kafkago.Message{
		Topic:   "orders",
		Value:   []byte(body),
		Headers: []kafkago.Header{{Key: "bq-attempts", Value: []byte("2")}},
	}
	fr := &fakeReader{messages: []kafkago.Message{rec}}
	tr := newTransport(t, &fakeWriter{}, fr)

	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if msg == nil {
		t.Fatal("Pop returned nil")
	}
	env, _ := babelqueue.Decode([]byte(msg.Body))
	if env.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (from bq-attempts header)", env.Attempts)
	}

	if err := tr.Ack(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(fr.committed) != 1 {
		t.Errorf("committed %d, want 1", len(fr.committed))
	}
}

func TestPopEmptyReturnsNil(t *testing.T) {
	tr := newTransport(t, &fakeWriter{}, &fakeReader{})
	msg, err := tr.Pop(context.Background(), "orders", 20*time.Millisecond)
	if err != nil || msg != nil {
		t.Errorf("Pop = (%v, %v), want (nil, nil)", msg, err)
	}
}

func TestAckNoopWithoutHandle(t *testing.T) {
	tr := newTransport(t, &fakeWriter{}, &fakeReader{})
	if err := tr.Ack(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders", Handle: nil}); err != nil {
		t.Errorf("Ack with no handle returned %v", err)
	}
}

func TestReconcileHeaderAuthoritativeAndFallback(t *testing.T) {
	body := sampleBody(t, 3)

	// Header present → authoritative (overrides the body's own count).
	out := reconcileAttempts(body, []kafkago.Header{{Key: "bq-attempts", Value: []byte("1")}})
	if env, _ := babelqueue.Decode([]byte(out)); env.Attempts != 1 {
		t.Errorf("header-present attempts = %d, want 1", env.Attempts)
	}
	// Header absent → fall back to the body's attempts.
	if reconcileAttempts(body, nil) != body {
		t.Error("header-absent should leave the body unchanged (fallback to body attempts)")
	}
	// Garbage header → fall back to the body.
	if reconcileAttempts(body, []kafkago.Header{{Key: "bq-attempts", Value: []byte("x")}}) != body {
		t.Error("garbage bq-attempts should fall back to the body")
	}
}

func TestMessageWithGarbageBodyHasNoHeaders(t *testing.T) {
	m := message("orders", "not-json")
	if string(m.Value) != "not-json" {
		t.Errorf("value = %q", string(m.Value))
	}
	if len(m.Headers) != 0 {
		t.Errorf("headers = %v, want none for a non-envelope body", m.Headers)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(); err == nil {
		t.Error("New() with no config should error")
	}
	if _, err := New(WithBrokers("localhost:9092")); err == nil {
		t.Error("New(WithBrokers) without a group id should error")
	}
	if _, err := New(WithBrokers("localhost:9092"), WithGroupID("g"), WithMaxWaitTime(time.Second)); err != nil {
		t.Errorf("New with brokers + group id should succeed, got %v", err)
	}
}

// popGated fetches one record carrying the given bq-schema-version header (none when set is false).
func popGated(t *testing.T, body string, version string, set bool, opts ...Option) *babelqueue.ReceivedMessage {
	t.Helper()
	rec := kafkago.Message{Topic: "orders", Value: []byte(body)}
	if set {
		rec.Headers = []kafkago.Header{{Key: "bq-schema-version", Value: []byte(version)}}
	}
	fr := &fakeReader{messages: []kafkago.Message{rec}}
	opts = append([]Option{WithWriter(&fakeWriter{}), WithReaderFactory(func(string) Reader { return fr })}, opts...)
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
			msg := popGated(t, sampleBody(t, 0), c.version, c.set,
				WithOnSchemaVersionRejected(func(string, string) { called = true }))
			if msg.RouteURN != "" || called {
				t.Errorf("RouteURN = %q, hook called = %v; want neither", msg.RouteURN, called)
			}
			if _, err := babelqueue.Decode([]byte(msg.Body)); err != nil {
				t.Fatalf("Decode: %v", err)
			}
		})
	}
}

func TestPopSchemaGateRejectsUnsupportedWithoutDecoding(t *testing.T) {
	for _, v := range []string{"2", "x", "01", " 1", "1 ", "\u00a0", "\ufeff"} {
		t.Run(v, func(t *testing.T) {
			var gotVersion, gotQueue string
			// A body that is not JSON proves the gate runs before any decode / reconcile.
			msg := popGated(t, "not json", v, true,
				WithOnSchemaVersionRejected(func(version, queue string) { gotVersion, gotQueue = version, queue }))
			if want := unsupportedSchemaRoutePrefix + url.QueryEscape(v); msg.RouteURN != want {
				t.Errorf("RouteURN = %q, want %q", msg.RouteURN, want)
			}
			if msg.Body != "not json" {
				t.Errorf("Body = %q, want it handed over untouched", msg.Body)
			}
			if gotVersion != v || gotQueue != "orders" {
				t.Errorf("hook got (%q,%q), want (%q,%q)", gotVersion, gotQueue, v, "orders")
			}
			if _, ok := msg.Handle.(kafkago.Message); !ok {
				t.Error("Handle must stay the fetched record so Ack commits its offset")
			}
		})
	}
}

func TestPopSchemaGateFirstHeaderWinsAndHookPanicIsIgnored(t *testing.T) {
	rec := kafkago.Message{Topic: "orders", Value: []byte(sampleBody(t, 0)), Headers: []kafkago.Header{
		{Key: "bq-schema-version", Value: []byte("2")},
		{Key: "bq-schema-version", Value: []byte("1")},
	}}
	fr := &fakeReader{messages: []kafkago.Message{rec}}
	tr, err := New(WithWriter(&fakeWriter{}), WithReaderFactory(func(string) Reader { return fr }),
		WithOnSchemaVersionRejected(func(string, string) { panic("boom") }))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil {
		t.Fatalf("Pop: msg=%v err=%v", msg, err)
	}
	if msg.RouteURN == "" {
		t.Error("record was not gated on its first bq-schema-version header")
	}
	if err := tr.Ack(context.Background(), msg); err != nil || len(fr.committed) != 1 {
		t.Errorf("Ack: err=%v committed=%d, want the gated record committed once", err, len(fr.committed))
	}
}

func TestPopSchemaGateRejectFloorsDeliveryCountFromAttemptsHeader(t *testing.T) {
	cases := map[string]struct {
		attempts []kafkago.Header
		want     int
	}{
		"no header":   {nil, 1},
		"attempts 2":  {[]kafkago.Header{{Key: "bq-attempts", Value: []byte("2")}}, 3},
		"unparseable": {[]kafkago.Header{{Key: "bq-attempts", Value: []byte("x")}}, 1},
		"negative":    {[]kafkago.Header{{Key: "bq-attempts", Value: []byte("-4")}}, 1},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			hdrs := append([]kafkago.Header{{Key: "bq-schema-version", Value: []byte("2")}}, c.attempts...)
			rec := kafkago.Message{Topic: "orders", Value: []byte("not json"), Headers: hdrs}
			fr := &fakeReader{messages: []kafkago.Message{rec}}
			tr, err := New(WithWriter(&fakeWriter{}), WithReaderFactory(func(string) Reader { return fr }))
			if err != nil {
				t.Fatal(err)
			}
			msg, err := tr.Pop(context.Background(), "orders", 0)
			if err != nil || msg == nil {
				t.Fatalf("Pop: msg=%v err=%v", msg, err)
			}
			if msg.DeliveryCount != c.want {
				t.Errorf("DeliveryCount = %d, want %d", msg.DeliveryCount, c.want)
			}
		})
	}
}

// offsetLog models one partition plus a consumer group's committed offset, shared by every
// reader created over it. A reader starts at the committed offset (like a kafka-go group reader
// that rejoins), and committing an offset is cumulative (it commits everything below it).
type offsetLog struct {
	records   []kafkago.Message
	committed int64 // next offset to fetch for a fresh reader
	fetched   []int64
	commits   []int64
	opened    int
	closed    int
}

type offsetReader struct {
	log *offsetLog
	pos int64
}

func (l *offsetLog) newReader() *offsetReader {
	l.opened++
	return &offsetReader{log: l, pos: l.committed}
}

func (r *offsetReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if r.pos >= int64(len(r.log.records)) {
		<-ctx.Done()
		return kafkago.Message{}, ctx.Err()
	}
	m := r.log.records[r.pos]
	r.pos++
	r.log.fetched = append(r.log.fetched, m.Offset)
	return m, nil
}

func (r *offsetReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	for _, m := range msgs {
		r.log.commits = append(r.log.commits, m.Offset)
		if m.Offset+1 > r.log.committed {
			r.log.committed = m.Offset + 1
		}
	}
	return nil
}

func (r *offsetReader) Close() error { r.log.closed++; return nil }

// dlqWriter records writes and rejects those to the dead-letter topic while failDLQ is set.
type dlqWriter struct {
	failDLQ bool
	sent    []kafkago.Message
}

func (w *dlqWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	for _, m := range msgs {
		if m.Topic == "orders.dlq" && w.failDLQ {
			return errors.New("dlq topic unavailable")
		}
		w.sent = append(w.sent, m)
	}
	return nil
}

// A gated record whose DLQ write fails must not be committed (the commit is cumulative, so
// committing it - or anything after it - would lose it): the transport rewinds the partition to
// the record's offset, the next poll hands the same record again, the committed offset never
// passes it, and once the DLQ recovers it is dead-lettered, committed, and the stream moves on.
func TestGatedDeadLetterFailureRewindsAndNeverCommitsPastTheRecord(t *testing.T) {
	gated := kafkago.Message{Topic: "orders", Partition: 0, Offset: 0, Value: []byte("not json"),
		Headers: []kafkago.Header{{Key: "bq-schema-version", Value: []byte("2")}}}
	next := kafkago.Message{Topic: "orders", Partition: 0, Offset: 1, Value: []byte(sampleBody(t, 0))}
	log := &offsetLog{records: []kafkago.Message{gated, next}}
	w := &dlqWriter{failDLQ: true}

	tr, err := New(WithWriter(w), WithReaderFactory(func(string) Reader { return log.newReader() }),
		WithMaxWaitTime(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	handled := 0
	app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true), babelqueue.WithRewindPause(0))
	app.Handle(urn, func(context.Context, babelqueue.Envelope) error { handled++; return nil })

	// Three polls while the DLQ is down: the gated record comes back every time, record 1 is
	// never reached, nothing is committed.
	if _, err := app.Drain(context.Background(), "orders", 3); err != nil {
		t.Fatal(err)
	}
	for i, off := range log.fetched {
		if off != 0 {
			t.Fatalf("fetch #%d returned offset %d; the rewound partition must keep returning offset 0", i, off)
		}
	}
	if len(log.fetched) != 3 || log.closed != 3 || log.opened != 3 {
		t.Fatalf("fetched = %v, readers closed = %d, opened = %d; want 3 fetches of offset 0, 3 rewinds (close), 3 readers (a rewound reader is reopened lazily)",
			log.fetched, log.closed, log.opened)
	}
	if len(log.commits) != 0 || log.committed != 0 {
		t.Fatalf("commits = %v, committed offset = %d; nothing may be committed while the DLQ write fails", log.commits, log.committed)
	}
	if handled != 0 {
		t.Fatalf("handler ran %d times", handled)
	}
	if len(w.sent) != 0 {
		t.Fatalf("writes = %d; nothing may be re-published to the source topic (no retry path)", len(w.sent))
	}

	// The DLQ recovers: the gated record is dead-lettered and committed, then the next one is handled.
	w.failDLQ = false
	log.fetched = nil
	if _, err := app.Drain(context.Background(), "orders", 2); err != nil {
		t.Fatal(err)
	}
	if len(w.sent) != 1 || w.sent[0].Topic != "orders.dlq" {
		t.Fatalf("writes = %+v; want exactly one DLQ record", w.sent)
	}
	if len(log.fetched) != 2 || log.fetched[0] != 0 || log.fetched[1] != 1 {
		t.Fatalf("fetched = %v, want [0 1]", log.fetched)
	}
	if len(log.commits) != 2 || log.commits[0] != 0 || log.commits[1] != 1 || log.committed != 2 {
		t.Fatalf("commits = %v, committed = %d; want offsets 0 then 1 in order", log.commits, log.committed)
	}
	if handled != 1 {
		t.Fatalf("handler ran %d times, want 1 (the well-formed record only)", handled)
	}
}

func TestRewindWithoutACachedReaderIsANoOp(t *testing.T) {
	tr, err := New(WithWriter(&fakeWriter{}), WithReaderFactory(func(string) Reader { return &fakeReader{} }))
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Rewind(context.Background(), &babelqueue.ReceivedMessage{Queue: "orders"}); err != nil {
		t.Fatalf("Rewind: %v", err)
	}
}

// A hostile bq-schema-version header is bounded to 64 characters plus "..." in the hook value,
// the route URN and therefore the DLQ error; exactly 64 characters is left alone.
func TestPopSchemaGateTruncatesLongDeclaredVersion(t *testing.T) {
	long := strings.Repeat("9", 5000)
	var hooked string
	msg := popGated(t, "not json", long, true,
		WithOnSchemaVersionRejected(func(version, _ string) { hooked = version }))
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
