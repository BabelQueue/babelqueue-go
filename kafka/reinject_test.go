package kafka

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	kafkago "github.com/segmentio/kafka-go"
)

// eventLog records writes and commits in the order they happen, safe across goroutines.
type eventLog struct {
	mu     sync.Mutex
	events []string
	writes []kafkago.Message
	failN  int // the first failN writes fail
	tried  int
	notify chan struct{}
}

func newEventLog() *eventLog { return &eventLog{notify: make(chan struct{}, 64)} }

func (l *eventLog) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tried++
	if l.tried <= l.failN {
		l.events = append(l.events, "write-failed")
		return errors.New("broker unavailable")
	}
	l.events = append(l.events, "write")
	l.writes = append(l.writes, msgs...)
	return nil
}

func (l *eventLog) commit(offset int64) {
	l.mu.Lock()
	l.events = append(l.events, "commit")
	l.mu.Unlock()
	l.notify <- struct{}{}
	_ = offset
}

func (l *eventLog) snapshot() ([]string, []kafkago.Message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...), append([]kafkago.Message(nil), l.writes...)
}

// feedReader hands the queued records to FetchMessage and reports commits to the event log.
type feedReader struct {
	in  chan kafkago.Message
	log *eventLog
}

func (f *feedReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	select {
	case m := <-f.in:
		return m, nil
	case <-ctx.Done():
		return kafkago.Message{}, ctx.Err()
	}
}

func (f *feedReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	for _, m := range msgs {
		f.log.commit(m.Offset)
	}
	return nil
}

// startReinjector runs a Reinjector for topic "orders" with a fixed clock and the given sleep hook.
// It returns the feed for retry topic tier 1 and a stop function that waits for Run to return.
func startReinjector(t *testing.T, log *eventLog, now time.Time, sleep func(context.Context, time.Duration) error) (feed chan kafkago.Message, stop func()) {
	t.Helper()
	feed = make(chan kafkago.Message, 16)
	tr, err := New(WithWriter(log),
		WithReaderFactory(func(string) Reader { return &feedReader{in: feed, log: log} }),
		WithRetryTiers(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r, err := tr.NewReinjector("orders")
	if err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return now }
	if sleep != nil {
		r.sleep = sleep
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return feed, func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	}
}

func waitCommits(t *testing.T, log *eventLog, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-log.notify:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for commit %d of %d", i+1, n)
		}
	}
}

func noSleep(context.Context, time.Duration) error { return nil }

// The whole retry path: a failing message is republished to orders.retry.1 and the re-injector
// returns it to orders with the retry record's value and key byte for byte, without bq-delay and
// bq-original-topic, with traceparent kept (A6/A7).
func TestReinjectReturnsRetryRecordByteForByte(t *testing.T) {
	// 1. The failing delivery is republished to the first tier.
	retryWriter := &fakeWriter{}
	tr, err := New(WithWriter(retryWriter), WithReaderFactory(func(string) Reader { return &fakeReader{} }),
		WithRetryTiers(DefaultRetryTiers...))
	if err != nil {
		t.Fatal(err)
	}
	from := &babelqueue.ReceivedMessage{
		Body:  sampleBody(t, 0),
		Queue: "orders",
		Handle: kafkago.Message{
			Key: []byte("customer-0042"),
			Headers: []kafkago.Header{
				{Key: "traceparent", Value: []byte("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")},
				{Key: "x-other", Value: []byte("dropped-by-retry-publish")},
			},
		},
	}
	if err := tr.PublishRetryForAttempt(context.Background(), from, 0); err != nil {
		t.Fatal(err)
	}
	if len(retryWriter.sent) != 1 || retryWriter.sent[0].Topic != "orders.retry.1" {
		t.Fatalf("retry write = %+v, want one record on orders.retry.1", retryWriter.sent)
	}
	retryRec := retryWriter.sent[0]
	retryRec.Partition, retryRec.Offset = 0, 7

	// 2. The re-injector drains it back to the work topic.
	log := newEventLog()
	feed, stop := startReinjector(t, log, retryRec.Time.Add(time.Hour), noSleep)
	defer stop()
	feed <- retryRec
	waitCommits(t, log, 1)

	events, writes := log.snapshot()
	if len(writes) != 1 {
		t.Fatalf("writes = %+v, want 1", writes)
	}
	out := writes[0]
	if out.Topic != "orders" {
		t.Errorf("topic = %q, want orders", out.Topic)
	}
	if !bytes.Equal(out.Value, retryRec.Value) {
		t.Errorf("value = %q, want %q", out.Value, retryRec.Value)
	}
	if !bytes.Equal(out.Key, retryRec.Key) || string(out.Key) != "customer-0042" {
		t.Errorf("key = %q, want %q", out.Key, retryRec.Key)
	}
	got := headerMap(out.Headers)
	if _, ok := got["bq-delay"]; ok {
		t.Error("bq-delay must be dropped")
	}
	if _, ok := got["bq-original-topic"]; ok {
		t.Error("bq-original-topic must be dropped")
	}
	if got["traceparent"] != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Errorf("traceparent = %q, want it kept", got["traceparent"])
	}
	if got["bq-attempts"] != "1" || got["bq-job"] != urn {
		t.Errorf("other headers must be kept, got %v", got)
	}
	if len(events) != 2 || events[0] != "write" || events[1] != "commit" {
		t.Errorf("events = %v, want write then commit", events)
	}
	env, derr := babelqueue.Decode(out.Value)
	if derr != nil || env.Attempts != 1 {
		t.Errorf("attempts = %d (%v), want the republish's 1 untouched", env.Attempts, derr)
	}
}

// An undecodable retry record is carried as is: the re-injector never decodes or re-encodes.
func TestReinjectCarriesUndecodableRecordAsIs(t *testing.T) {
	log := newEventLog()
	feed, stop := startReinjector(t, log, time.Unix(1_700_000_000, 0), noSleep)
	defer stop()
	raw := []byte("{not json, \xff\xfe")
	feed <- kafkago.Message{Topic: "orders.retry.1", Key: nil, Value: raw, Time: time.Unix(1_700_000_000, 0),
		Headers: []kafkago.Header{{Key: "bq-original-topic", Value: []byte("orders")}, {Key: "bq-delay", Value: []byte("5000")}}}
	waitCommits(t, log, 1)
	_, writes := log.snapshot()
	if len(writes) != 1 || writes[0].Topic != "orders" || !bytes.Equal(writes[0].Value, raw) || writes[0].Key != nil {
		t.Fatalf("writes = %+v, want the raw bytes on orders", writes)
	}
}

// The wait is anchored on record.timestamp + tier and clamped to [now, now+tier] (A6).
func TestReinjectWaitClampsToTier(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	tier := 5 * time.Second
	r := &Reinjector{now: func() time.Time { return now }}
	cases := []struct {
		name string
		at   time.Time
		want time.Duration
	}{
		{"just written waits a full tier", now, 5 * time.Second},
		{"half elapsed waits the rest", now.Add(-2 * time.Second), 3 * time.Second},
		{"older than the tier is carried without waiting", now.Add(-time.Hour), 0},
		{"exactly one tier old is carried without waiting", now.Add(-tier), 0},
		{"future timestamp waits at most one tier", now.Add(time.Hour), 5 * time.Second},
		{"no timestamp is carried without waiting", time.Time{}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := r.waitFor(kafkago.Message{Time: c.at}, tier); got != c.want {
				t.Errorf("waitFor = %s, want %s", got, c.want)
			}
		})
	}
}

// The re-injector sleeps the computed wait and only then writes: an old record is carried at once.
func TestReinjectSleepsBeforeWriting(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	log := newEventLog()
	var mu sync.Mutex
	var slept []time.Duration
	sleep := func(_ context.Context, d time.Duration) error {
		mu.Lock()
		slept = append(slept, d)
		mu.Unlock()
		log.mu.Lock()
		log.events = append(log.events, "sleep")
		log.mu.Unlock()
		return nil
	}
	feed, stop := startReinjector(t, log, now, sleep)
	defer stop()
	hdr := []kafkago.Header{{Key: "bq-original-topic", Value: []byte("orders")}}
	feed <- kafkago.Message{Topic: "orders.retry.1", Value: []byte(`{}`), Time: now.Add(-time.Hour), Headers: hdr}
	feed <- kafkago.Message{Topic: "orders.retry.1", Value: []byte(`{}`), Time: now.Add(-2 * time.Second), Headers: hdr}
	waitCommits(t, log, 2)

	mu.Lock()
	defer mu.Unlock()
	if len(slept) < 2 || slept[0] != 0 || slept[1] != 3*time.Second {
		t.Fatalf("slept = %v, want [0s 3s ...]", slept)
	}
	events, _ := log.snapshot()
	want := []string{"sleep", "write", "commit", "sleep", "write", "commit"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

// The offset is committed only after the write is acknowledged; a failed write is retried and
// never committed (at-least-once).
func TestReinjectCommitsOnlyAfterWriteAck(t *testing.T) {
	log := newEventLog()
	log.failN = 2
	var mu sync.Mutex
	pauses := 0
	sleep := func(_ context.Context, d time.Duration) error {
		if d > 0 {
			mu.Lock()
			pauses++
			mu.Unlock()
		}
		return nil
	}
	feed, stop := startReinjector(t, log, time.Unix(1_700_000_000, 0), sleep)
	defer stop()
	feed <- kafkago.Message{Topic: "orders.retry.1", Value: []byte(`{"x":1}`), Time: time.Unix(0, 0),
		Headers: []kafkago.Header{{Key: "bq-original-topic", Value: []byte("orders")}}}
	waitCommits(t, log, 1)

	events, writes := log.snapshot()
	want := []string{"write-failed", "write-failed", "write", "commit"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
	if len(writes) != 1 {
		t.Fatalf("acknowledged writes = %d, want 1", len(writes))
	}
	mu.Lock()
	defer mu.Unlock()
	if pauses != 2 {
		t.Errorf("backoff pauses = %d, want 2", pauses)
	}
}

// A record waiting on one partition does not hold up the other partitions.
func TestReinjectPartitionsWaitIndependently(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	log := newEventLog()
	release := make(chan struct{})
	sleep := func(ctx context.Context, d time.Duration) error {
		if d == 0 {
			return nil
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	feed, stop := startReinjector(t, log, now, sleep)
	defer stop()
	hdr := []kafkago.Header{{Key: "bq-original-topic", Value: []byte("orders")}}
	// Partition 0 is still inside its tier (blocks); partition 1 is old (carried at once).
	feed <- kafkago.Message{Topic: "orders.retry.1", Partition: 0, Offset: 0, Value: []byte(`{"p":0}`), Time: now, Headers: hdr}
	feed <- kafkago.Message{Topic: "orders.retry.1", Partition: 1, Offset: 0, Value: []byte(`{"p":1}`), Time: now.Add(-time.Hour), Headers: hdr}
	waitCommits(t, log, 1)
	_, writes := log.snapshot()
	if len(writes) != 1 || string(writes[0].Value) != `{"p":1}` {
		t.Fatalf("writes while partition 0 waits = %+v, want only partition 1's record", writes)
	}
	close(release)
	waitCommits(t, log, 1)
	if _, writes = log.snapshot(); len(writes) != 2 {
		t.Fatalf("writes after release = %d, want 2", len(writes))
	}
}

func TestReinjectTargetFallsBackToTopicName(t *testing.T) {
	got := reinjectRecord(kafkago.Message{Topic: "orders.v2.retry.3", Value: []byte("v")})
	if got.Topic != "orders.v2" {
		t.Errorf("topic = %q, want orders.v2", got.Topic)
	}
}

func TestNewReinjectorNeedsTiersAndTopics(t *testing.T) {
	tr, err := New(WithWriter(&fakeWriter{}), WithReaderFactory(func(string) Reader { return &fakeReader{} }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.NewReinjector("orders"); !errors.Is(err, ErrDelayUnsupported) {
		t.Errorf("no tiers: err = %v, want ErrDelayUnsupported", err)
	}
	tr, _ = New(WithWriter(&fakeWriter{}), WithReaderFactory(func(string) Reader { return &fakeReader{} }), WithRetryTiers(time.Second))
	if _, err := tr.NewReinjector(); err == nil {
		t.Error("no work topics: want an error")
	}
}

// A message whose attempts_before is at or beyond the tier count lands on the last tier (§6.5).
func TestPublishRetryForAttemptKeepsLastTier(t *testing.T) {
	for _, before := range []int{3, 4, 10} {
		w := &fakeWriter{}
		tr, _ := New(WithWriter(w), WithReaderFactory(func(string) Reader { return &fakeReader{} }), WithRetryTiers(DefaultRetryTiers...))
		from := &babelqueue.ReceivedMessage{Body: sampleBody(t, before), Queue: "orders"}
		if err := tr.PublishRetryForAttempt(context.Background(), from, before); err != nil {
			t.Fatal(err)
		}
		rec := w.sent[0]
		h := headerMap(rec.Headers)
		if rec.Topic != "orders.retry.4" || h["bq-delay"] != "1800000" {
			t.Errorf("attempts_before=%d: topic=%q bq-delay=%q, want orders.retry.4 / 1800000", before, rec.Topic, h["bq-delay"])
		}
		if h["bq-attempts"] != "4"[:0]+itoa(before+1) {
			t.Errorf("attempts_before=%d: bq-attempts = %q", before, h["bq-attempts"])
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

// The deprecated PublishRetry keeps its delay-to-tier meaning, whatever the attempt count (A8).
func TestPublishRetryKeepsDelayToTierMapping(t *testing.T) {
	w := &fakeWriter{}
	tr, _ := New(WithWriter(w), WithReaderFactory(func(string) Reader { return &fakeReader{} }), WithRetryTiers(DefaultRetryTiers...))
	from := &babelqueue.ReceivedMessage{Body: sampleBody(t, 3), Queue: "orders", Handle: kafkago.Message{Key: []byte("k")}}
	if err := tr.PublishRetry(context.Background(), from, 20*time.Second); err != nil {
		t.Fatal(err)
	}
	rec := w.sent[0]
	h := headerMap(rec.Headers)
	if rec.Topic != "orders.retry.2" || h["bq-delay"] != "20000" || h["bq-attempts"] != "4" || string(rec.Key) != "k" {
		t.Errorf("topic=%q headers=%v key=%q", rec.Topic, h, rec.Key)
	}
	if err := tr.PublishRetry(context.Background(), from, time.Hour); !errors.Is(err, ErrDelayUnsupported) {
		t.Errorf("delay beyond the last tier: err = %v, want ErrDelayUnsupported", err)
	}
}
