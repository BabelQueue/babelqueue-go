package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// noSleep is an injected sleeper that records nothing — keeps tests instant.
func noSleep(time.Duration) {}

// newEnvelope builds a canonical envelope for the given urn/queue.
func newEnvelope(t *testing.T, urn, queue string) babelqueue.Envelope {
	t.Helper()
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1042}, babelqueue.WithQueue(queue))
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// TestWrite_StoresEncodedBytesVerbatim is the GR-1/GR-5 parity check: what the outbox
// stores is byte-identical to the envelope codec's output, and the captured queue is the
// envelope's meta.queue.
func TestWrite_StoresEncodedBytesVerbatim(t *testing.T) {
	store := NewInMemoryStore()
	ob := New(store)
	env := newEnvelope(t, "urn:babel:orders:created", "orders")

	want, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}

	id, err := ob.Write(env)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("Write returned an empty outbox id")
	}

	recs, err := store.FetchUnpublished(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("stored %d rows, want 1", len(recs))
	}
	if recs[0].Body != string(want) {
		t.Errorf("stored body is not the encoded envelope verbatim:\n got %q\nwant %q", recs[0].Body, want)
	}
	if recs[0].Queue != "orders" {
		t.Errorf("captured queue = %q, want %q", recs[0].Queue, "orders")
	}
	if recs[0].ID != id {
		t.Errorf("fetched id %q != Write id %q", recs[0].ID, id)
	}
}

// TestWrite_DefaultsQueueWhenMetaBlank falls back to "default" when meta.queue is empty.
func TestWrite_DefaultsQueueWhenMetaBlank(t *testing.T) {
	store := NewInMemoryStore()
	ob := New(store)
	// An envelope with no queue set on it directly.
	env := babelqueue.Envelope{Job: "urn:babel:x:y", TraceID: "t", Data: map[string]any{}}

	if _, err := ob.Write(env); err != nil {
		t.Fatal(err)
	}
	recs, _ := store.FetchUnpublished(10)
	if len(recs) != 1 || recs[0].Queue != "default" {
		t.Fatalf("blank meta.queue should default to \"default\"; got %+v", recs)
	}
}

// TestRelay_PublishesVerbatimAndMarksPublished proves the relay forwards the stored bytes
// unchanged through the Transport (trace_id preserved, GR-4) and marks the row published.
func TestRelay_PublishesVerbatimAndMarksPublished(t *testing.T) {
	store := NewInMemoryStore()
	ob := New(store)
	tr := babelqueue.NewInMemoryTransport()
	relay := NewRelay(tr, store, Options{Sleeper: noSleep})

	env := newEnvelope(t, "urn:babel:orders:created", "orders")
	want, _ := env.Encode()
	if _, err := ob.Write(env); err != nil {
		t.Fatal(err)
	}

	res, err := relay.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Published != 1 || res.Failed != 0 {
		t.Fatalf("res = %+v, want {Published:1 Failed:0}", res)
	}
	if store.PendingCount() != 0 {
		t.Errorf("row should be marked published; pending = %d", store.PendingCount())
	}

	// The transport received the stored bytes verbatim — decode and confirm trace_id rode through.
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil {
		t.Fatalf("transport has no message on \"orders\": msg=%v err=%v", msg, err)
	}
	if msg.Body != string(want) {
		t.Errorf("relay did not publish the stored bytes verbatim:\n got %q\nwant %q", msg.Body, want)
	}
	got, err := babelqueue.Decode([]byte(msg.Body))
	if err != nil {
		t.Fatal(err)
	}
	if got.TraceID != env.TraceID {
		t.Errorf("trace_id not preserved end-to-end: got %q, want %q", got.TraceID, env.TraceID)
	}
}

// flakyTransport publishes everything except bodies whose queue is in failQueues, which
// always error — to exercise the MarkFailed / poison-row path.
type flakyTransport struct {
	*babelqueue.InMemoryTransport
	failQueue string
}

func (f *flakyTransport) Publish(ctx context.Context, queue, body string) error {
	if queue == f.failQueue {
		return errors.New("broker refused")
	}
	return f.InMemoryTransport.Publish(ctx, queue, body)
}

// TestRelay_FailingPublishMarksFailedAndContinues proves a poison row is marked failed,
// stays pending, and does not block the rest of the batch.
func TestRelay_FailingPublishMarksFailedAndContinues(t *testing.T) {
	store := NewInMemoryStore()
	ob := New(store)
	base := babelqueue.NewInMemoryTransport()
	tr := &flakyTransport{InMemoryTransport: base, failQueue: "poison"}
	relay := NewRelay(tr, store, Options{Sleeper: noSleep})

	// One good row, one poison row, one more good row — the poison one must not block the others.
	goodA, _ := ob.Write(newEnvelope(t, "urn:babel:orders:a", "orders"))
	poison, _ := ob.Write(newEnvelope(t, "urn:babel:orders:p", "poison"))
	goodB, _ := ob.Write(newEnvelope(t, "urn:babel:orders:b", "orders"))

	res, err := relay.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Published != 2 || res.Failed != 1 {
		t.Fatalf("res = %+v, want {Published:2 Failed:1}", res)
	}

	// The poison row stays pending; the two good rows are gone.
	if store.PendingCount() != 1 {
		t.Errorf("only the poison row should remain pending; pending = %d", store.PendingCount())
	}
	if store.AttemptsOf(poison) != 1 {
		t.Errorf("poison attempts = %d, want 1", store.AttemptsOf(poison))
	}
	if store.LastErrorOf(poison) != "broker refused" {
		t.Errorf("poison lastError = %q, want %q", store.LastErrorOf(poison), "broker refused")
	}
	if store.AttemptsOf(goodA) != 0 || store.AttemptsOf(goodB) != 0 {
		t.Error("good rows should not have recorded a failure")
	}
	if base.Size("orders") != 2 {
		t.Errorf("transport \"orders\" has %d messages, want 2 (both good rows)", base.Size("orders"))
	}
}

// TestRelay_DrainLoopsToEmpty proves Drain keeps flushing batches until the outbox is
// empty (multiple passes, batch smaller than the backlog).
func TestRelay_DrainLoopsToEmpty(t *testing.T) {
	store := NewInMemoryStore()
	ob := New(store)
	tr := babelqueue.NewInMemoryTransport()
	relay := NewRelay(tr, store, Options{BatchSize: 2, Sleeper: noSleep}) // 2 per pass

	const total = 5
	for i := 0; i < total; i++ {
		if _, err := ob.Write(newEnvelope(t, "urn:babel:orders:created", "orders")); err != nil {
			t.Fatal(err)
		}
	}

	res, err := relay.Drain(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Published != total || res.Failed != 0 {
		t.Fatalf("res = %+v, want {Published:%d Failed:0}", res, total)
	}
	if store.PendingCount() != 0 {
		t.Errorf("outbox not drained; pending = %d", store.PendingCount())
	}
	if tr.Size("orders") != total {
		t.Errorf("transport \"orders\" has %d messages, want %d", tr.Size("orders"), total)
	}
}

// TestRelay_DrainStopsWhenOnlyFailingRowsRemain proves Drain does not spin forever when a
// pass makes no progress (all remaining rows fail).
func TestRelay_DrainStopsWhenOnlyFailingRowsRemain(t *testing.T) {
	store := NewInMemoryStore()
	ob := New(store)
	base := babelqueue.NewInMemoryTransport()
	tr := &flakyTransport{InMemoryTransport: base, failQueue: "poison"}
	relay := NewRelay(tr, store, Options{Sleeper: noSleep})

	if _, err := ob.Write(newEnvelope(t, "urn:babel:orders:p", "poison")); err != nil {
		t.Fatal(err)
	}

	res, err := relay.Drain(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Published != 0 || res.Failed != 1 {
		t.Fatalf("res = %+v, want {Published:0 Failed:1} (one pass, no progress, stop)", res)
	}
	if store.PendingCount() != 1 {
		t.Errorf("the failing row should stay pending for a later Drain; pending = %d", store.PendingCount())
	}
}

// recordingSleeper captures every backoff duration the relay sleeps for.
type recordingSleeper struct {
	durations []time.Duration
}

func (s *recordingSleeper) sleep(d time.Duration) { s.durations = append(s.durations, d) }

// TestRelay_BackoffGrowsAndCaps asserts the injected sleeper sees a linearly-growing,
// then-capped backoff as a poison row's attempt count climbs across passes.
func TestRelay_BackoffGrowsAndCaps(t *testing.T) {
	store := NewInMemoryStore()
	ob := New(store)
	base := babelqueue.NewInMemoryTransport()
	tr := &flakyTransport{InMemoryTransport: base, failQueue: "poison"}

	rec := &recordingSleeper{}
	relay := NewRelay(tr, store, Options{
		BackoffStep: 10 * time.Millisecond,
		BackoffCap:  35 * time.Millisecond,
		Sleeper:     rec.sleep,
	})

	if _, err := ob.Write(newEnvelope(t, "urn:babel:orders:p", "poison")); err != nil {
		t.Fatal(err)
	}

	// Flush the same poison row five times; each pass bumps its attempt count, so the
	// backoff grows 10, 20, 30, then caps at 35, 35 ms.
	for i := 0; i < 5; i++ {
		if _, err := relay.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	want := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		35 * time.Millisecond, // capped (would be 40)
		35 * time.Millisecond, // capped (would be 50)
	}
	if len(rec.durations) != len(want) {
		t.Fatalf("recorded %d backoffs, want %d: %v", len(rec.durations), len(want), rec.durations)
	}
	for i, w := range want {
		if rec.durations[i] != w {
			t.Errorf("backoff[%d] = %v, want %v (full sequence %v)", i, rec.durations[i], w, rec.durations)
		}
	}
}

// TestRelay_EmptyOutboxIsNoOp proves flushing an empty outbox publishes nothing and errs not.
func TestRelay_EmptyOutboxIsNoOp(t *testing.T) {
	store := NewInMemoryStore()
	tr := babelqueue.NewInMemoryTransport()
	relay := NewRelay(tr, store, Options{Sleeper: noSleep})

	res, err := relay.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{}) {
		t.Errorf("flush of an empty outbox = %+v, want zero", res)
	}
}
