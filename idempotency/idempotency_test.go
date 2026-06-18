package idempotency

import (
	"context"
	"errors"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

func msg(id string) babelqueue.Envelope {
	return babelqueue.Envelope{
		Job:     "urn:babel:orders:created",
		TraceID: "trace-1",
		Data:    map[string]any{"order_id": 7},
		Meta:    babelqueue.Meta{ID: id, Queue: "orders", Lang: "go", SchemaVersion: 1},
	}
}

func TestWrap_RunsAndRemembersOnFirstDelivery(t *testing.T) {
	store := NewInMemoryStore()
	calls := 0
	h := Wrap(store, func(_ context.Context, _ babelqueue.Envelope) error { calls++; return nil })

	if err := h(context.Background(), msg("msg-1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	if seen, _ := store.Seen(context.Background(), "msg-1"); !seen {
		t.Fatal("id should be remembered after a successful handler")
	}
}

func TestWrap_SkipsRedeliveryOfSameID(t *testing.T) {
	store := NewInMemoryStore()
	calls := 0
	h := Wrap(store, func(_ context.Context, _ babelqueue.Envelope) error { calls++; return nil })

	_ = h(context.Background(), msg("msg-1"))
	if err := h(context.Background(), msg("msg-1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (redelivery must be skipped)", calls)
	}
}

func TestWrap_RunsAgainForDifferentID(t *testing.T) {
	store := NewInMemoryStore()
	calls := 0
	h := Wrap(store, func(_ context.Context, _ babelqueue.Envelope) error { calls++; return nil })

	_ = h(context.Background(), msg("msg-1"))
	_ = h(context.Background(), msg("msg-2"))
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestWrap_DoesNotRememberWhenHandlerErrors(t *testing.T) {
	store := NewInMemoryStore()
	calls := 0
	boom := errors.New("boom")
	h := Wrap(store, func(_ context.Context, _ babelqueue.Envelope) error { calls++; return boom })

	if err := h(context.Background(), msg("msg-1")); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom to propagate so the runtime retries", err)
	}
	if seen, _ := store.Seen(context.Background(), "msg-1"); seen {
		t.Fatal("an errored id must not be remembered")
	}

	// A redelivery runs the handler again — retry works.
	_ = h(context.Background(), msg("msg-1"))
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestWrap_RunsWhenNoUsableID(t *testing.T) {
	store := NewInMemoryStore()
	calls := 0
	h := Wrap(store, func(_ context.Context, _ babelqueue.Envelope) error { calls++; return nil })

	_ = h(context.Background(), msg("")) // empty id → cannot dedupe → runs
	_ = h(context.Background(), msg("")) // still runs
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (no id → no dedupe)", calls)
	}
}

func TestWrap_SeenErrorPropagates(t *testing.T) {
	failing := &failingStore{seenErr: errors.New("store down")}
	calls := 0
	h := Wrap(failing, func(_ context.Context, _ babelqueue.Envelope) error { calls++; return nil })

	if err := h(context.Background(), msg("msg-1")); err == nil {
		t.Fatal("a Seen error must propagate so the message retries")
	}
	if calls != 0 {
		t.Fatalf("handler must not run when the dedup check fails; calls = %d", calls)
	}
}

func TestWrap_RememberErrorStillAcks(t *testing.T) {
	failing := &failingStore{rememberErr: errors.New("write failed")}
	calls := 0
	h := Wrap(failing, func(_ context.Context, _ babelqueue.Envelope) error { calls++; return nil })

	if err := h(context.Background(), msg("msg-1")); err != nil {
		t.Fatalf("a Remember failure after a successful handler must still ack; got %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestInMemoryStore_Forget(t *testing.T) {
	store := NewInMemoryStore()
	_ = store.Remember(context.Background(), "msg-1")
	if seen, _ := store.Seen(context.Background(), "msg-1"); !seen {
		t.Fatal("should be seen after Remember")
	}
	_ = store.Forget(context.Background(), "msg-1")
	if seen, _ := store.Seen(context.Background(), "msg-1"); seen {
		t.Fatal("should not be seen after Forget")
	}
}

type failingStore struct {
	seenErr     error
	rememberErr error
}

func (f *failingStore) Seen(_ context.Context, _ string) (bool, error) { return false, f.seenErr }
func (f *failingStore) Remember(_ context.Context, _ string) error     { return f.rememberErr }
func (f *failingStore) Forget(_ context.Context, _ string) error       { return nil }
