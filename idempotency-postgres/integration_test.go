package postgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"github.com/babelqueue/babelqueue-go/idempotency"
	postgres "github.com/babelqueue/babelqueue-go/idempotency-postgres"
)

// liveStore connects to the DSN in BABELQUEUE_TEST_PG and migrates the schema, or
// skips the test cleanly when the env var is unset (the no-DB default in CI / local
// `go test ./...`). Set e.g.
//
//	BABELQUEUE_TEST_PG='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable'
func liveStore(t *testing.T, opts ...postgres.Option) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("BABELQUEUE_TEST_PG")
	if dsn == "" {
		t.Skip("set BABELQUEUE_TEST_PG to run the PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := postgres.New(ctx, dsn, opts...)
	if err != nil {
		t.Skipf("postgres not reachable: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// TestIntegration_ConcurrentClaimsSerialize proves the ACID/parking contract end to
// end: N goroutines race to Claim the SAME id against a live Postgres; the unique
// PRIMARY KEY must let exactly ONE win. This is the in-flight serialization the
// in-memory store models, now durable across processes.
func TestIntegration_ConcurrentClaimsSerialize(t *testing.T) {
	store := liveStore(t)
	ctx := context.Background()
	id := fmt.Sprintf("concurrent-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = store.Forget(ctx, id) })

	const workers = 24
	var wins int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all goroutines at once to maximise contention
			won, err := store.Claim(ctx, id)
			if err != nil {
				t.Errorf("Claim: %v", err)
				return
			}
			if won {
				atomic.AddInt64(&wins, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if wins != 1 {
		t.Fatalf("exactly one concurrent Claim must win, got %d", wins)
	}
}

// TestIntegration_DuplicateRejected proves a duplicate delivery is detected as
// already-seen: the first Claim wins, the second loses, and Seen reports true.
func TestIntegration_DuplicateRejected(t *testing.T) {
	store := liveStore(t)
	ctx := context.Background()
	id := fmt.Sprintf("dup-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = store.Forget(ctx, id) })

	won, err := store.Claim(ctx, id)
	if err != nil || !won {
		t.Fatalf("first Claim won=%v err=%v, want won=true", won, err)
	}
	again, err := store.Claim(ctx, id)
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if again {
		t.Fatal("a duplicate delivery must NOT win the claim")
	}
	seen, err := store.Seen(ctx, id)
	if err != nil || !seen {
		t.Fatalf("Seen after claim = %v err=%v, want true", seen, err)
	}
}

// TestIntegration_TTLExpiryReclaims proves expiry is honored: after a short TTL
// lapses the id is no longer Seen and may be re-claimed.
func TestIntegration_TTLExpiryReclaims(t *testing.T) {
	store := liveStore(t, postgres.WithTTL(time.Second))
	ctx := context.Background()
	id := fmt.Sprintf("ttl-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = store.Forget(ctx, id) })

	if won, err := store.Claim(ctx, id); err != nil || !won {
		t.Fatalf("initial Claim won=%v err=%v", won, err)
	}
	time.Sleep(1500 * time.Millisecond) // let the TTL lapse

	seen, err := store.Seen(ctx, id)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatal("an expired id must not be seen")
	}
	if won, err := store.Claim(ctx, id); err != nil || !won {
		t.Fatalf("re-claim after expiry won=%v err=%v, want won=true", won, err)
	}
}

// TestIntegration_WrapDedupes drives the store through idempotency.Wrap end to end:
// the same id delivered twice runs the handler once.
func TestIntegration_WrapDedupes(t *testing.T) {
	store := liveStore(t)
	ctx := context.Background()
	id := fmt.Sprintf("wrap-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = store.Forget(ctx, id) })

	var calls int64
	h := idempotency.Wrap(store, func(context.Context, babelqueue.Envelope) error {
		atomic.AddInt64(&calls, 1)
		return nil
	})
	env := babelqueue.Envelope{
		Job:  "urn:babel:orders:created",
		Meta: babelqueue.Meta{ID: id, SchemaVersion: 1},
	}
	for i := 0; i < 3; i++ {
		if err := h(ctx, env); err != nil {
			t.Fatalf("handler: %v", err)
		}
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times, want 1 (redeliveries deduped)", calls)
	}
}
