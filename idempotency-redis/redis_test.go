package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// fakeRedis is an in-process stand-in for the go-redis client surface the Store
// uses. It models the SET NX / EXISTS / DEL semantics (including atomic NX under a
// mutex) so the claim / duplicate / seen / forget logic is exercised with NO live
// Redis — the repo's fake-seam unit-test pattern. It is NOT a Redis emulator: TTL is
// recorded (to assert PX is passed) but keys are not auto-expired in the fake.
type fakeRedis struct {
	mu      sync.Mutex
	keys    map[string]time.Duration // present key -> the TTL it was SET with
	failErr error                    // when non-nil, every command returns this error
}

func newFakeRedis() *fakeRedis { return &fakeRedis{keys: map[string]time.Duration{}} }

func (f *fakeRedis) SetNX(_ context.Context, key string, _ any, ttl time.Duration) *goredis.BoolCmd {
	cmd := goredis.NewBoolCmd(context.Background())
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failErr != nil {
		cmd.SetErr(f.failErr)
		return cmd
	}
	if _, exists := f.keys[key]; exists {
		cmd.SetVal(false) // NX: key present -> not set
		return cmd
	}
	f.keys[key] = ttl
	cmd.SetVal(true) // NX: created
	return cmd
}

func (f *fakeRedis) Exists(_ context.Context, keys ...string) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(context.Background())
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failErr != nil {
		cmd.SetErr(f.failErr)
		return cmd
	}
	var n int64
	for _, k := range keys {
		if _, ok := f.keys[k]; ok {
			n++
		}
	}
	cmd.SetVal(n)
	return cmd
}

func (f *fakeRedis) Del(_ context.Context, keys ...string) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(context.Background())
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failErr != nil {
		cmd.SetErr(f.failErr)
		return cmd
	}
	var n int64
	for _, k := range keys {
		if _, ok := f.keys[k]; ok {
			delete(f.keys, k)
			n++
		}
	}
	cmd.SetVal(n)
	return cmd
}

// newFakeStore wires a Store onto the fake client so unit tests run with no Redis.
func newFakeStore(opts ...Option) (*Store, *fakeRedis) {
	fake := newFakeRedis()
	s := newStore(fake, opts...)
	return s, fake
}

// TestClaim_FirstDeliveryWins: the first Claim sets the key and reports a won race.
func TestClaim_FirstDeliveryWins(t *testing.T) {
	s, _ := newFakeStore()
	won, err := s.Claim(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if !won {
		t.Fatal("first delivery must win the claim")
	}
}

// TestClaim_DuplicateLoses: a second Claim of the same id sees the key present and
// reports a duplicate — the concurrency contract (duplicate detected as already-seen).
func TestClaim_DuplicateLoses(t *testing.T) {
	s, _ := newFakeStore()
	if _, err := s.Claim(context.Background(), "msg-1"); err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	won, err := s.Claim(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if won {
		t.Fatal("a duplicate delivery must NOT win the claim")
	}
}

// TestClaim_ConcurrentSerialize: many goroutines race to Claim the SAME id; the NX
// semantics must let exactly ONE win. This models the in-flight serialization the
// in-memory store guarantees, now backed by Redis NX.
func TestClaim_ConcurrentSerialize(t *testing.T) {
	s, _ := newFakeStore()
	const workers = 64
	var wins int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			won, err := s.Claim(context.Background(), "race")
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

// TestClaim_PassesTTL: a configured TTL must ride the SET NX command (PX), proving
// expiry is honored at write time.
func TestClaim_PassesTTL(t *testing.T) {
	s, fake := newFakeStore(WithTTL(2 * time.Hour))
	if _, err := s.Claim(context.Background(), "msg-ttl"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	fake.mu.Lock()
	got := fake.keys[DefaultPrefix+"msg-ttl"]
	fake.mu.Unlock()
	if got != 2*time.Hour {
		t.Fatalf("SET NX TTL = %v, want 2h", got)
	}
}

// TestRemember_DelegatesAndIsIdempotent: Remember drives the same NX set and a
// duplicate Remember is not an error (the key already exists afterwards either way).
func TestRemember_DelegatesAndIsIdempotent(t *testing.T) {
	s, fake := newFakeStore()
	if err := s.Remember(context.Background(), "msg-1"); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	if err := s.Remember(context.Background(), "msg-1"); err != nil {
		t.Fatalf("duplicate Remember must not error: %v", err)
	}
	fake.mu.Lock()
	_, present := fake.keys[DefaultPrefix+"msg-1"]
	fake.mu.Unlock()
	if !present {
		t.Fatal("key must exist after Remember")
	}
}

// TestSeen reflects key presence and absence.
func TestSeen(t *testing.T) {
	s, _ := newFakeStore()
	if seen, _ := s.Seen(context.Background(), "msg-1"); seen {
		t.Fatal("must not be seen before Remember")
	}
	_ = s.Remember(context.Background(), "msg-1")
	if seen, _ := s.Seen(context.Background(), "msg-1"); !seen {
		t.Fatal("must be seen after Remember")
	}
}

// TestForget evicts a remembered id.
func TestForget(t *testing.T) {
	s, _ := newFakeStore()
	_ = s.Remember(context.Background(), "msg-1")
	if err := s.Forget(context.Background(), "msg-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if seen, _ := s.Seen(context.Background(), "msg-1"); seen {
		t.Fatal("must not be seen after Forget")
	}
}

// TestWithPrefix_NamespacesKeys: a custom prefix isolates keys (so two apps on a
// shared Redis don't collide).
func TestWithPrefix_NamespacesKeys(t *testing.T) {
	s, fake := newFakeStore(WithPrefix("app1:"))
	_ = s.Remember(context.Background(), "id")
	fake.mu.Lock()
	_, present := fake.keys["app1:id"]
	fake.mu.Unlock()
	if !present {
		t.Fatal("WithPrefix must namespace the stored key")
	}
}

// TestSeenAndClaim_ErrorPropagates: a real client error surfaces from every method
// so Wrap retries the message instead of guessing the dedupe state.
func TestSeenAndClaim_ErrorPropagates(t *testing.T) {
	s, fake := newFakeStore()
	boom := errors.New("redis down")
	fake.failErr = boom

	if _, err := s.Seen(context.Background(), "x"); !errors.Is(err, boom) {
		t.Errorf("Seen error must propagate, got %v", err)
	}
	if _, err := s.Claim(context.Background(), "x"); !errors.Is(err, boom) {
		t.Errorf("Claim error must propagate, got %v", err)
	}
	if err := s.Remember(context.Background(), "x"); !errors.Is(err, boom) {
		t.Errorf("Remember error must propagate, got %v", err)
	}
	if err := s.Forget(context.Background(), "x"); !errors.Is(err, boom) {
		t.Errorf("Forget error must propagate, got %v", err)
	}
}

// TestClose_NoopForBorrowedClient: NewWithClient-style construction (no closer) must
// make Close a no-op so a borrowed client survives.
func TestClose_NoopForBorrowedClient(t *testing.T) {
	s, _ := newFakeStore()
	if err := s.Close(); err != nil {
		t.Fatalf("Close on a borrowed client must be a no-op, got %v", err)
	}
}
