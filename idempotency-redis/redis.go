// Package redis is a Redis-backed [idempotency.Store] for the BabelQueue Go
// runtime: a shared, persistent record of processed message ids so a fleet of
// consumers dedupes on the envelope's meta.id (ADR-0022) instead of every worker
// keeping its own in-memory set.
//
//	store, _ := redis.New("redis://localhost:6379/0", redis.WithTTL(24*time.Hour))
//	defer store.Close()
//	app.Handle(urn, idempotency.Wrap(store, handler))
//
// It satisfies the dependency-free [idempotency.Store] interface from the core
// (Seen / Remember / Forget) with the SAME contract the reference in-memory store
// guarantees, and adds atomic-claim semantics so concurrent consumers serialize:
//
//   - Remember is an ATOMIC claim — SET key value NX [PX ttl]. Redis evaluates NX
//     ("set only if Not eXists") on a single thread, so exactly one of N concurrent
//     callers sets the key (and is the "winner"); a duplicate delivery sees the key
//     already present and is a no-op. This is race-safe across processes — the key's
//     existence is the lock.
//   - Claim exposes that race result directly: true iff THIS caller set the key
//     (first delivery), false when the key already existed (duplicate / in-flight
//     "parked" delivery). Remember delegates to Claim and discards the bool, so the
//     [idempotency.Store] contract is preserved exactly.
//   - Seen reports whether the key currently exists.
//   - TTL/expiry is honored by Redis natively via PX: a key auto-expires after the
//     TTL, after which Seen is false and the id may be re-claimed (the documented
//     at-least-once behaviour). With no TTL the key persists until Forget.
//
// The GR-7 zero-dependency core stays dependency-free: this driver lives in its own
// submodule (its own go.mod) and is the only place go-redis is pulled in — the same
// client the …/redis *transport* submodule already uses, so a consumer importing
// both shares one driver version.
package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/babelqueue/babelqueue-go/idempotency"
)

// DefaultPrefix is the key namespace used when [WithPrefix] is not given. The stored
// key is prefix + message id.
const DefaultPrefix = "bq:idemp:"

// claimValue is the placeholder value stored under a claimed key. The Store is a
// seen-set, so the value is irrelevant — only the key's existence matters; a tiny
// constant keeps the footprint minimal.
const claimValue = "1"

// cmdable is the subset of the go-redis client surface this Store uses. The concrete
// *goredis.Client (and *goredis.ClusterClient) satisfy it; tests inject a fake so the
// claim / seen / forget logic runs WITHOUT a live Redis (the repo's fake-seam pattern).
type cmdable interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *goredis.BoolCmd
	Exists(ctx context.Context, keys ...string) *goredis.IntCmd
	Del(ctx context.Context, keys ...string) *goredis.IntCmd
}

// Store is a Redis-backed [idempotency.Store]. Construct it with [New] (owns the
// client) or [NewWithClient] (bring your own go-redis client).
type Store struct {
	client cmdable
	prefix string
	ttl    time.Duration // 0 = keys never expire (kept until Forget)
	closer func() error  // closes the client iff this Store opened it
}

// Option customizes a Store.
type Option func(*Store)

// WithPrefix overrides the key namespace (default [DefaultPrefix]). Use distinct
// prefixes to isolate idempotency keys per app on a shared Redis.
func WithPrefix(prefix string) Option {
	return func(s *Store) { s.prefix = prefix }
}

// WithTTL sets how long a remembered id stays "seen" before Redis expires it (PX)
// and it may be re-processed. Zero (the default) means keys never expire on their
// own — evict with Forget. A negative TTL is treated as zero.
func WithTTL(ttl time.Duration) Option {
	return func(s *Store) {
		if ttl < 0 {
			ttl = 0
		}
		s.ttl = ttl
	}
}

// New connects to Redis from a redis:// or rediss:// URL and returns a Store. The
// caller owns the lifetime: call [Store.Close] to release the client.
func New(url string, opts ...Option) (*Store, error) {
	options, err := goredis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	client := goredis.NewClient(options)
	s := newStore(client, opts...)
	s.closer = client.Close
	return s, nil
}

// NewWithClient wraps an existing go-redis client (bring your own pool / TLS / auth).
// The caller keeps ownership: [Store.Close] is a no-op for a borrowed client.
func NewWithClient(client *goredis.Client, opts ...Option) *Store {
	return newStore(client, opts...)
}

// newStore is the shared constructor over the cmdable seam (so tests can inject a
// fake client). Public constructors set closer when they own the client.
func newStore(client cmdable, opts ...Option) *Store {
	s := &Store{client: client, prefix: DefaultPrefix}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// key returns the namespaced Redis key for a message id.
func (s *Store) key(messageID string) string {
	return s.prefix + messageID
}

// Claim atomically records messageID and reports whether THIS call won the race —
// true on the first delivery (the key was set), false when the key already existed
// (a duplicate / in-flight "parked" delivery).
//
// The race-safety comes from SET key value NX [PX ttl]: Redis applies NX on its
// single command thread, so under any number of concurrent consumers exactly one
// SET creates the key and reports a claim; the rest see it present and report a
// duplicate. The TTL (when set) rides the same atomic command, so the key both
// appears and gets its expiry in one round trip.
//
// This is the atomic primitive [Remember] is built on; the [idempotency.Store]
// interface does not expose it, so it is available only on a *Store.
func (s *Store) Claim(ctx context.Context, messageID string) (bool, error) {
	set, err := s.client.SetNX(ctx, s.key(messageID), claimValue, s.ttl).Result()
	if err != nil {
		return false, err
	}
	return set, nil
}

// Remember records messageID as processed (atomic claim; the [idempotency.Store]
// write side). It delegates to [Store.Claim] and discards whether this caller won
// the race — the seen-set contract only needs the key to exist afterwards.
func (s *Store) Remember(ctx context.Context, messageID string) error {
	_, err := s.Claim(ctx, messageID)
	return err
}

// Seen reports whether messageID's key currently exists — the [idempotency.Store]
// read side. An expired (TTL-lapsed) key is already gone, so it returns false and
// the id may be re-processed.
func (s *Store) Seen(ctx context.Context, messageID string) (bool, error) {
	n, err := s.client.Exists(ctx, s.key(messageID)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Forget drops messageID from the store (manual eviction; the [idempotency.Store]
// contract). Removing an absent id is a no-op.
func (s *Store) Forget(ctx context.Context, messageID string) error {
	return s.client.Del(ctx, s.key(messageID)).Err()
}

// Close releases the underlying client when this Store opened it ([New]). For a
// borrowed client ([NewWithClient]) it is a no-op, leaving the caller's client intact.
func (s *Store) Close() error {
	if s.closer != nil {
		return s.closer()
	}
	return nil
}

// Compile-time proof the persistent store satisfies the core's dependency-free
// Store interface, so idempotency.Wrap accepts it unchanged.
var _ idempotency.Store = (*Store)(nil)
