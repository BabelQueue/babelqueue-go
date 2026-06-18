// Package idempotency adds optional, dependency-free dedupe to a babelqueue
// consumer: it wraps a [babelqueue.Handler] so a message whose meta.id was already
// processed is skipped instead of run again (ADR-0022). It is the Go mirror of the
// PHP BabelQueue\Idempotency helper.
//
// It lives in the core module (zero dependencies, stdlib only) rather than a
// transport submodule, so it ships with every consumer.
package idempotency

import (
	"context"
	"sync"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// Store is a pluggable record of message ids that have already been processed,
// keyed on the envelope's meta.id (the canonical per-message identity). The
// reference [InMemoryStore] is for tests / single-process consumers; production
// backends (Redis, a database table) implement the same three methods.
//
// The contract is "seen-set" dedupe: it answers "was this id processed?", not
// "what did it return" — queue handlers have no response to replay. It provides
// post-success dedupe under at-least-once + idempotent handlers (error-handling.md
// §1), not exactly-once and not in-flight concurrency locking. A transactional /
// outbox mode is a documented future direction (ADR-0022).
type Store interface {
	// Seen reports whether this message id has already been processed (remembered).
	Seen(ctx context.Context, messageID string) (bool, error)
	// Remember records this message id as processed.
	Remember(ctx context.Context, messageID string) error
	// Forget drops a message id from the store (manual eviction; a backend may also
	// expire ids on its own TTL).
	Forget(ctx context.Context, messageID string) error
}

// InMemoryStore is a process-local, goroutine-safe [Store] backed by a map. It is
// suitable for tests and single-process consumers; it is not shared across workers
// and not persistent — use a Redis- or database-backed store for production fleets.
type InMemoryStore struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewInMemoryStore returns an empty in-memory store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{seen: make(map[string]struct{})}
}

// Seen implements [Store].
func (s *InMemoryStore) Seen(_ context.Context, messageID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.seen[messageID]
	return ok, nil
}

// Remember implements [Store].
func (s *InMemoryStore) Remember(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[messageID] = struct{}{}
	return nil
}

// Forget implements [Store].
func (s *InMemoryStore) Forget(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.seen, messageID)
	return nil
}

// Wrap returns handler decorated so a message whose meta.id was already processed
// successfully is skipped. It composes with the App's ack-on-return /
// retry-on-error contract:
//
//   - an already-seen id → returns nil, so the runtime acks it and the broker
//     stops redelivering;
//   - the handler returns an error → the id is left unmarked and the error
//     propagates, so retry / dead-letter still apply and a later delivery runs the
//     handler again;
//   - a message with no usable meta.id runs unchanged (fail-open).
//
// Register it like any handler: app.Handle(urn, idempotency.Wrap(store, handler)).
func Wrap(store Store, handler babelqueue.Handler) babelqueue.Handler {
	return func(ctx context.Context, env babelqueue.Envelope) error {
		id := env.Meta.ID

		// No usable id → cannot dedupe; run the handler unchanged.
		if id == "" {
			return handler(ctx, env)
		}

		seen, err := store.Seen(ctx, id)
		if err != nil {
			// Don't guess the dedup state — surface the error so the message is
			// retried (the runtime's retry / dead-letter path) once the store recovers.
			return err
		}
		if seen {
			return nil
		}

		if err := handler(ctx, env); err != nil {
			return err
		}

		// Best-effort mark: the handler already succeeded, so we ack regardless. A
		// failed Remember leaves the id unrecorded (the documented at-least-once
		// dual-write window); a later redelivery may reprocess.
		_ = store.Remember(ctx, id)
		return nil
	}
}
