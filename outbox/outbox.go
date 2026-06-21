// Package outbox adds an optional, dependency-free transactional outbox to a
// babelqueue producer (ADR-0029): the message is persisted into the same database,
// in the same transaction, as the business data — so it commits or rolls back
// atomically with it — and a separate [Relay] publishes the durable rows afterwards.
// It is the Go mirror of the PHP BabelQueue\Outbox helper, and the producer-side
// counterpart to the consumer-side github.com/babelqueue/babelqueue-go/idempotency.
//
// The pattern removes the producer dual write: a plain producer must both commit the
// business row (INSERT INTO orders …) and publish to the broker, two independent
// systems that disagree on a crash. The outbox makes the handoff to the broker a
// local problem (read a row, publish, mark it) instead of a cross-system one.
//
// The transaction boundary is the CALLER'S. [Outbox.Write] is invoked from inside a
// transaction the caller already opened around its own business write; the caller
// commits both together. The core never opens, commits or rolls back anything — that
// is the whole point — which keeps it free of any DB driver (GR-7): the core defines
// the [Store] contract; a concrete adapter (a database table) binds it to a real
// connection. The reference [InMemoryStore] is for tests / single-process demos.
//
// The stored value is the EnvelopeCodec-encoded wire envelope, byte-for-byte
// unchanged (GR-1): what the relay publishes is the same bytes that were stored, so
// trace_id is preserved end-to-end (GR-4) and cross-SDK parity holds (GR-5) — the
// relay never decodes, rebuilds or re-encodes the envelope. The outbox adds its own
// bookkeeping (id, status, attempts) around the envelope; it never adds a field to it.
//
// Like idempotency and schema, it lives in the core module (zero dependencies, stdlib
// only) so it ships with every producer.
//
// Usage — the caller owns the transaction boundary:
//
//	tx, _ := db.BeginTx(ctx, nil)
//	if err := insertOrder(ctx, tx, order); err != nil { // the business write
//		tx.Rollback()
//		return err
//	}
//	env, _ := babelqueue.Make("urn:babel:orders:created", data, babelqueue.WithQueue("orders"))
//	if _, err := ob.Write(env); err != nil { // same tx, via a tx-bound Store
//		tx.Rollback()
//		return err
//	}
//	return tx.Commit() // both, or neither
package outbox

import (
	"strconv"
	"sync"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// Store is the persistence seam for the transactional outbox (ADR-0029) — the durable
// "outbox" table an [Outbox] writer fills and a [Relay] drains. The core defines it
// and binds to NO database driver (GR-7); the caller binds a concrete adapter to their
// own connection, and the reference [InMemoryStore] backs tests.
//
// Save runs inside the transaction the caller has already opened around its business
// write, so the message commits atomically with the business data. The body it stores
// is the frozen wire envelope, byte-for-byte unchanged — implementations must not
// re-encode or mutate it.
type Store interface {
	// Save persists one encoded envelope into the outbox, within the transaction the
	// caller has already opened around its business write, and returns the new row's
	// outbox id (the store's own primary key, NOT meta.id). The body is stored verbatim.
	Save(encoded []byte, queue string) (id string, err error)

	// FetchUnpublished reserves up to limit rows that are pending publish, oldest
	// first, so a relay can forward them. Implementations SHOULD lock/claim the rows
	// they return (e.g. SELECT … FOR UPDATE SKIP LOCKED, or a picked_at claim) so two
	// concurrent relays do not both publish the same row; at-least-once tolerates a rare
	// double send. The slice is empty when the outbox is drained.
	FetchUnpublished(limit int) ([]Record, error)

	// MarkPublished marks the given rows as successfully published (so they are never
	// relayed again). The relay calls it only after the transport accepted the message.
	MarkPublished(ids []string) error

	// MarkFailed records a failed publish attempt for one row: it increments the row's
	// attempt counter and stores the last reason, leaving the row pending so a later
	// relay pass retries it (at-least-once). A store MAY park a row that exceeds a
	// max-attempts threshold, but that policy is the adapter's, not the core's.
	MarkFailed(id, reason string) error
}

// Record is one pending row read back from a [Store] for a [Relay] to publish. It
// pairs the store's bookkeeping (ID, Attempts) with the verbatim, frozen wire envelope
// (Body) and the queue it should go to. Body is the exact Encode output handed to
// [Store.Save] — the relay publishes these bytes unchanged (GR-1/GR-5), so trace_id is
// preserved end-to-end (GR-4) without the relay ever decoding the envelope.
type Record struct {
	ID       string // the outbox row id (the store's primary key, not meta.id)
	Body     string // the frozen, encoded envelope JSON, byte-for-byte as stored
	Queue    string // the logical queue the relay should publish to
	Attempts int    // how many times the relay has already tried to publish this row
}

// Outbox is the write side of the transactional outbox: it turns a babelqueue envelope
// into a stored outbox row, so the message is persisted atomically with the business
// data and a separate [Relay] publishes it later.
//
// It is intentionally tiny and dependency-free: [Outbox.Write] encodes via the frozen
// envelope codec (GR-1 — the bytes are stored unchanged; the outbox never adds an
// envelope field) and delegates persistence to the injected [Store], which the caller
// binds to their own DB (GR-7). It does NOT begin or commit anything — the caller owns
// the transaction boundary, which is the whole point of the pattern.
type Outbox struct {
	store Store
}

// New returns an Outbox that persists through store. The store is expected to be bound
// to the transaction the caller opens around each [Outbox.Write] (e.g. a tx-scoped
// adapter), so the message commits atomically with the business write.
func New(store Store) *Outbox {
	return &Outbox{store: store}
}

// Write encodes env via the frozen codec (bytes unchanged) and persists it through the
// store, inside the transaction the caller has already opened. It captures the target
// queue from env's meta.queue (falling back to "default") so the relay can publish to
// the right queue without decoding the body, and returns the new outbox row id (for the
// caller's own correlation, if wanted).
//
// It does not begin or commit a transaction. It returns the codec's error if the
// envelope is not cleanly encodable, or the store's error if persistence fails.
func (o *Outbox) Write(env babelqueue.Envelope) (id string, err error) {
	body, err := env.Encode()
	if err != nil {
		return "", err
	}
	return o.store.Save(body, queueOf(env))
}

// queueOf is the logical queue the message targets: its meta.queue, falling back to
// "default". Captured at write time so the relay never has to decode the body.
func queueOf(env babelqueue.Envelope) string {
	if env.Meta.Queue != "" {
		return env.Meta.Queue
	}
	return "default"
}

// InMemoryStore is a process-local, goroutine-safe reference [Store] backed by a map —
// for tests and single-process demos. It has NO real transaction: Save just appends, so
// it cannot deliver the atomic-with-the-business-write guarantee a production store
// gives; use a database-backed adapter in production.
//
// It still faithfully models the relay contract: rows are pending until MarkPublished,
// FetchUnpublished returns them oldest-first, and MarkFailed bumps the attempt count and
// stores the last reason while leaving the row pending for retry. It does not implement
// claim/locking (FetchUnpublished's documented adapter concern) — it is single-process.
type InMemoryStore struct {
	mu       sync.Mutex
	rows     []*memRow
	sequence int
}

type memRow struct {
	id        string
	body      string
	queue     string
	attempts  int
	published bool
	lastError string
}

// NewInMemoryStore returns an empty in-memory store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{}
}

// Save appends the encoded envelope as a new pending row and returns its id.
func (s *InMemoryStore) Save(encoded []byte, queue string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequence++
	id := "ob-" + strconv.Itoa(s.sequence)
	s.rows = append(s.rows, &memRow{id: id, body: string(encoded), queue: queue})
	return id, nil
}

// FetchUnpublished returns up to limit pending rows, oldest first. A non-positive limit
// returns no rows.
func (s *InMemoryStore) FetchUnpublished(limit int) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		return nil, nil
	}
	out := make([]Record, 0, limit)
	for _, r := range s.rows {
		if r.published {
			continue
		}
		out = append(out, Record{ID: r.id, Body: r.body, Queue: r.queue, Attempts: r.attempts})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// MarkPublished marks the given rows published; unknown ids are ignored.
func (s *InMemoryStore) MarkPublished(ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		if r := s.find(id); r != nil {
			r.published = true
		}
	}
	return nil
}

// MarkFailed bumps the attempt count and records the last reason for one row, leaving it
// pending; an unknown id is ignored.
func (s *InMemoryStore) MarkFailed(id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.find(id); r != nil {
		r.attempts++
		r.lastError = reason
	}
	return nil
}

// PendingCount reports how many rows are still pending publish (test/inspection helper).
func (s *InMemoryStore) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.rows {
		if !r.published {
			n++
		}
	}
	return n
}

// AttemptsOf reports the recorded attempt count for one row (0 if unknown) — a
// test/inspection helper.
func (s *InMemoryStore) AttemptsOf(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.find(id); r != nil {
		return r.attempts
	}
	return 0
}

// LastErrorOf reports the last recorded failure reason for one row ("" if none) — a
// test/inspection helper.
func (s *InMemoryStore) LastErrorOf(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.find(id); r != nil {
		return r.lastError
	}
	return ""
}

// find returns the row with id, or nil. Callers hold s.mu.
func (s *InMemoryStore) find(id string) *memRow {
	for _, r := range s.rows {
		if r.id == id {
			return r
		}
	}
	return nil
}
