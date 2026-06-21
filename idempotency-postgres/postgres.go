// Package postgres is a PostgreSQL-backed [idempotency.Store] for the BabelQueue
// Go runtime: a shared, persistent record of processed message ids so a fleet of
// consumers dedupes on the envelope's meta.id (ADR-0022) instead of every worker
// keeping its own in-memory set.
//
//	store, _ := postgres.New(ctx, "postgres://user:pass@localhost:5432/app?sslmode=disable")
//	defer store.Close()
//	app.Handle(urn, idempotency.Wrap(store, handler))
//
// It satisfies the dependency-free [idempotency.Store] interface from the core
// (Seen / Remember / Forget) with the SAME contract the reference in-memory store
// guarantees, and adds atomic-claim semantics on top so concurrent consumers
// serialize:
//
//   - Remember is an ATOMIC claim — INSERT ... ON CONFLICT DO NOTHING. Exactly one
//     concurrent caller inserts the row (and is the "winner"); a duplicate delivery
//     hits the conflict and is a no-op. This is race-safe under any number of
//     concurrent workers and connections (the unique primary key is the lock).
//   - Claim exposes that race result directly: it returns true iff THIS caller won
//     the insert (first delivery), false if the row already existed (duplicate /
//     in-flight "parked" delivery). Remember delegates to Claim and discards the
//     bool, so the [idempotency.Store] contract is preserved exactly.
//   - Seen reports whether a live (non-expired) row exists.
//   - TTL/expiry is honored in SQL: an expired row is treated as absent by Seen and
//     re-claimable by Claim/Remember (an expired key may be re-processed once its
//     window lapses, the documented at-least-once behaviour).
//
// The GR-7 zero-dependency core stays dependency-free: this driver lives in its own
// submodule (its own go.mod) and is the only place a database driver is pulled in.
// It talks to Postgres through the standard library [database/sql] over the pgx
// stdlib driver, so any *sql.DB wired to a Postgres-compatible database also works
// via [NewWithDB].
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" database/sql driver

	"github.com/babelqueue/babelqueue-go/idempotency"
)

// DefaultTable is the table name used when [WithTable] is not given.
const DefaultTable = "babelqueue_idempotency"

// Store is a PostgreSQL-backed [idempotency.Store]. Construct it with [New]
// (owns the pool) or [NewWithDB] (bring your own *sql.DB).
type Store struct {
	db    *sql.DB
	table string
	ttl   time.Duration // 0 = ids never expire (rows kept until Forget / external GC)
	owned bool          // true when this Store opened db itself and Close should close it
}

// Option customizes a Store.
type Option func(*Store)

// WithTable overrides the table name (default [DefaultTable]). The name is
// validated as a plain SQL identifier — it is interpolated into DDL/DML, never a
// query parameter, so it must not come from untrusted input.
func WithTable(table string) Option {
	return func(s *Store) { s.table = table }
}

// WithTTL sets how long a remembered id stays "seen" before it expires and may be
// re-processed. Zero (the default) means ids never expire on their own — evict with
// Forget or an external job. A negative TTL is treated as zero.
func WithTTL(ttl time.Duration) Option {
	return func(s *Store) {
		if ttl < 0 {
			ttl = 0
		}
		s.ttl = ttl
	}
}

// New opens a pgx-backed connection pool to dsn (a postgres:// URL or a key=value
// DSN) and returns a Store. The caller owns the lifetime: call [Store.Close] to
// release the pool. It pings once so a bad DSN fails fast.
func New(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := NewWithDB(db, opts...)
	s.owned = true
	return s, nil
}

// NewWithDB wraps an existing *sql.DB (bring your own pool / driver / config). The
// caller keeps ownership of db; [Store.Close] is a no-op for a borrowed pool.
func NewWithDB(db *sql.DB, opts ...Option) *Store {
	s := &Store{db: db, table: DefaultTable}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Migrate creates the idempotency table if it does not exist. It is idempotent and
// safe to run on every boot; production deployments may instead apply the shipped
// DDL (see schema.sql) through their own migration tool.
func (s *Store) Migrate(ctx context.Context) error {
	if err := validateIdentifier(s.table); err != nil {
		return err
	}
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	message_id  TEXT        NOT NULL PRIMARY KEY,
	created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	expires_at  TIMESTAMPTZ NULL
)`, s.table)
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return err
	}
	idx := fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at)`,
		s.table, s.table,
	)
	_, err := s.db.ExecContext(ctx, idx)
	return err
}

// Claim atomically records messageID and reports whether THIS call won the race —
// true on the first delivery (the row was inserted), false when the id was already
// claimed by a live (non-expired) row (a duplicate / in-flight "parked" delivery).
//
// The race-safety comes from INSERT ... ON CONFLICT DO NOTHING on the primary key:
// under any number of concurrent consumers exactly one INSERT inserts a row and
// reports a claim; the rest hit the conflict and report a duplicate. An expired row
// is reclaimed in the same statement (DO UPDATE when the existing row has lapsed),
// so a claim succeeds again once the TTL window passes.
//
// This is the atomic primitive [Remember] is built on; the [idempotency.Store]
// interface does not expose it, so it is available only on a *Store.
func (s *Store) Claim(ctx context.Context, messageID string) (bool, error) {
	if err := validateIdentifier(s.table); err != nil {
		return false, err
	}
	// ON CONFLICT DO UPDATE ... WHERE the stored row has already expired lets a lapsed
	// id be re-claimed atomically; the WHERE makes a still-live conflict a no-op
	// (no row updated). RETURNING fires only for the row actually written, so a live
	// duplicate returns zero rows -> not claimed.
	var expires any // NULL when TTL is disabled
	if s.ttl > 0 {
		expires = time.Now().Add(s.ttl).UTC()
	}
	query := fmt.Sprintf(`
INSERT INTO %s (message_id, created_at, expires_at)
VALUES ($1, now(), $2)
ON CONFLICT (message_id) DO UPDATE
	SET created_at = now(),
	    expires_at = EXCLUDED.expires_at
	WHERE %s.expires_at IS NOT NULL AND %s.expires_at <= now()
RETURNING message_id`, s.table, s.table, s.table)

	var got string
	err := s.db.QueryRowContext(ctx, query, messageID, expires).Scan(&got)
	switch {
	case err == nil:
		return true, nil // a row was written -> this caller claimed it
	case err == sql.ErrNoRows:
		return false, nil // live row already present -> duplicate / parked
	default:
		return false, err
	}
}

// Remember records messageID as processed (atomic claim; the [idempotency.Store]
// write side). It delegates to [Store.Claim] and discards whether this caller won
// the race — the seen-set contract only needs the id to be durable afterwards.
func (s *Store) Remember(ctx context.Context, messageID string) error {
	_, err := s.Claim(ctx, messageID)
	return err
}

// Seen reports whether messageID has a live (non-expired) row — the [idempotency.Store]
// read side. An expired row is treated as absent (returns false), so the id may be
// re-processed once its TTL lapses.
func (s *Store) Seen(ctx context.Context, messageID string) (bool, error) {
	if err := validateIdentifier(s.table); err != nil {
		return false, err
	}
	query := fmt.Sprintf(
		`SELECT 1 FROM %s WHERE message_id = $1 AND (expires_at IS NULL OR expires_at > now())`,
		s.table,
	)
	var one int
	err := s.db.QueryRowContext(ctx, query, messageID).Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case err == sql.ErrNoRows:
		return false, nil
	default:
		return false, err
	}
}

// Forget drops messageID from the store (manual eviction; the [idempotency.Store]
// contract). Removing an absent id is a no-op.
func (s *Store) Forget(ctx context.Context, messageID string) error {
	if err := validateIdentifier(s.table); err != nil {
		return err
	}
	query := fmt.Sprintf(`DELETE FROM %s WHERE message_id = $1`, s.table)
	_, err := s.db.ExecContext(ctx, query, messageID)
	return err
}

// Close releases the connection pool when this Store opened it ([New]). For a
// borrowed pool ([NewWithDB]) it is a no-op, leaving the caller's *sql.DB intact.
func (s *Store) Close() error {
	if s.owned && s.db != nil {
		return s.db.Close()
	}
	return nil
}

// validateIdentifier guards the table name interpolated into SQL. The name is a
// configuration value (never request input), but validating it keeps the
// interpolation safe-by-construction: only ASCII letters, digits and underscores,
// not starting with a digit, bounded length.
func validateIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("postgres: empty table name")
	}
	if len(name) > 63 {
		return fmt.Errorf("postgres: table name %q too long (max 63)", name)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return fmt.Errorf("postgres: invalid table name %q (allowed: [A-Za-z_][A-Za-z0-9_]*)", name)
		}
	}
	return nil
}

// Compile-time proof the persistent store satisfies the core's dependency-free
// Store interface, so idempotency.Wrap accepts it unchanged.
var _ idempotency.Store = (*Store)(nil)
