# BabelQueue idempotency store — PostgreSQL (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/babelqueue/babelqueue-go/idempotency-postgres.svg)](https://pkg.go.dev/github.com/babelqueue/babelqueue-go/idempotency-postgres)

A **PostgreSQL-backed** [`idempotency.Store`](https://pkg.go.dev/github.com/babelqueue/babelqueue-go/idempotency)
for the [BabelQueue Go runtime](https://github.com/BabelQueue/babelqueue-go): a shared,
persistent record of processed message ids so a **fleet** of consumers dedupes on the
envelope's `meta.id` ([ADR-0022](https://babelqueue.com)) — instead of every worker keeping
its own in-memory set.

The framework-agnostic core stays **zero-dependency** (GR-7): the dep-free `Store` interface
(and the reference `InMemoryStore`) live in the core; this module is a **separate submodule**
with its own `go.mod`, the only place a database driver (`pgx`) is pulled in.

```bash
go get github.com/babelqueue/babelqueue-go/idempotency-postgres
```

```go
import (
    babelqueue "github.com/babelqueue/babelqueue-go"
    "github.com/babelqueue/babelqueue-go/idempotency"
    postgres "github.com/babelqueue/babelqueue-go/idempotency-postgres"
)

store, err := postgres.New(ctx, "postgres://user:pass@localhost:5432/app?sslmode=disable")
if err != nil { panic(err) }
defer store.Close()

// Create the table once (or apply schema.sql through your own migration tool).
_ = store.Migrate(ctx)

app.Handle("urn:babel:orders:created", idempotency.Wrap(store, handler))
```

Bring your own pool with `postgres.NewWithDB(db)` (any `*sql.DB` on a Postgres-compatible
driver). `WithTable("…")` isolates the table per app; `WithTTL(d)` expires ids after `d`
so they may be re-processed once the window lapses (default: ids never expire — evict with
`Forget` or an external GC job).

## Atomic claim (ACID / parking)

The race-safety comes from the table's **primary key** on `message_id`:

```sql
INSERT INTO babelqueue_idempotency (message_id, created_at, expires_at)
VALUES ($1, now(), $2)
ON CONFLICT (message_id) DO UPDATE
    SET created_at = now(), expires_at = EXCLUDED.expires_at
    WHERE babelqueue_idempotency.expires_at IS NOT NULL
      AND babelqueue_idempotency.expires_at <= now()
RETURNING message_id;
```

Under any number of concurrent consumers, exactly **one** `INSERT` writes the row (and
`RETURNING` yields it → that caller **won** the claim); every duplicate hits the conflict and,
because the live row has not expired, the `DO UPDATE … WHERE` matches nothing → `RETURNING`
yields zero rows → that caller is a **duplicate / parked** delivery. An **expired** row is
re-claimed atomically by the `DO UPDATE` branch, so an id becomes processable again once its
TTL lapses. This is the same "exactly one wins, the rest park" contract the in-memory store
models — now durable across processes.

`Claim(ctx, id) (bool, error)` exposes that race result directly (`true` = first delivery,
`false` = duplicate). `Remember` (the `idempotency.Store` write side) delegates to `Claim` and
discards the bool, so `idempotency.Wrap` accepts this store unchanged. `Seen` returns true only
for a **live** (non-expired) row.

## Schema

`store.Migrate(ctx)` creates the table on demand; [`schema.sql`](schema.sql) ships the same
DDL for migration tools. Periodically reclaim expired rows (optional — expired rows are already
ignored by `Seen`/`Claim`):

```sql
DELETE FROM babelqueue_idempotency WHERE expires_at IS NOT NULL AND expires_at <= now();
```

## Tests

Unit tests run with **no database** (`go-sqlmock` over `database/sql`), covering the
claim / duplicate / expiry / Seen / Forget / migrate logic. Integration tests skip cleanly
unless a live Postgres is provided:

```bash
go test ./...                                   # unit only (no DB) — what CI runs on every push
BABELQUEUE_TEST_PG='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
  go test ./...                                 # + integration (concurrent claims serialize, duplicate rejected, TTL)
```

Full spec: **[babelqueue.com](https://babelqueue.com)** · [MIT](../LICENSE) © Muhammet Şafak
