# BabelQueue idempotency store — Redis (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/babelqueue/babelqueue-go/idempotency-redis.svg)](https://pkg.go.dev/github.com/babelqueue/babelqueue-go/idempotency-redis)

A **Redis-backed** [`idempotency.Store`](https://pkg.go.dev/github.com/babelqueue/babelqueue-go/idempotency)
for the [BabelQueue Go runtime](https://github.com/BabelQueue/babelqueue-go): a shared,
persistent record of processed message ids so a **fleet** of consumers dedupes on the
envelope's `meta.id` ([ADR-0022](https://babelqueue.com)) — instead of every worker keeping
its own in-memory set.

The framework-agnostic core stays **zero-dependency** (GR-7): the dep-free `Store` interface
(and the reference `InMemoryStore`) live in the core; this module is a **separate submodule**
with its own `go.mod`, the only place a Redis driver is pulled in. It reuses the same
[`go-redis`](https://github.com/redis/go-redis) client as the `…/redis` *transport* submodule,
so a consumer importing both shares one driver version.

```bash
go get github.com/babelqueue/babelqueue-go/idempotency-redis
```

```go
import (
    babelqueue "github.com/babelqueue/babelqueue-go"
    "github.com/babelqueue/babelqueue-go/idempotency"
    redis "github.com/babelqueue/babelqueue-go/idempotency-redis"
)

store, err := redis.New("redis://localhost:6379/0", redis.WithTTL(24*time.Hour))
if err != nil { panic(err) }
defer store.Close()

app.Handle("urn:babel:orders:created", idempotency.Wrap(store, handler))
```

Bring your own client with `redis.NewWithClient(client)` (custom pool / TLS / auth).
`WithPrefix("…")` isolates keys per app on a shared Redis (default `bq:idemp:`); `WithTTL(d)`
expires ids after `d` so they may be re-processed once the window lapses (default: keys never
expire — evict with `Forget`).

## Atomic claim (parking)

The race-safety comes from `SET key value NX [PX ttl]`:

```
SET bq:idemp:<message_id> 1 NX PX <ttl-ms>
```

Redis applies `NX` ("set only if Not eXists") on its single command thread, so under any number
of concurrent consumers exactly **one** `SET` creates the key (reply `OK` → that caller **won**
the claim); every duplicate sees the key present (reply `nil` → a **duplicate / parked**
delivery). The TTL rides the same atomic command (`PX`), so the key appears and gets its expiry
in one round trip, and Redis expires it natively — after which `Seen` is false and the id may be
re-claimed. This is the same "exactly one wins, the rest park" contract the in-memory store
models — now durable across processes.

`Claim(ctx, id) (bool, error)` exposes that race result directly (`true` = first delivery,
`false` = duplicate). `Remember` (the `idempotency.Store` write side) delegates to `Claim` and
discards the bool, so `idempotency.Wrap` accepts this store unchanged. `Seen` is an `EXISTS`
check; `Forget` is a `DEL`.

## Tests

Unit tests run with **no broker** (a fake go-redis client seam that models `SET NX` / `EXISTS` /
`DEL`), covering the claim / duplicate / concurrent-serialize / TTL / Seen / Forget logic.
Integration tests skip cleanly unless a live Redis is provided:

```bash
go test ./...                                          # unit only (no broker) — what CI runs on every push
BABELQUEUE_TEST_REDIS='redis://localhost:6379/0' \
  go test ./...                                        # + integration (concurrent claims serialize, duplicate rejected, TTL)
```

Full spec: **[babelqueue.com](https://babelqueue.com)** · [MIT](../LICENSE) © Muhammet Şafak
