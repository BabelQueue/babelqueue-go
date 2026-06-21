# BabelQueue for Go

[![CI](https://github.com/BabelQueue/babelqueue-go/actions/workflows/ci.yml/badge.svg)](https://github.com/BabelQueue/babelqueue-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/babelqueue/babelqueue-go.svg)](https://pkg.go.dev/github.com/babelqueue/babelqueue-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/babelqueue/babelqueue-go)](https://goreportcard.com/report/github.com/babelqueue/babelqueue-go)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **Polyglot Queues, Simplified.** Read and write the canonical BabelQueue message
> envelope from Go — so your Go services exchange messages with Laravel, Symfony,
> Python, .NET and Node over one strict JSON format, on the broker you already run.

This is the framework-agnostic **Go core**: the wire-envelope codec, contracts
and dead-letter helpers — **zero dependencies** (standard library only). The full
standard is documented at **[babelqueue.com](https://babelqueue.com)**.

## Installation

```bash
go get github.com/babelqueue/babelqueue-go
```

Requires Go `>=1.21`.

## Usage

```go
package main

import (
	"fmt"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

func main() {
	// Produce — build the canonical envelope and publish the JSON to your broker.
	env, err := babelqueue.Make(
		"urn:babel:orders:created",
		map[string]any{"order_id": 1042},
		babelqueue.WithQueue("orders"),
	)
	if err != nil {
		panic(err)
	}
	body, _ := env.Encode() // []byte of compact UTF-8 JSON
	// redisClient.RPush(ctx, "queues:orders", body)
	//   /  ch.PublishWithContext(ctx, "", "orders", false, false, amqp.Publishing{Body: body})

	// Consume — decode a message produced by ANY BabelQueue SDK.
	in, err := babelqueue.Decode(body)
	if err != nil || !in.Accepts() {
		return // malformed or unsupported — dead-letter / drop
	}
	switch in.URN() {
	case "urn:babel:orders:created":
		fmt.Println(in.Data["order_id"], in.TraceID) // 1042, the cross-service trace id
	}
}
```

The envelope is identical to every other SDK's:

```json
{
  "job": "urn:babel:orders:created",
  "trace_id": "…",
  "data": { "order_id": 1042 },
  "meta": { "id": "…", "queue": "orders", "lang": "go", "schema_version": 1, "created_at": 1749132727000 },
  "attempts": 0
}
```

> `Encode` disables HTML escaping and emits compact JSON — the same canonical wire
> form as the PHP and Python cores (the envelope frame is byte-identical). Key
> order inside your `data` map follows `encoding/json` (sorted), where PHP/Python
> keep insertion order — JSON object key order is insignificant, so consumers read
> them identically. JSON numbers decode into `Data` as `float64`
> (encoding/json's default for `any`).

### Trace continuation

`Make` mints a fresh `trace_id` unless you pass one — propagate an inbound trace
across a hop with `WithTraceID`:

```go
next, _ := babelqueue.Make(
	"urn:babel:shipping:requested",
	map[string]any{"order_id": 1042},
	babelqueue.WithTraceID(in.TraceID),
)
```

### Dead-letter

```go
dl := babelqueue.Annotate(env, "failed", "orders", 3, err) // additive dead_letter block
body, _ := dl.Encode()
// publish body to the "orders.dlq" queue
```

`Annotate` returns a copy — the original envelope is preserved unchanged inside
the dead-lettered message, so any-language consumers can still read it.

## Runtime — produce & consume

The core is just the codec. An **optional, still-zero-dependency** runtime (`App`)
ties it to a broker through a small `Transport` interface, with URN routing,
attempts-based retry → dead-letter, and unknown-URN strategies:

```go
import babelqueue "github.com/babelqueue/babelqueue-go"

app := babelqueue.NewApp(transport, // a Transport (see below)
    babelqueue.WithDefaultQueue("orders"),
    babelqueue.WithMaxAttempts(3),
    babelqueue.WithDeadLetter(true),
)

app.Handle("urn:babel:orders:created", func(ctx context.Context, env babelqueue.Envelope) error {
    // ... handle env.Data; return an error to retry / dead-letter
    return nil
})

app.Publish(ctx, "urn:babel:orders:created", map[string]any{"order_id": 1042})
app.Consume(ctx) // blocks; routes by URN until ctx is cancelled
```

`InMemoryTransport` (in the core) backs tests and local runs with zero deps. Broker
drivers live in **separate modules**, so the core itself never pulls a dependency:

```bash
go get github.com/babelqueue/babelqueue-go/redis   # Redis  (go-redis)
go get github.com/babelqueue/babelqueue-go/amqp    # RabbitMQ (amqp091-go)
```

```go
import (
    babelqueue "github.com/babelqueue/babelqueue-go"
    "github.com/babelqueue/babelqueue-go/redis"
)

tr, _ := redis.New("redis://localhost:6379/0")        // reliable BLMOVE + processing list
app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
```

The RabbitMQ transport (`amqp.New("amqp://…")`) publishes to a durable queue with
persistent delivery and the contract AMQP properties (`type`=URN,
`correlation_id`=trace_id, `x-schema-version`/`x-source-lang`/`x-attempts`), and
consumes with `basic.get` + manual ack. Implement `babelqueue.Transport` yourself
to back the runtime with any other broker.

## Idempotent consumption (dedupe on `meta.id`)

BabelQueue is **at-least-once**, so handlers should dedupe on the envelope's `meta.id`
([ADR-0022](https://babelqueue.com)). The `idempotency` subpackage (in the **core**,
zero-dep) makes that a one-line wrap: a message whose id was already processed is
skipped instead of re-run.

```go
import "github.com/babelqueue/babelqueue-go/idempotency"

store := idempotency.NewInMemoryStore() // process-local reference store
app.Handle("urn:babel:orders:created", idempotency.Wrap(store, func(ctx context.Context, env babelqueue.Envelope) error {
    // runs at most once per meta.id; a thrown error leaves the id unmarked so retry/DLQ still apply
    return nil
}))
```

`InMemoryStore` is for tests / single-process consumers. A **fleet** of consumers
needs a *shared, persistent* `Store` — two backends ship as **separate submodules**
(so the core stays zero-dependency; a driver is pulled in only if you import them):

```bash
go get github.com/babelqueue/babelqueue-go/idempotency-redis      # Redis    (go-redis)
go get github.com/babelqueue/babelqueue-go/idempotency-postgres   # Postgres (database/sql + pgx)
```

```go
import (
    "github.com/babelqueue/babelqueue-go/idempotency"
    redisstore "github.com/babelqueue/babelqueue-go/idempotency-redis"
)

store, _ := redisstore.New("redis://localhost:6379/0", redisstore.WithTTL(24*time.Hour))
defer store.Close()
app.Handle("urn:babel:orders:created", idempotency.Wrap(store, handler))
```

Both persistent backends implement the **same** `idempotency.Store` interface as the
in-memory reference, with an **atomic claim** so concurrent consumers serialize on
`meta.id` — Redis via `SET key val NX PX <ttl>`, Postgres via `INSERT … ON CONFLICT …
RETURNING` on the primary key (DDL via `schema.sql` / `Store.Migrate`). A duplicate
delivery is detected as already-seen; an optional `WithTTL` expires ids so they may be
re-processed once the window lapses. See each submodule's README for details.

## Transactional outbox (atomic write + relayed publish)

A plain producer makes a **dual write** — commit the business row **and** publish to the
broker — two independent systems that disagree on a crash. The `outbox` subpackage (in
the **core**, zero-dep) removes it ([ADR-0029](https://babelqueue.com)): the message is
persisted into the **same database, in the same transaction**, as the business data, so
it commits or rolls back atomically with it; a separate **relay** publishes the durable
rows afterwards. It is the producer-side counterpart to consumer-side idempotency above.

**The caller owns the transaction boundary** — `Outbox.Write` does not begin or commit
anything; it just encodes the envelope (frozen codec, bytes unchanged) and hands it to a
`Store` the caller has bound to their open transaction:

```go
import "github.com/babelqueue/babelqueue-go/outbox"

ob := outbox.New(store) // store is YOUR DB-bound outbox.Store (tx-scoped)

// inside the transaction you already opened around the business write:
insertOrder(ctx, tx, order)                  // the business write
env, _ := babelqueue.Make("urn:babel:orders:created", data, babelqueue.WithQueue("orders"))
ob.Write(env)                                // same tx, via a tx-bound Store
tx.Commit()                                  // both, or neither
```

The `outbox.Store` interface is the only thing you implement — the core pulls in **no**
DB driver (GR-7). It stores the `Encode`d envelope **verbatim** and the relay publishes
those exact bytes, so `trace_id` is preserved end-to-end (GR-4) and every SDK stays
byte-compatible (GR-5):

```go
type Store interface {
    Save(encoded []byte, queue string) (id string, err error)
    FetchUnpublished(limit int) ([]outbox.Record, error) // SHOULD claim rows (FOR UPDATE SKIP LOCKED)
    MarkPublished(ids []string) error
    MarkFailed(id, reason string) error
}
```

A separate **relay** drains the committed rows through the same `Transport` seam, on a
worker loop or a cron:

```go
relay := outbox.NewRelay(transport, store, outbox.Options{}) // zero Options = sane defaults

relay.Flush(ctx)      // publish one batch; mark each row published AFTER publish returns
relay.Drain(ctx, 0)   // loop Flush until a pass makes no progress (safety ceiling)
```

A row is marked published **only after** the transport accepts it — a crash in between
re-publishes it next pass (at-least-once; the consumer dedupes on `meta.id`). A publish
error is caught, the row is marked failed (with a bounded linear backoff) and left
pending, so **one poison row never blocks the batch**. `outbox.InMemoryStore` is a
process-local reference for tests / single-process demos (no real transaction — use a
DB-backed adapter in production).

## GDPR field encryption (encrypt PII inside `data`)

A registry can declare which `data` fields carry personal data with the
`x-gdpr-sensitive` schema keyword, and `bqschema gdpr` audits/masks them
([ADR-0030](https://babelqueue.com)) — but that is **governance**. The `gdpr`
subpackage (in the **core**, zero-dep) is the **runtime** half: a producer
encrypts each marked field before publish, a consumer decrypts it after decode,
so PII never sits in cleartext on the broker.

The envelope stays **frozen** (GR-1): `Protect` rewrites only the *values* inside
`data` — a sensitive field's value becomes a ciphertext **string** — it never
adds, renames or retypes an envelope field, `meta.schema_version` stays `1`, and
`trace_id` is untouched (GR-4). `data` remains pure JSON (GR-3), so any SDK can
carry the envelope even without the key (it just can't read the protected fields).

The crypto is a **`Cipher` interface the caller provides** (KMS / Vault / HSM /
tokenisation) — keeping the core dependency-free (GR-7). A stdlib-only
`AESGCMCipher` (AES-256-GCM, random nonce, base64) ships as the reference:

```go
import (
    babelqueue "github.com/babelqueue/babelqueue-go"
    "github.com/babelqueue/babelqueue-go/gdpr"
    "github.com/babelqueue/babelqueue-go/schema"
)

cipher, _ := gdpr.NewAESGCMCipher(key32) // your 32-byte key — or bind a KMS-backed Cipher
provider, _ := schema.NewDirProvider("registry.json")

// Producer — encrypt the marked fields after building data, before publishing.
env, _ := babelqueue.Make("urn:babel:users:registered", data, babelqueue.WithQueue("users"))
if sch, ok, _ := provider.Schema(env.URN()); ok {
    _ = schema.Check(provider, env.URN(), env.Data) // validate CLEARTEXT first (see note)
    _ = gdpr.Protect(env.Data, sch, cipher)         // email, profile.full_name, addresses[].line → ciphertext
}
body, _ := env.Encode()                             // ciphertext rides inside data; frame unchanged

// Consumer — decrypt after Decode, before the handler reads data.
in, _ := babelqueue.Decode(body)
if sch, ok, _ := provider.Schema(in.URN()); ok {
    _ = gdpr.Unprotect(in.Data, sch, cipher)        // restores the original values byte-for-byte
}
```

`Protect`/`Unprotect` are **standalone helpers** — strictly opt-in, no behaviour
change when unused. The sensitive paths come from the **same per-URN schema** the
validation path already loads, including **nested objects** (`profile.full_name`)
and **array items** (`addresses[].line`); an absent field is skipped, not an error.

> **Validate cleartext, not ciphertext.** A schema that constrains a sensitive
> field (`minLength`, `enum`, `type:"integer"`, …) would reject the ciphertext
> string. Run `schema.Check` **before** `Protect` on the producer and **after**
> `Unprotect` on the consumer — or register `schema.Wrap` so it runs after your
> `Unprotect` step. A wrong-key `Unprotect` returns `gdpr.ErrDecrypt`, so the
> message takes the retry / dead-letter path rather than being handled blind.

This is the **reference implementation** the other SDKs mirror: the same
`Cipher` seam, the same encrypt-values-in-place contract, the same frozen
envelope.

## What this core is

It enforces the **contract**: the envelope shape, URN identity, trace propagation,
schema-version gating and the dead-letter block — with **zero dependencies**. The
optional `App` runtime and `InMemoryTransport` are zero-dep too; only the Redis and
RabbitMQ transport modules pull a broker driver, and only if you import them.

Unknown-URN strategy constants (`StrategyFail`, `StrategyDelete`,
`StrategyRelease`, `StrategyDeadLetter`) drive the runtime's unknown-URN handling.

## Conformance

This core passes the shared **cross-SDK conformance suite** (vendored under
[`testdata/conformance/`](testdata/conformance)) — the same fixtures every
BabelQueue SDK must satisfy, so a Go producer and, say, a Laravel consumer agree
byte-for-byte.

```bash
go test ./...
```

## License

[MIT](LICENSE) © Muhammet Şafak
