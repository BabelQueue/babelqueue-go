# BabelQueue Redis transport (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/babelqueue/babelqueue-go/redis.svg)](https://pkg.go.dev/github.com/babelqueue/babelqueue-go/redis)

A Redis-backed `babelqueue.Transport` for the [BabelQueue Go runtime](https://github.com/BabelQueue/babelqueue-go),
using the reliable-queue pattern (`RPUSH` to produce, `BLMOVE` head → per-queue
processing list to reserve, `LREM` to ack). Built on
[`go-redis`](https://github.com/redis/go-redis).

```bash
go get github.com/babelqueue/babelqueue-go/redis
```

```go
import (
    babelqueue "github.com/babelqueue/babelqueue-go"
    "github.com/babelqueue/babelqueue-go/redis"
)

tr, err := redis.New("redis://localhost:6379/0")
if err != nil { panic(err) }
defer tr.Close()

app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
app.Handle("urn:babel:orders:created", func(ctx context.Context, env babelqueue.Envelope) error {
    // handle env.Data ...
    return nil
})
app.Publish(ctx, "urn:babel:orders:created", map[string]any{"order_id": 1042})
app.Consume(ctx)
```

Bring your own client with `redis.NewWithClient(client)` (custom pool / TLS / auth).
The BabelQueue core stays zero-dependency; this module is the only place `go-redis`
is pulled in.

## Laravel-compatible layout, delays and recovery (ADR-0033)

`redis.WithLaravelLayout(prefix)` puts the transport on the shared Laravel keys (`<q>` ready list,
`<q>:delayed` / `<q>:reserved` sorted sets, `<q>:notify` wake-up list). It is opt-in; the default
layout stays the Go-owned reliable queue below.

- **Reserve** is Laravel's pop script: the handler receives the raw head byte for byte and the attempt
  count it sees is `job.attempts+1`. Ack is `ZREM` of the `:reserved` member.
- **Compatibility exception (ADR-0033, to ADR-0031):** the member Laravel's pop script writes to
  `:reserved` is re-encoded by `cjson`, so it is not the stored bytes. That is Laravel's behaviour and the
  only place a Redis value is not preserved byte for byte. Release and delayed publish use the SDK codec,
  never `cjson`, and keep unknown envelope keys with their values and types.
- **`PublishDelayed(ctx, queue, env, delay)`** is one `ZADD <q>:delayed floor(now)+ceil(delay) payload`
  (no notify token). **`Release(ctx, msg, delay)`** is one script: `ZREM :reserved` plus `ZADD :delayed`
  with `attempts+1`. There is no push-and-ack fallback.
- **Undecodable bodies (poison, binding section 1.1):** a head the reserve script flags (`{job, job}`
  answer) and a body the core codec rejects (string `meta`/`data`, numeric `trace_id`, fractional
  `attempts`) are flagged `Poison`, and so is an **identity-less body** (neither a valid `job` nor a valid
  `urn`; ADR-0036 decision 4, binding section 1.1 "Identity-less body"): it is no unknown URN, so
  `on_unknown_urn: delete` and a disabled dead-letter have no effect on it. The `App` writes the raw body,
  untouched, to `<q>.dlq` (ADR-0035 record, `reason: "poison"`) through the reservation-gated `DeadLetter`,
  on the first delivery and in every configuration (also with dead-lettering off, any retry strategy).
  `Release` returns `ErrReleaseUnsupported` for such a body and never re-encodes it, so it is never lost and
  never loops.
- **Migration** runs before every pop: due `:delayed` members first, then expired `:reserved` members
  (score <= `floor(now)`), `WithMigrationBatchSize(n)` bounds one migrate (default: unbounded). An expired
  `:reserved` member goes back to `<q>` and counts as an attempt.
- **`retry_after`** defaults to 90 s (`WithRetryAfter(d)`); `WithoutRetryAfter()` is `retry_after: null`
  (this consumer never recovers expired reservations). A negative value is a configuration error.
- Every blocking wait is sliced to at most 1 s with a migrate before each slice, so a delayed message is
  picked up within about a second of becoming due.
- **No `traceparent` in this mode (A3).** The `__bq_frame` below is not written: `PublishWithHeaders`
  drops the headers and stores the bare envelope, because Laravel workers cannot unwrap a frame.
  `trace_id` stays in the envelope; the W3C `traceparent` header is not carried across a Laravel-layout
  queue.

## Legacy mode: `:processing` list with deadlines (ADR-0033 section 9)

The default (non-Laravel) layout keeps `<q>:processing` as its in-flight list and adds a deadlines sorted
set `<q>:processing:deadlines` whose members are `<32 lowercase hex token>:<raw envelope>` scored
`floor(now)+retry_after` (`+inf` for `retry_after: null`). The token comes from `crypto/rand` only.

- Reserve is `LMOVE <q> <q>:processing LEFT RIGHT` plus the `ZADD`, in one script.
- Ack, release and dead-letter are gated on `ZREM` of the deadline member: when it returns 0 the
  reservation had already been swept, nothing else is touched and a "reservation expired" warning is
  logged, so a late ack can never remove another worker's identical copy.
- A sweep script runs before every reserve (also for a `retry_after: null` consumer): an expired item is
  `LREM`'d from `:processing` (count 1) and pushed back to `<q>` unchanged. Recovery keeps the bytes and
  does not increment attempts. Malformed members and orphans are only removed from the deadlines set.
- When the queue is empty the reserve sleep-polls with a 100 ms backoff doubling up to 1 s, reset when a
  message arrives and never beyond the pop timeout.
- **Undecodable bodies (poison, binding section 1.1):** a body the core codec rejects (not JSON, wrong
  field types) is flagged `Poison` on delivery. The `App` writes the raw body to `<q>.dlq` through the
  deadline-gated `DeadLetter`, on the first delivery and in every configuration (also with dead-lettering
  off, any retry strategy), so it is never lost and never redelivered. An identity-less body (neither a valid
  `job` nor a valid `urn`) is poison too (ADR-0036 decision 4), also under `on_unknown_urn: delete`.
  `Release` returns `ErrReleaseUnsupported` for an undecodable body (this layout has
  no delivery counter, so a verbatim re-queue would loop forever); the `App` does not reach that branch for
  a flagged body.

## Out-of-band headers (`traceparent`, ADR-0028)

Default layout only: the Laravel-compatible layout writes the bare envelope and drops headers (A3).

The transport implements the optional `babelqueue.HeaderPublisher` capability, so
out-of-band transport headers — e.g. a W3C `traceparent` for cross-hop span linkage —
ride **beside** the frozen wire envelope, never inside it. Redis stores only the raw
list value (the `LREM` ack handle *is* that value), so there is no native per-message
metadata channel; the transport instead owns a small JSON **frame**, distinct from the
wire envelope:

```json
{"__bq_frame":1,"headers":{"traceparent":"00-…-01"},"body":"<raw wire envelope>"}
```

- `Publish` stores the **bare** envelope byte-for-byte (no frame).
- `PublishWithHeaders` writes a **frame** only when the header map is non-empty;
  with no usable headers it degrades to a byte-identical bare `Publish`.
- `Pop` detects frame-vs-bare by the reserved `__bq_frame` sentinel (the frozen
  envelope never carries it). A frame is unwrapped — `Body` is the raw envelope,
  `Headers` carries the frame's headers; the `Handle` stays the **stored frame**, so
  the reliable-queue `RPUSH`/`BLMOVE`/`LREM` semantics are untouched. A bare value
  (an older or non-otel publisher, or a cross-version queue) consumes exactly as before:
  `Headers` is nil and the handle is the bare value.

Full spec: **[babelqueue.com](https://babelqueue.com)** · [MIT](../LICENSE) © Muhammet Şafak
