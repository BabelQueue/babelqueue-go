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

## Out-of-band headers (`traceparent`, ADR-0028)

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
