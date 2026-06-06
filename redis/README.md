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

Full spec: **[babelqueue.com](https://babelqueue.com)** · [MIT](../LICENSE) © Muhammet Şafak
