# babelqueue-go/asynq

[![Go Reference](https://pkg.go.dev/badge/github.com/babelqueue/babelqueue-go/asynq.svg)](https://pkg.go.dev/github.com/babelqueue/babelqueue-go/asynq)

[asynq](https://github.com/hibiken/asynq) adapter for the
[BabelQueue Go runtime](https://github.com/babelqueue/babelqueue-go) —
"Polyglot Queues, Simplified."

asynq is the popular Redis-backed Go task queue. This adapter makes an asynq app
produce and consume the **canonical BabelQueue envelope**, so it interoperates
byte-for-byte with the PHP/Laravel, Python, … SDKs over Redis. asynq is not a new
broker binding (its storage is Redis, [§1](https://babelqueue.com)) — it is a
framework adapter, mirroring how Node's BullMQ adapter wraps the same core.

The mapping is direct and lossless:

| BabelQueue | asynq |
| :--- | :--- |
| canonical envelope JSON | task `Payload` (byte-identical — no wrapping, no added fields) |
| `job` (the URN) | task `TypeName` (asynq routes on it, like every BabelQueue consumer) |

## Install

```bash
go get github.com/babelqueue/babelqueue-go/asynq
```

Requires Go 1.24+ (asynq's floor).

## Produce

```go
import (
    babelqueue "github.com/babelqueue/babelqueue-go"
    bqasynq "github.com/babelqueue/babelqueue-go/asynq"
    "github.com/hibiken/asynq"
)

client := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})
defer client.Close()

p := bqasynq.NewProducer(client)

// Envelope options (WithQueue, WithTraceID) and asynq task options (MaxRetry,
// ProcessIn, …) are mixed freely — they're distinguished by type.
p.Dispatch(ctx, "urn:babel:orders:created", map[string]any{"order_id": 1042},
    babelqueue.WithQueue("orders"), // recorded in meta.queue
    asynq.MaxRetry(5),              // forwarded onto the asynq task
)
```

`meta.queue` records the **logical** BabelQueue queue; asynq's own queue is set via
`asynq.Queue(...)` (default `"default"`) and need not equal it.

## Consume — route by URN

```go
mux := asynq.NewServeMux()

bqasynq.Register(mux, "urn:babel:orders:created",
    func(ctx context.Context, env babelqueue.Envelope) error {
        // env.Data, env.TraceID … (trace_id preserved across the hop)
        return nil
    })

srv := asynq.NewServer(asynq.RedisClientOpt{Addr: "localhost:6379"}, asynq.Config{})
srv.Run(mux)
```

`Register` decodes the task payload into an envelope, rejects one that fails
consumer-side validation (`ErrNotAccepted`, so asynq applies its own retry/archive
policy), and routes by URN — the asynq-native equivalent of BabelQueue's URN
routing. For finer control, `Bind(handler)` returns a plain `asynq.HandlerFunc` you
can wire yourself, and `Task`/`Envelope` convert between an envelope and an
`*asynq.Task` directly.

## How it maps to the contract

The envelope is the byte-identical payload (Redis [§1](https://babelqueue.com)
payload identity); the URN is the asynq `TypeName`. The envelope is unchanged
(`schema_version` stays `1`); asynq is purely additive.

## Test

```bash
go test ./...   # unit + conformance (fake Enqueuer seam — no Redis)
```

The asynq client is reached through a small `Enqueuer` seam, so the suite runs
network-free.

## License

MIT
