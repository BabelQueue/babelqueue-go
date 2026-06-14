# babelqueue-go/machinery

[![Go Reference](https://pkg.go.dev/badge/github.com/babelqueue/babelqueue-go/machinery.svg)](https://pkg.go.dev/github.com/babelqueue/babelqueue-go/machinery)

[machinery](https://github.com/RichardKnop/machinery) adapter for the
[BabelQueue Go runtime](https://github.com/babelqueue/babelqueue-go) —
"Polyglot Queues, Simplified."

machinery is a popular Redis/AMQP-backed Go task queue. This adapter makes a
machinery app produce and consume the **canonical BabelQueue envelope**, so it
interoperates byte-for-byte with the PHP/Laravel, Python, … SDKs. machinery is not
a new broker binding (its storage is Redis/AMQP, [§1/§2](https://babelqueue.com)) —
it is a framework adapter, the sibling of the `…/asynq` module.

machinery is **argument-based** (a task is a `tasks.Signature` with a `Name` and
typed `Args`), so the mapping is:

| BabelQueue | machinery |
| :--- | :--- |
| `job` (the URN) | task `Name` (machinery routes on it, like every BabelQueue consumer) |
| canonical envelope JSON | a single string `Arg` (byte-identical — no wrapping, no added fields) |

## Install

```bash
go get github.com/babelqueue/babelqueue-go/machinery
```

Requires Go 1.23+.

## Produce

```go
import (
    babelqueue "github.com/babelqueue/babelqueue-go"
    bqmachinery "github.com/babelqueue/babelqueue-go/machinery"
)

// server is your configured *machinery/v2.Server
p := bqmachinery.NewProducer(server)

p.Dispatch(ctx, "urn:babel:orders:created", map[string]any{"order_id": 1042},
    babelqueue.WithQueue("orders"), // recorded in meta.queue
)
```

`meta.queue` records the **logical** BabelQueue queue; machinery's own broker queue
/ routing key is set on the server (or on the signature) and need not equal it. For
machinery-native delivery options (`RetryCount`, `ETA` for delay, `RoutingKey`),
build the signature with `Signature`/`SignatureFor`, set the fields, then send it
with `server.SendTaskWithContext`.

## Consume — route by URN

```go
bqmachinery.Register(server, "urn:babel:orders:created",
    func(ctx context.Context, env babelqueue.Envelope) error {
        // env.Data, env.TraceID … (trace_id preserved across the hop)
        return nil
    })
```

`Register` registers the handler under the URN as the machinery task `Name`, so
machinery routes tasks whose name equals the URN to it. The bound task function
decodes the envelope argument, rejects one that fails consumer-side validation
(`ErrNotAccepted`, so machinery applies its own retry policy), and invokes the
handler — the machinery-native equivalent of BabelQueue's URN routing. For finer
control, `Bind(handler)` returns the plain `func(ctx, string) error` machinery task
func, and `Signature`/`Envelope` convert between an envelope and a
`*tasks.Signature` directly.

## How it maps to the contract

The envelope is the byte-identical envelope argument (Redis/AMQP
[§1/§2](https://babelqueue.com) payload identity); the URN is the machinery task
`Name`. The envelope is unchanged (`schema_version` stays `1`); machinery is purely
additive.

## Test

```bash
go test ./...   # unit + conformance (fake Sender/Registrar seams — no broker)
```

The machinery server is reached through small `Sender`/`Registrar` seams, so the
suite runs network-free.

## License

MIT
