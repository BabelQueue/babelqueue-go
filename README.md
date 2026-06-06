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
