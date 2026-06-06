# BabelQueue RabbitMQ transport (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/babelqueue/babelqueue-go/amqp.svg)](https://pkg.go.dev/github.com/babelqueue/babelqueue-go/amqp)

A RabbitMQ (AMQP 0-9-1) `babelqueue.Transport` for the [BabelQueue Go runtime](https://github.com/BabelQueue/babelqueue-go).
Producing publishes to a **durable** queue with **persistent** delivery and the
contract AMQP properties (`type` = URN, `correlation_id` = trace_id, `message_id`
= meta.id, and `x-schema-version` / `x-source-lang` / `x-attempts` headers) — so a
PHP/Python/… consumer can route on `properties.type` without parsing the body.
Consuming uses `basic.get` + manual ack (at-least-once). Built on
[`amqp091-go`](https://github.com/rabbitmq/amqp091-go).

```bash
go get github.com/babelqueue/babelqueue-go/amqp
```

```go
import (
    babelqueue "github.com/babelqueue/babelqueue-go"
    "github.com/babelqueue/babelqueue-go/amqp"
)

tr := amqp.New("amqp://guest:guest@localhost:5672/") // lazy connect
defer tr.Close()

app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
app.Handle("urn:babel:orders:created", func(ctx context.Context, env babelqueue.Envelope) error {
    // handle env.Data ...
    return nil
})
app.Publish(ctx, "urn:babel:orders:created", map[string]any{"order_id": 1042})
app.Consume(ctx)
```

The BabelQueue core stays zero-dependency; this module is the only place
`amqp091-go` is pulled in.

Full spec: **[babelqueue.com](https://babelqueue.com)** · [MIT](../LICENSE) © Muhammet Şafak
