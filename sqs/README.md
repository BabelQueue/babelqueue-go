# babelqueue-go/sqs

Amazon SQS transport for the [BabelQueue Go runtime](https://github.com/babelqueue/babelqueue-go) —
"Polyglot Queues, Simplified."

It implements [§3 of the broker-bindings contract](https://babelqueue.com): the
canonical envelope is the message body, and the contract fields are projected onto
native SQS `MessageAttributes` so a PHP/Python/… peer can route and correlate
**without parsing the body**.

## Install

```bash
go get github.com/babelqueue/babelqueue-go/sqs
```

## Use

```go
ctx := context.Background()

tr, err := sqs.New(ctx,
    sqs.WithRegion("eu-central-1"),
    // sqs.WithEndpoint("http://localhost:4566"), // LocalStack/ElasticMQ
)
if err != nil { log.Fatal(err) }

app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))

// produce
app.Publish(ctx, "urn:babel:orders:created", map[string]any{"order_id": 1042})

// consume
app.Handle("urn:babel:orders:created", func(ctx context.Context, env babelqueue.Envelope) error {
    // env.Data, env.TraceID ...
    return nil
})
app.Run(ctx)
```

Credentials come from the standard AWS default provider chain (env vars, shared
profile, instance role, IRSA, SSO). FIFO queues: add `sqs.WithFIFO(true)` (the queue
name must end in `.fifo`).

## How it maps to the contract (§3)

| Envelope | SQS |
| :--- | :--- |
| body | `MessageBody` (byte-identical across SDKs) |
| `job` (URN) | `MessageAttributes.bq-job` |
| `trace_id` | `MessageAttributes.bq-trace-id` |
| `meta.id` | `MessageAttributes.bq-message-id` |
| `meta.schema_version` | `MessageAttributes.bq-schema-version` (Number) |
| `meta.lang` | `MessageAttributes.bq-source-lang` |
| `meta.created_at` | `MessageAttributes.bq-created-at` (Number, ms) |
| `attempts` | reconciled to `ApproximateReceiveCount − 1` on receive |
| reserve / ack | visibility timeout → `DeleteMessage` |
| release (handler failure, unknown-URN `release`) | `ChangeMessageVisibility(VisibilityTimeout = backoff s)` — never deleted or re-sent |
| out-of-band headers (e.g. `traceparent`) | extra String `MessageAttributes` (see below) |

The envelope is unchanged (`schema_version` stays `1`); SQS is purely additive.

## Release and the poison-message risk

When a handler fails (and attempts remain) or an unknown URN uses the `release`
strategy, the App releases the message in place with `ChangeMessageVisibility`. The
delay is `WithRetryBackoff` / `WithUnknownURNReleaseDelay` — **default `0` s**, i.e.
the message becomes visible again immediately (clamped to SQS's 0–43200 s). If that
call fails, the message is left reserved and SQS redelivers it when its visibility
timeout lapses; it is never deleted or re-sent. The IAM principal needs
`sqs:ChangeMessageVisibility`.

A zero delay means a message that always fails is redelivered in a tight loop until
the App's `WithMaxAttempts` is reached (counted from `ApproximateReceiveCount`, surfaced
as `ReceivedMessage.DeliveryCount` — so the bound also holds for a body that does not
decode — under the `release` strategy such a body bypasses `release` and takes the
bounded `fail` path rather than being redelivered forever; `delete` acks it at once and
`dead_letter` writes it to the DLQ at once). On that `fail` path, once `WithMaxAttempts`
is reached the body is dead-lettered if `WithDeadLetter` is on — as an empty envelope,
the raw body is not preserved — and otherwise **deleted with `DeleteMessage`**: it never
reaches the queue's `RedrivePolicy`, and nothing is logged. To keep such bodies intact in
the broker-side DLQ, set `WithMaxAttempts` above the queue's `maxReceiveCount` — note this is App-wide, so it
also routes every handler failure to the broker DLQ (raw, without a `dead_letter` block)
instead of the App's `WithDeadLetter` queue. A failed `ChangeMessageVisibility` (e.g. a missing IAM permission) is
reported through `WithReleaseErrorHandler`; the retry then waits for the queue's
visibility timeout rather than `WithRetryBackoff`. A failed
`DeleteMessage` after a successful handler is an ack failure, not a handler failure: it
is reported through `WithAckErrorHandler` and the message is not released. As a
broker-side backstop — and for any consumer that stops before dead-lettering it —
**configure a `RedrivePolicy` with a `maxReceiveCount` on every queue**, and set a
non-zero `WithRetryBackoff` where a hot retry loop is undesirable.

## Out-of-band transport headers

This transport implements the optional `babelqueue.HeaderPublisher` capability:
out-of-band headers ride as String `MessageAttributes` **beside the frozen envelope,
never in it** (GR-1), and are surfaced back to the consumer on
`ReceivedMessage.Headers`. So the optional [`otel`](../otel) module's W3C
`traceparent` (ADR-0028) — and the `bq-replay-bypass` marker (ADR-0027) — propagate
over SQS for true cross-hop span parent-child linkage. Headers are merged beside the
contract `bq-*` attributes without clobbering them, and bounded by the **10-attribute
SQS limit** (the contract attributes are always preserved first).

## Test

```bash
go test ./...                       # unit (fake SQS client, no broker)
go test -tags=integration ./...     # integration (needs LocalStack at SQS_ENDPOINT)
```

The default unit run fakes the AWS client. The `New` config-loading path is covered
by the LocalStack integration test (`-tags=integration`).

## License

MIT
