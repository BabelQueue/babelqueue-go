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

## Schema-version gate (§3.7)

`Pop` reads the `bq-schema-version` message attribute **before** it decodes the body. A
missing or blank attribute (empty, or only ASCII whitespace) takes the normal path.
Anything else is compared as-is — never trimmed — with the version the core supports
(`1`); on a mismatch (`"2"`, `"x"`, `"01"`, `" 1"`, a non-ASCII space, ...) the body is
**not decoded** and is handed to the `App` untouched with the reserved route
`babelqueue:unsupported-schema-version:<v>`. No handler owns that route, so the `App`
dead-letters the message **directly, with no retry** — whatever `WithMaxAttempts` is (the
handler is never reached): with `WithDeadLetter(true)` it lands on the dead-letter queue with
the `unknown_urn` reason and the rejected version in the recorded cause, and without
dead-lettering it is acknowledged (deleted) and dropped. `StrategyDelete` is the one
exception: it drops the message (acknowledged, no dead-letter record), as in the Python SDK.
If the dead-letter write itself fails the message is **not** deleted: it stays in the queue
and becomes visible again after the visibility timeout, and the failure is logged and
reported to `WithDeadLetterErrorHandler`. A retry could not change the outcome, so none is
attempted. This needs a core release with the direct dead-letter path (see the root
`CHANGELOG.md`).

The rejected version in the hook, the route and the dead-letter record is bounded to its
first 64 characters plus `...`, so a hostile attribute cannot bloat them.

An optional hook reports each gated message — useful to alert on a producer that is
ahead of this consumer. It runs before the body is touched, so it also fires for a body
that is not valid JSON, and a panic in it is recovered and ignored:

```go
tr, _ := sqs.New(ctx, sqs.WithOnSchemaVersionRejected(func(version, queue string) {
    log.Printf("quarantined schema_version=%q from %s", version, queue)
}))
```

Without the hook the gate still works; the `App`'s error names the rejected version.

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
