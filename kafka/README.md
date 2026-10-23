# BabelQueue Kafka transport (Go)

A Kafka-backed `babelqueue.Transport` for the [BabelQueue Go runtime](https://github.com/BabelQueue/babelqueue-go),
built on [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go) (pure Go, no cgo).

```bash
go get github.com/babelqueue/babelqueue-go/kafka
```

## Retry tiers and the re-inject consumer (ADR-0034)

Kafka has no delayed delivery. The retry-topic pattern parks a record on a tier topic
`<topic>.retry.<n>` (`n` starts at 1) and a consumer carries it back once the tier has elapsed.

```go
tr, _ := kafka.New(
    kafka.WithBrokers("localhost:9092"),
    kafka.WithGroupID("orders-reinject"),   // use a dedicated group for the re-injector
    kafka.WithRetryTiers(kafka.DefaultRetryTiers...), // 5s, 30s, 5m, 30m (RECOMMENDED, opt-in)
)
reinjector, _ := tr.NewReinjector("orders")
go reinjector.Run(ctx) // without it a retried or delayed record is never redelivered
```

- **Fail target:** `PublishRetryForAttempt(ctx, from, attemptsBefore)` writes to
  `<topic>.retry.<min(attemptsBefore, n-1)+1>`; once the tiers run out the last tier is kept. It sets
  `bq-attempts=attemptsBefore+1`, syncs the body `attempts`, sets `bq-delay` (the tier in ms) and
  `bq-original-topic`, and keeps the record key byte for byte.
- **Delay request:** `PublishDelayed` / `PublishRetry(ctx, from, delay)` pick the smallest tier that is
  >= `delay`, and return `ErrDelayUnsupported` when none fits (no clamping). `PublishRetry` is
  **deprecated** in favour of `PublishRetryForAttempt`; its behaviour is unchanged.
- **`retry_backoff` does not apply on Kafka:** the tier sets the delay.
- **Re-inject consumer:** for each record of `<topic>.retry.<n>` it waits until
  `record.timestamp + tier_ms` (clamped to `[now, now+tier_ms]`, so an old record is carried at once),
  writes the value and key byte for byte to `bq-original-topic` (no decode, no re-encode, `attempts`
  untouched, an undecodable record is carried as it is), drops `bq-delay` and `bq-original-topic`, keeps
  every other header (`traceparent` included) and commits the retry-topic offset only after the broker
  acknowledged the write (at-least-once). kafka-go has no partition pause/resume for a group reader, so
  the wait is held per partition in a worker fed through a bounded buffer: the fetch loop keeps polling
  while a record sleeps.
- **Create the retry topics with `message.timestamp.type=LogAppendTime` (SHOULD).** The wait is anchored
  on the record timestamp; with `LogAppendTime` it is the broker's append time, so a producer's clock
  cannot skew the delay. With the default `CreateTime` the producer's clock is trusted.
- **Poison records** (identity-less or undecodable envelopes) go to `<topic>.dlq`; the identity rule
  follows the conformance `kafka_poison` cases.

Full spec: **[babelqueue.com](https://babelqueue.com)** · [MIT](../LICENSE) © Muhammet Şafak
