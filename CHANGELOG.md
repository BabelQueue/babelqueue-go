# Changelog

All notable changes to `babelqueue-go` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.8.0] - 2026-06-21

The new `gdpr` subpackage is the **runtime** half of GDPR sensitive-field governance
([ADR-0030](https://babelqueue.com)) — the registry already declares/audits
`x-gdpr-sensitive`; this enforces it on the wire. It lives in the **core** module
(zero-dependency, stdlib only, like `idempotency`, `schema` and `outbox`), so it ships
with every producer/consumer. The crypto is a caller-provided `Cipher` interface
(KMS/Vault/HSM/tokenisation); only a stdlib `AESGCMCipher` reference ships — **the core
pulls no crypto/KMS dependency (GR-7)**. The envelope stays **frozen**
(`schema_version: 1`): only the *values* of marked fields change (a sensitive value →
ciphertext string); `trace_id` is untouched and `data` stays pure JSON (GR-1/GR-3/GR-4).
This is the Go **reference** other SDKs mirror.

### Added
- **GDPR field-encryption helper** — a new `gdpr` subpackage in the **core** module
  (`github.com/babelqueue/babelqueue-go/gdpr`, zero-dependency) that encrypts the `data`
  fields a registry marked `x-gdpr-sensitive`, so PII never sits in cleartext on the
  broker ([ADR-0030](https://babelqueue.com)).
  - `gdpr.Cipher` — the encryption seam the **caller** binds to a KMS/Vault/HSM or a
    tokenisation service (`Encrypt(plaintext []byte) (string, error)` /
    `Decrypt(ciphertext string) ([]byte, error)`). The core defines it and pulls in **no**
    crypto dependency (GR-7).
  - `gdpr.AESGCMCipher` — a reference `Cipher` built only on the Go standard library
    (`crypto/aes` + `crypto/cipher` + `crypto/rand`): AES-256-GCM with a fresh random
    nonce per call, prepended and base64-encoded so it drops straight into a JSON string.
    The key is the caller's; a wrong-key/tampered `Decrypt` fails GCM authentication
    rather than returning corrupt plaintext.
  - `gdpr.Protect(data, schema, cipher)` / `gdpr.Unprotect(data, schema, cipher)` —
    standalone, opt-in producer/consumer helpers that encrypt/decrypt each
    `x-gdpr-sensitive` leaf **in place**. The sensitive paths come from the **same**
    per-URN `schema.Schema` the validation path loads (`schema.Schema.SensitivePaths`),
    covering nested objects (`profile.full_name`) and array items (`addresses[].line`);
    an absent field is skipped, not an error. A wrong-key `Unprotect` returns
    `gdpr.ErrDecrypt` so the message takes the retry / dead-letter path.
  - `schema.Schema` now parses the `x-gdpr-sensitive` extension keyword (boolean `true`
    or a string category) and exposes `SensitivePaths()` — additive and
    **validation-neutral**: the keyword never makes a value valid or invalid, mirroring
    `babelqueue-registry`'s model, so annotating a schema is never a breaking change (GR-1).

    The envelope is unchanged (`schema_version: 1`); this is purely additive (GR-6). The
    encrypted value is a JSON string, so `data` stays pure JSON (GR-3) and `trace_id` is
    preserved (GR-4).

The new `…/asynq` and `…/machinery` modules are published as the Go submodule tags
`asynq/v1.0.0` and `machinery/v1.0.0`
(`go get github.com/babelqueue/babelqueue-go/{asynq,machinery}`); the core and other
transport modules are unchanged. asynq **requires Go 1.24+** (its floor); machinery floors at
Go 1.23.

The new `…/idempotency-postgres` and `…/idempotency-redis` modules are persistent
[`idempotency.Store`](idempotency) backends in their own submodules (own `go.mod`,
`replace`-based on the core like the transport submodules) — the **core stays
zero-dependency (GR-7)**; a database/Redis driver is pulled in only if you import them.
Both floor at **Go 1.21** (the core's floor): postgres on `database/sql` + pgx
`v5.7.0`, redis on the same `go-redis/v9` the `…/redis` transport uses.

The new `outbox` subpackage is the Go port of the PHP `BabelQueue\Outbox` helper
([ADR-0029](https://babelqueue.com)) — a transactional outbox in the **core** module
(zero-dependency, stdlib only, like `idempotency` and `schema`), so it ships with every
producer. The `Store` is an interface the caller binds to their own DB; only an
in-memory reference ships (**no DB driver in the core, GR-7**). The envelope is unchanged
(`schema_version: 1`); this is purely additive.

### Added
- **transactional outbox helper** — a new `outbox` subpackage in the **core** module
  (`github.com/babelqueue/babelqueue-go/outbox`, zero-dependency) that removes the
  producer dual write ([ADR-0029](https://babelqueue.com)): the message is persisted into
  the same database, in the same transaction, as the business write — so it commits or
  rolls back atomically with it — and a separate relay publishes the durable rows
  afterwards. The Go mirror of the PHP `BabelQueue\Outbox` helper and the producer-side
  counterpart to the consumer-side `idempotency` package.
  - `outbox.Store` — the persistence contract the **caller** binds to their own DB
    (`Save(encoded, queue) (id, err)`, `FetchUnpublished(limit)`, `MarkPublished(ids)`,
    `MarkFailed(id, reason)`). The core defines it and pulls in **no** DB driver (GR-7);
    `FetchUnpublished` SHOULD lock/claim rows (`FOR UPDATE SKIP LOCKED`) in a concurrent
    adapter — documented as the adapter's job, not the in-memory reference's.
  - `outbox.Outbox.Write(env)` — encodes via the **frozen** envelope codec (bytes stored
    verbatim, GR-1), captures the target queue from `meta.queue` (else `"default"`), and
    delegates to `Store.Save`. It does **not** begin/commit anything — **the caller owns
    the transaction boundary**, which is the whole point.
  - `outbox.Relay` — `Flush(ctx)` publishes one batch through the frozen `Transport`
    seam, marking each row published only **after** publish returns, or failed (caught,
    `MarkFailed`, left pending) with a bounded linear backoff (injectable sleeper); one
    poison row never blocks the batch. `Drain(ctx, maxPasses)` loops `Flush` until a pass
    makes no progress (a safety ceiling). It publishes the **stored bytes verbatim** —
    never decoding/rebuilding the envelope — so `trace_id` is preserved end-to-end (GR-4)
    and cross-SDK parity holds (GR-5).
  - `outbox.InMemoryStore` — a process-local reference `Store` for tests / single-process
    demos (no real transaction; use a DB-backed adapter in production).

    The envelope is unchanged (`schema_version: 1`); this is purely additive (GR-6).
- **persistent idempotency stores (Postgres + Redis)** — two new modules implementing
  the **unchanged** core `idempotency.Store` interface ([ADR-0022](https://babelqueue.com)),
  so a fleet of consumers shares one dedupe record keyed on the envelope's `meta.id`
  instead of every worker keeping its own in-memory set:
  - `…/idempotency-postgres` (`github.com/babelqueue/babelqueue-go/idempotency-postgres`) —
    over `database/sql` (pgx driver). The atomic claim is
    `INSERT … ON CONFLICT (message_id) DO NOTHING/UPDATE … RETURNING` on the table's
    primary key: exactly one of N concurrent inserts wins (RETURNING proves it), a
    duplicate hits the conflict, and an expired row is re-claimed atomically by the
    `DO UPDATE … WHERE expires_at <= now()` branch. DDL ships as `schema.sql` and via
    `Store.Migrate(ctx)`. `New(ctx, dsn)` owns the pool; `NewWithDB(db)` borrows one;
    `WithTable`/`WithTTL` configure namespace and expiry.
  - `…/idempotency-redis` (`github.com/babelqueue/babelqueue-go/idempotency-redis`) —
    reuses the `go-redis` client the `…/redis` transport uses. The atomic claim is
    `SET key value NX PX <ttl>`: Redis evaluates `NX` single-threaded so exactly one
    `SET` creates the key, duplicates see it present, and Redis expires it natively via
    `PX`. `New(url)` owns the client; `NewWithClient(client)` borrows one;
    `WithPrefix`/`WithTTL` configure namespace and expiry.
  - Both add a non-interface `Claim(ctx, id) (bool, error)` exposing the race result
    (`true` = first delivery / won, `false` = duplicate / parked); `Remember` delegates
    to `Claim` and discards the bool, so `idempotency.Wrap` accepts either store
    unchanged. Unit tests are DB/broker-free (Postgres via `go-sqlmock`, Redis via a fake
    client seam); integration tests prove concurrent claims serialize, duplicates are
    rejected and TTL expiry reclaims, skipping cleanly without `BABELQUEUE_TEST_PG` /
    `BABELQUEUE_TEST_REDIS`. The envelope is unchanged (`schema_version: 1`); these are
    purely additive.
- **machinery adapter** — new `…/machinery` module
  (`github.com/babelqueue/babelqueue-go/machinery`), a framework adapter over the Redis/AMQP-backed
  [machinery](https://github.com/RichardKnop/machinery) task queue (not a new broker binding — its
  storage is Redis/AMQP, [§1/§2](https://babelqueue.com)); the sibling of the `…/asynq` module.
  machinery is **argument-based**, so the envelope URN (`job`) is the task `Name` (machinery routes
  on it like every BabelQueue consumer) and a single string `Arg` carries the canonical envelope
  JSON byte-for-byte (no wrapping, no added fields). Produce:
  `NewProducer(server).Dispatch(ctx, urn, data, …)` (and `Send` for a pre-built envelope);
  `Signature`/`SignatureFor` expose the `*tasks.Signature` for setting machinery-native fields
  (`RetryCount`, `ETA`, `RoutingKey`). Consume: `Register(server, urn, handler)` registers the
  handler under the URN as the task name, decoding the envelope arg and rejecting an unacceptable
  envelope with `ErrNotAccepted`; `Bind`/`Envelope` expose the lower-level conversions. Unit- and
  conformance-tested against fake `Sender`/`Registrar` seams (no broker, no network). The envelope
  is unchanged (`schema_version: 1`); machinery is purely additive. Ships as a per-SDK MINOR.
- **asynq adapter** — new `…/asynq` module (`github.com/babelqueue/babelqueue-go/asynq`),
  a framework adapter over the Redis-backed [asynq](https://github.com/hibiken/asynq) task
  queue (not a new broker binding — asynq's storage is Redis, [§1](https://babelqueue.com)).
  The canonical envelope JSON is the asynq task `Payload` byte-for-byte (no wrapping, no added
  fields), and the envelope URN (`job`) is the asynq task `TypeName`, so asynq routes by URN
  like every BabelQueue consumer. Produce: `NewProducer(client).Dispatch(ctx, urn, data, …)`
  (and `Enqueue` for a pre-built envelope), with envelope options (`WithQueue`, `WithTraceID`)
  and asynq task options (`MaxRetry`, `ProcessIn`, …) mixed freely — distinguished by type.
  Consume: `Register(mux, urn, handler)` routes by URN on an `*asynq.ServeMux`, decoding the
  payload and rejecting an unacceptable envelope with `ErrNotAccepted`; `Bind`/`Task`/`Envelope`
  expose the lower-level conversions. Unit- and conformance-tested against a fake `Enqueuer`
  seam (no Redis, no network). The envelope is unchanged (`schema_version: 1`); asynq is purely
  additive. Ships as a per-SDK MINOR.

## [1.2.0] - 2026-06-13

The new `…/azureservicebus` module is published as the Go submodule tag
`azureservicebus/v1.0.0` (`go get github.com/babelqueue/babelqueue-go/azureservicebus`); the
core, `redis`, `amqp` and `sqs` modules are unchanged. **Requires Go 1.23+** (the Azure SDK floor).

### Added
- **Azure Service Bus transport** — new `…/azureservicebus` module
  (`github.com/babelqueue/babelqueue-go/azureservicebus`), a `babelqueue.Transport` over the
  official Azure SDK (`azure-sdk-for-go/.../azservicebus`). Implements
  [§4 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#azure-service-bus):
  the canonical envelope is the message `Body`, projected onto native fields (`Subject` = URN,
  `CorrelationID` = `trace_id`, `MessageID` = `meta.id`) plus the `bq-` application properties;
  PeekLock reserve → `CompleteMessage` ack; `attempts` reconciled to
  `max(body, DeliveryCount − 1)`. Options: `WithConnectionString`, `WithAzureClient`,
  `WithClient`, `WithMaxWaitTime`. Unit-tested against fake senders/receivers (no Azure, no
  network). The envelope is unchanged (`schema_version: 1`); ASB is purely additive. Ships as a
  per-SDK MINOR.

## [1.1.0] - 2026-06-12

The new `…/sqs` module is published as the Go submodule tag `sqs/v1.0.0`
(`go get github.com/babelqueue/babelqueue-go/sqs`); the core, `redis` and `amqp`
modules are unchanged at `v1.0.0`.

### Added
- **Amazon SQS transport** — new `…/sqs` module (`github.com/babelqueue/babelqueue-go/sqs`),
  a `babelqueue.Transport` over the official AWS SDK (`aws-sdk-go-v2`). Implements
  [§3 of the broker-bindings contract](https://babelqueue.com): the canonical
  envelope is the `MessageBody`, projected onto native `MessageAttributes`
  (`bq-job`/`bq-trace-id`/`bq-message-id`/`bq-schema-version`/`bq-source-lang`/`bq-created-at`);
  visibility-timeout reserve → `DeleteMessage` ack; `attempts` reconciled to
  `ApproximateReceiveCount − 1` (never lowering a runtime-incremented count). Options:
  `WithRegion`, `WithEndpoint` (LocalStack/ElasticMQ), `WithQueueURLPrefix`,
  `WithWaitTimeSeconds`, `WithVisibilityTimeout`, `WithFIFO`, `WithMessageGroupID`,
  `WithContentDedup`, `WithClient`. Unit-tested against a fake SQS client (≈90%);
  `-tags=integration` runs a LocalStack round-trip (covers the AWS config path). The
  envelope is unchanged (`schema_version: 1`); SQS is purely additive. Ships as a
  per-SDK MINOR.

## [1.0.0] - 2026-06-07

**1.0.0 — the public API is now SemVer-stable**: breaking changes require a MAJOR,
following the deprecation policy. The wire envelope is unchanged
(`schema_version: 1`). The `…/redis` and `…/amqp` transport modules are released as
`v1.0.0` alongside and now require the core at `v1.0.0`. Full reference at
[babelqueue.com](https://babelqueue.com).

### Internal
- CI adds **staticcheck** (core + `redis`/`amqp` modules) and a **>=90% core
  coverage gate** (`scripts/check-coverage.sh`, currently 92%); added runtime
  tests covering `Consume`/`Run`, the dead-letter options and the unknown-URN
  strategies.
- **GR-8 latency benchmark** (`bench_test.go`): `BenchmarkEnvelopeRoundTrip` /
  `BenchmarkBarePayloadJSON` plus `TestCodecOverheadWithinBudget`, which asserts the
  envelope encode/decode path adds **≤2%** over plain-JSON serialization vs a
  conservative 750µs broker round-trip (measured ~0.3%; corroborated at ~1.4% over a
  live loopback Redis round-trip).

## [0.2.0] - 2026-06-06

### Added
- **Runtime** — `App` (`NewApp`) with `Handle`, `Publish`, `Consume`/`Run` and a
  bounded `Drain`. Routes by URN over the canonical envelope; attempts-based
  retry → opt-in dead-letter queue; unknown-URN strategies
  (`fail`/`delete`/`release`/`dead_letter`). Options: `WithDefaultQueue`,
  `WithMaxAttempts`, `WithUnknownURNStrategy`, `WithDeadLetter`,
  `WithDeadLetterQueue`, `WithDeadLetterSuffix`, `WithPollTimeout`.
- **Transport** abstraction — `Transport` interface + `ReceivedMessage`, with an
  in-process `InMemoryTransport`. The runtime stays **zero-dependency**; broker
  drivers ship as separate modules:
  - `github.com/babelqueue/babelqueue-go/redis` — Redis transport (reliable
    BLMOVE + processing list), on `go-redis`.
  - `github.com/babelqueue/babelqueue-go/amqp` — RabbitMQ transport (durable
    queue, persistent delivery, contract AMQP properties, `basic.get` + manual
    ack), on `amqp091-go`.

### Notes
- The core module remains zero-dependency; only the `redis`/`amqp` submodules
  pull a broker driver.

## [0.1.0] - 2026-06-06

### Added
- `Envelope` / `Meta` types and `Make`, `Encode`, `Decode` — build, render and
  parse the canonical `{job, trace_id, data, meta, attempts}` envelope
  (`schema_version` 1). The single Go implementation of the wire format.
- `Encode` produces compact UTF-8 JSON with HTML escaping disabled — the canonical
  wire form (envelope frame byte-identical to the PHP and Python cores; `data` key
  order follows `encoding/json`, JSON-insignificant).
- `Envelope.URN()` — resolve the URN (`job`, accepting `urn` as an inbound alias).
- `Envelope.Accepts()` — consumer-side validation (rejects empty URN, unsupported
  `meta.schema_version`, blank `trace_id`, missing `data`).
- `Make` options `WithQueue` and `WithTraceID` (trace continuation).
- `Annotate` / `DeadLetter` — additive `dead_letter` block builder.
- `StrategyFail` / `StrategyDelete` / `StrategyRelease` / `StrategyDeadLetter`
  unknown-URN strategy constants; `ErrEmptyURN` / `ErrUnknownURN`.
- Shared cross-SDK **conformance suite** under `testdata/conformance/` (vendored
  from the canonical `conformance/` set) plus a `TestConformance` runner.

### Notes
- Pre-1.0: the public API may change before the `1.0.0` tag.
- **Zero dependencies** (standard library only); Go `>=1.21`.

[Unreleased]: https://github.com/BabelQueue/babelqueue-go/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/BabelQueue/babelqueue-go/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/BabelQueue/babelqueue-go/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/BabelQueue/babelqueue-go/compare/v0.2.0...v1.0.0
[0.2.0]: https://github.com/BabelQueue/babelqueue-go/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/BabelQueue/babelqueue-go/releases/tag/v0.1.0
