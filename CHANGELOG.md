# Changelog

All notable changes to `babelqueue-go` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added

- **core `Redrive`: now uses the optional `KeyedRepublisher` capability.** A DLQ redrive (ADR-0026)
  re-publishes through `PublishFrom` when the transport implements it, for both the write to the
  work topic and the restore write back to the DLQ (dry run, unselected, undecodable body, failed
  redrive write), so broker-native per-message metadata such as the Kafka record key is carried
  over. A transport without the capability behaves exactly as before (route-preserving plain
  publish); with `Bypass` set, a `HeaderPublisher` still takes precedence for the redrive write.
  No new exported API; envelope, `dead_letter` block and header projection unchanged. Core MINOR.

### Fixed

- **core `Redrive`: a failed restore write no longer acks the DLQ message.** When writing a
  message back to the DLQ fails (dry run, unselected, undecodable body, failed redrive write), the
  error is now returned and the DLQ message is not acknowledged, so it stays on the DLQ instead of
  being lost. Previously the restore error was swallowed and the message was acked
  unconditionally. Matches the Java and .NET SDKs.
- **`kafka`: the restore write back to the DLQ now preserves the record exactly.** Key, headers
  and value (including a null value or a non-UTF-8 body) are written back byte-for-byte, matching
  the Java and .NET Kafka transports.
- **`kafka`: a DLQ redrive now keeps the DLQ record's key.** Redrive through the `kafka`
  `Transport` (`KeyedRepublisher`) carries the key byte-for-byte on the redrive write and on the
  restore write back to the DLQ (a null key stays null and a non-empty key is carried
  byte-for-byte; never synthesised), per contract §6.5. An empty key is the exception: see
  Known limitations. Requires the core release containing the `Redrive` change above.

### Known limitations

- **`kafka`: an empty (zero-length) record key is written back as a null key.** kafka-go's
  high-level `Reader` (v0.4.51) cannot tell an empty record key from a null one: it returns a nil
  `Message.Key` for both. Every record the Go consumer reads therefore loses an empty key, so the
  consumer's retry and DLQ writes, and a DLQ redrive (which pops through the same `Reader`), write
  a null key instead. Null and non-empty keys are carried byte-for-byte. The other SDKs preserve
  an empty key (contract §6.5 SHOULD); a fix would need a protocol-level reader instead of
  `kafka.Reader`. The live integration test pins this behaviour.

## [1.12.0] - 2026-10-11

### Added

- **core: optional `KeyedRepublisher` transport capability** (`PublishFrom(ctx, queue, body, from)`).
  The `App` now hands the received message to a transport that implements it for every re-publish
  it makes on that message's behalf (retry, unknown-URN release fallback, dead-letter writes), so
  broker-native per-message metadata the plain `Publish` cannot express can be carried over.
  Additive and opt-in (a transport without it behaves exactly as before); this new exported
  interface makes the next core release a MINOR.

- **`kafka`: `(*Transport).Close()` and `ErrClosed`.** `Close` closes the readers the transport
  opened and, when it built its own Writer (no `WithWriter`), that Writer and its private kafka-go
  Transport; an injected Writer is left to its owner. After `Close`, `Pop` and `Ack` return
  `ErrClosed` and never open a new reader; a second `Close` is a no-op. New public API, so the
  `kafka` module is released as a MINOR.

### Fixed

- **`kafka`: a retry or dead-letter re-publish now keeps the consumed record's key.** The
  `kafka` `Transport` implements `KeyedRepublisher`: the retry re-publish, the envelope
  dead-letter write and the raw / version-gated (poison) dead-letter write carry the original
  record key byte-for-byte (a null key stays null, an empty key stays empty; a key is never
  synthesised). Previously the key was dropped (contract §6.5). Partition equality is not
  guaranteed. Envelope, `dead_letter` block and header projection are unchanged; the `kafka`
  module's next release is a MINOR with the core.

- **core `App`: a failed re-publish on the retry and unknown-URN release paths no longer loses the
  message.** When a transport has no in-place `Releaser` (or reports `ErrReleaseUnsupported`), a
  retry (handler error / `StrategyFail`) and a `StrategyRelease` unknown URN fall back to
  re-publish + ack; the re-publish error was discarded and the message acknowledged anyway. It is
  now treated like a failed dead-letter write: the message is not acknowledged, the failure is
  logged and reported to `WithReleaseErrorHandler`, and the message is handed back to the broker
  (`Rewinder` when the transport has it, otherwise it stays reserved and is redelivered per the
  transport's reservation semantics, e.g. a Redis message stays in its processing list until
  recovered). An envelope re-encode failure on the retry path is handled the same way. No
  public API, envelope or `dead_letter` change; core PATCH.

- **`kafka`: the default Writer no longer shares kafka-go's default Transport and now waits for
  broker acknowledgement.** The Writer `New` builds (when no `WithWriter` is given) used the shared
  default `kafkago.Transport`, whose 6s metadata cache also holds a negative answer, so the first
  write to a topic created moments earlier could fail with `Unknown Topic Or Partition`; and its
  `RequiredAcks` was the zero value (`RequireNone`), so a write was reported successful without
  waiting for any broker acknowledgement and broker-side produce failures (for example a failed
  retry or dead-letter re-publish) went unnoticed. It now uses a module-owned
  `Transport{MetadataTTL: 1s}` and `RequiredAcks: RequireAll`; such a failure is returned and the
  core `App` does not acknowledge the record (it is handed back). Impact: `Publish` latency now
  includes the in-sync replicas' acknowledgement. A Writer injected with `WithWriter` is unchanged.
  No envelope or wire change.

- **`kafka`: core pin raised to `v1.12.0`.** Key preservation needs the core `KeyedRepublisher`
  capability and the no-ack-on-failed-republish fix lives in the core `App`; the local
  `replace => ../` hides a stale pin from the repo's own tests but not from a consumer.

### Release note for the transport modules

Tag the core `v1.12.0` first, then `kafka/v1.2.0` (MINOR: `Close()`/`ErrClosed`, module-owned
Transport, key preservation). The other transport modules are unchanged and keep their pins.

## [1.11.1] - 2026-10-11

### Changed

- **core `App` and the gating transports (`kafka`, `artemis`, `sqs`, `pulsar`, `azureservicebus`):
  a version-gated delivery is dead-lettered as the incoming raw body, byte-identical.** The
  `bq-schema-version` gate (`bq_schema_version` on Artemis) rejects a message "without decoding it"
  (contract `broker-bindings.md` §3.7, §4.7, §5.7, §6.7, §7.7); the `App` previously decoded the body
  anyway and wrote a `dead_letter{reason: "unknown_urn"}`-annotated record, which made Go differ
  from Java, .NET and Node (they forward the raw value). The `App` now checks the gate route before
  decoding and publishes `ReceivedMessage.Body` to the dead-letter queue unchanged: no decode, no
  `dead_letter` block. A failed dead-letter write is still not acknowledged (the `Rewinder` /
  stay-reserved pattern is unchanged), the delivery is still never retried, `StrategyDelete` and
  dead-lettering off still drop it, and the poison, handler-error and unknown-URN dead-letter
  records are unchanged. Impact: a DLQ consumer that expected an annotated `unknown_urn` record
  for a version-gated message now receives the original body instead (the rejected version is still
  reported through `WithOnSchemaVersionRejected`). The `Transport` API only carries a body, so the
  Java/.NET/Node forwarding of the source key and headers is not reproduced. No wire change; this
  reverses the annotated-record decision of the previous release's gate work. The transport
  modules (`kafka`, `artemis`, `sqs`, `pulsar`, `azureservicebus`) need no code change of their
  own - they already hand the raw body over - but should pin the core release that carries this.

### Fixed

- **core `App`: an undecodable (poison) body is dead-lettered wrapped, with no retry.** Previously the
  `StrategyRelease` (and `StrategyFail` / `StrategyDeadLetter`) paths handed a body that does not
  decode to the retry machinery with an empty `Envelope`, which re-published an empty envelope and
  finally dead-lettered an annotated empty one, so the original body was lost. With dead-lettering
  on (`WithDeadLetter(true)` or `StrategyDeadLetter`) the body is now dead-lettered at once, never
  retried, and acknowledged only after the DLQ write succeeded. The record follows the PHP
  reference (ADR-0009, `error-handling.md` §6): a body that is a JSON object keeps its keys and
  values (key order and whitespace are normalised), any other body is carried as
  `{"raw": <body>}`, and a `dead_letter` block with `reason: "poison"` is added (a body that is
  not valid UTF-8 is stored with U+FFFD substitutions). A failed write is not acknowledged:
  the message is handed back (`Rewinder` when the transport has it, otherwise it stays reserved), and the failure is reported to
  `WithDeadLetterErrorHandler`.
  `StrategyDelete` still drops it, and with dead-lettering off the behaviour is unchanged.
- **core `App`: a failed dead-letter write is no longer swallowed.** After retries were exhausted
  (or for an unknown URN under `StrategyDeadLetter`) the `App` ignored a DLQ publish error and
  acknowledged the message anyway, losing it. It is now left unacknowledged and handed back the
  same way as a version-gated delivery. Note that on redelivery the handler runs again until the
  DLQ write succeeds (with a `Rewinder` transport, paced by `WithRewindPause`, 1s by default), so
  a handler with side effects should be idempotent (see the idempotency helper, ADR-0022).
- **`artemis`: a `bq_schema_version` application property that is present with a null value is
  treated as absent.** Previously the §7.7 gate rejected a null value as an unsupported version
  (dead-letter path); it now falls through to the normal decode path, like a missing property and
  like Azure Service Bus (`null` = no version). Locked by a new `null` case in the shared
  conformance `artemis.schema_version_gate`. No wire change.

- **`sqs`, `azureservicebus`, `kafka`, `artemis`, `pulsar`: core pin raised to `v1.11.1`.** The
  raw gate dead-letter forward and the poison/failed-DLQ-write fixes live in the core `App`; the
  local `replace => ../` hides a stale pin from the repo's own tests but not from a consumer.

### Release note for the transport modules

Tag the core `v1.11.1` first, then the modules: `artemis/v1.1.1`, `kafka/v1.1.1`, `sqs/v1.10.1`,
`azureservicebus/v1.1.1` and `pulsar/v1.2.1` (each PATCH: core pin only, plus the `artemis` null fix).

## [1.11.0] - 2026-10-10

### Added

- **core: `Rewinder`, `WithDeadLetterErrorHandler`, `WithRewindPause`, `TruncateDeclaredVersion`.**
  New exported API for the version-gate dead-letter path: the optional `Rewinder` transport
  interface (`Rewind(ctx, msg)`), the `WithDeadLetterErrorHandler(func(ctx, msg, err))` hook, the
  `WithRewindPause(d)` option and the `TruncateDeclaredVersion` helper. See the `### Fixed`
  entries below for the behaviour.
- **`sqs`, `azureservicebus`, `kafka`, `artemis`: `WithOnSchemaVersionRejected(func(version, queue string))`**
  option (same signature as `pulsar`), and `Rewind` on `pulsar`, `azureservicebus`, `kafka` and
  `artemis` (implementing `Rewinder`). No wire change.

### Fixed

- **core `App`: a version-gated delivery is dead-lettered directly, with no retry.** A delivery
  whose transport handed it over under the reserved `babelqueue:unsupported-schema-version:<v>`
  route (the `bq-schema-version` gate) can never be handled by any registered handler, so
  retrying it only delays the same outcome. The `App` no longer sends it through `routePoison`'s
  bounded retry: with `WithDeadLetter(true)` (or `StrategyDeadLetter`) it publishes one annotated
  record (reason `unknown_urn`, the rejected version in the recorded cause) to the dead-letter
  queue and acknowledges the original, and without dead-lettering it acknowledges (drops) it. This
  holds for the `StrategyFail`, `StrategyRelease` and `StrategyDeadLetter` strategies, for any `WithMaxAttempts`,
  for an undecodable body, and for every transport that gates (`pulsar`, `sqs`, `azureservicebus`,
  `kafka`, `artemis`), matching the Python SDK. Handler failures and genuinely unknown URNs keep
  their bounded retry. `StrategyDelete` is the exception, as in the Python SDK: it drops a
  version-gated delivery (acknowledged, no dead-letter record) even with dead-lettering on. No
  wire change. Transports must pin the core release that carries this change to get the direct
  dead-letter path.
- **core `App`: a version-gated delivery whose dead-letter write fails is never acknowledged.**
  The failure is logged (`log/slog`) and reported to the new
  `WithDeadLetterErrorHandler(func(ctx, msg, err))` hook; the handler is never called and the
  retry path is never used. The message is handed back to the broker through the new optional
  `Rewinder` transport interface (`Rewind(ctx, msg)`), then the consume loop pauses
  `WithRewindPause(d)` (default 1s, as in the Python SDK) before the next poll. Transports
  without `Rewinder` (`sqs`: the message reappears after its visibility timeout) simply leave it
  unacknowledged. `kafka` implements `Rewind` by closing the cached group reader so the next
  `Pop` resumes from the last committed offset, i.e. the failed record: because the offset
  commit is cumulative, the commit never passes it. `artemis` releases the delivery
  (`ReleaseMessage`), `azureservicebus` abandons the lock (`AbandonMessage`). New exported API:
  `Rewinder`, `WithDeadLetterErrorHandler`, `WithRewindPause`, `TruncateDeclaredVersion`.
  The message is handed back (`Rewind`) before the hook is called, and a panic in the hook is
  recovered and logged, so a faulty hook cannot keep the message from being rewound.
  Transports must pin the core release that carries these additions.
- **`pulsar`: implements `Rewinder`.** When the `App` cannot write the dead-letter record for a
  version-gated message, `pulsar.Transport.Rewind` hands it back with `Nack` on the consumer so
  the broker redelivers it after the negative-ack delay; the message is never acked. The package
  doc no longer describes the gate as going through the bounded retry path (it is dead-lettered
  directly). Pin the `pulsar` module to the core release that carries `Rewinder`.
- **`sqs`, `azureservicebus`, `kafka`, `artemis`: the rejected `bq-schema-version` is bounded.**
  The declared value placed in the `WithOnSchemaVersionRejected` hook, in the reserved route and
  therefore in the dead-letter record's error is at most its first 64 characters plus `...`
  (`babelqueue.TruncateDeclaredVersion`), matching the Java and .NET SDKs; the gate comparison
  itself still uses the full value.
- **`sqs`, `azureservicebus`, `kafka`, `artemis`: pre-decode `schema-version` gate** (broker-bindings
  §3.7, §4.7, §6.3/§6.7, §7.7). `Pop` now reads the `bq-schema-version` message attribute /
  application property / record header (`bq_schema_version` on Artemis, ADR-0017) before it decodes
  the body, like the `pulsar` module already did. A missing or blank (ASCII whitespace) value takes
  the normal path; any other value that is not exactly the core's supported version (compared as-is,
  never trimmed) is not decoded and is routed through the core's reserved
  `babelqueue:unsupported-schema-version:<v>` route, so the `App` dead-letters it directly (see
  the core entry above) instead of mis-decoding a future-version body. On Azure Service Bus the
  application property is AMQP-typed: the string `"1"` and a native integer `1` of any width
  (signed or unsigned) are accepted, and so is a null value (it carries no version, so it takes the
  normal path like a missing property); every other value or type is rejected, floating-point
  values included (`1.0` too), as in the Java and .NET SDKs. Each module gains the optional
  `WithOnSchemaVersionRejected(func(version, queue string))` hook (same signature as `pulsar`).
  The only API addition; no wire change (`schema_version` stays `1`). On `kafka` the rejected
  record's delivery count is derived from its `bq-attempts` header.
- **`sqs`, `azureservicebus`, `kafka`, `artemis`, `pulsar`: core pin raised to `v1.11.0`.** The
  modules depend on the `Rewinder` interface and the direct dead-letter path of this core release
  (and `sqs` previously required `v1.9.0`, which predates `ReceivedMessage.RouteURN` and the
  reserved route); the local `replace => ../` hides a stale pin from the repo's own tests but not
  from a consumer. `pulsar` additionally gains the shared conformance reader for
  `schema_version_gate` (its gate is unchanged).

### Release note for the transport modules

Tag the core `v1.11.0` first (new exported core API: `Rewinder`, `WithDeadLetterErrorHandler`,
`WithRewindPause`, `TruncateDeclaredVersion` - MINOR), then the modules: `sqs/v1.10.0`,
`azureservicebus/v1.1.0`, `kafka/v1.1.0`, `artemis/v1.1.0` (each: new `WithOnSchemaVersionRejected`
option, MINOR) and `pulsar/v1.2.0` (new `Transport.Rewind`, MINOR).

## [1.10.0] - 2026-10-10

### Added

- **`ReceivedMessage.RouteURN` routing seam** (core, MINOR). An optional field a transport can
  set when its broker carries the URN out of band. When non-empty the `App` looks the handler
  up by it instead of the decoded body's URN; empty (the default for every existing transport)
  keeps routing on the body. The body is never rewritten, so the handler, the retry re-publish
  and the dead-letter record all carry the producer's original body. No wire change.
- **`RoutedPublisher` optional transport capability** (core, MINOR). `PublishRouted(ctx, queue,
  body, routeURN)`. When a delivery carries a `RouteURN` and the transport implements it, the
  `App` uses it for its retry, unknown-URN release and dead-letter writes, and `Redrive`, so the broker's routing
  property keeps the original value across attempts (no flip to the body URN on the second
  delivery, and the DLQ record keeps it too). Transports without it, and deliveries without a `RouteURN`, use plain `Publish`.
  The `pulsar` module implements it (`bq-job` pinned to the route URN; blank derives from the
  body). No wire change.

### Fixed

- **Pulsar consumer version-gates on `bq-schema-version`** (`pulsar` module, broker-bindings §5.7
  step 3). A message whose `bq-schema-version` property is present but not the core's supported
  version (`"2"`, `"x"`, `"01"`, ...) is now rejected **before its body is decoded**: `Pop` hands
  it to the `App` unchanged under a reserved route (`babelqueue:unsupported-schema-version:<value>`)
  that no handler owns, so it is quarantined through the existing unknown-URN retry /
  dead-letter path (the error names the rejected version). A missing or blank property, and `"1"`,
  behave exactly as before. "Blank" is the same set in every SDK: the empty string or only ASCII whitespace (space, `\t`, `\n`, `\v`, `\f`, `\r`); anything else (NBSP, U+001C-U+001F, U+0085, U+FEFF, U+3000, ...) is not blank and is rejected unless it is exactly the supported version. (Go `strings.TrimSpace` is not used: it also trims Unicode spaces.) No wire change. (A post-decode `Envelope.Accepts()` check in the
  `App` dispatch path is out of scope.) The reserved route is an in-process marker only: it
  also carries the delivery's original `bq-job`, and the retry / dead-letter re-publish writes
  that original `bq-job` (or, when the delivery had none, the body's job) plus the original raw
  `bq-schema-version` back, never the sentinel, so a redrive meets the same gate. The new
  optional `WithOnSchemaVersionRejected(func(version, queue string))` hook (additive) names the
  rejected version, including for a body that is not valid JSON, which the `App` quarantines
  as a poison message whose error names the reserved route.
- **Pulsar consumer follows broker-bindings §5.2 / §5.7** (`pulsar` module). `Pop` now routes
  on the `bq-job` property (the body's `job` is used only when the property is absent or
  blank) and takes `attempts` from the authoritative `bq-attempts` property (the body's value
  is used when the property is absent or not a plain non-negative ASCII-decimal integer that
  fits a 32-bit int), then raises it to `max(.., RedeliveryCount)`. Previously the body decided
  both and `bq-attempts` was never read. Routing is done through the new
  `ReceivedMessage.RouteURN` (filled from `bq-job`); the body's `job` / `urn` alias is **not**
  rewritten, so a `bq-job` / body mismatch no longer leaks into retries or the DLQ. A body with
  no `job` is still rejected as non-conformant. The new optional
  `pulsar.WithOnRoutingMismatch(func(propertyURN, bodyURN string, env *babelqueue.Envelope))`
  option reports a mismatch for an accepted envelope, after attempts are reconciled (a panic in
  it is recovered; delivery and ack are unaffected). No wire change.
- **`StrategyRelease` no longer loops on a transport version-gated delivery** (core `App`). A
  delivery whose `RouteURN` starts with the reserved `babelqueue:unsupported-schema-version:`
  prefix (the `pulsar` module's `bq-schema-version` gate, broker-bindings §5.7 step 3) can never
  be handled, yet `StrategyRelease` re-published it unchanged on every delivery, forever. It now
  takes the same bounded path as an empty-URN message: `StrategyRelease` is treated as
  `StrategyFail` (attempts + 1 re-publish, then dead-letter or drop); the other strategies are
  unchanged and the error still names the rejected version. A genuinely unknown URN under
  `StrategyRelease` is still released verbatim. No public API or wire change.
- **Release note for the `pulsar` module:** it now depends on the core's
  `ReceivedMessage.RouteURN`, so `pulsar/v1.1.0` (MINOR: new `WithOnRoutingMismatch` /
  `WithOnSchemaVersionRejected` options and `PublishRouted`) must `require` core `v1.10.0`, which
  ships this seam and the version-gate `StrategyRelease` fix above (the module resolves the core
  through `replace ../` locally).

## [1.9.0] - 2026-10-03

Forward-compatible re-emit, Unicode-correct `minLength`, the SQS §3 in-place release,
and a Go 1.24 floor. The envelope stays **frozen** (`schema_version: 1`) and golden
fixtures encode byte-for-byte as before.

### Added

- **Unknown-key round-trip** (message-envelope.md §4/§8, GR-5): `Decode` keeps unknown
  top-level and `meta` keys and `Encode` re-emits them verbatim after the known fields
  (sorted), so retry (`attempts++`), dead-lettering, redrive, outbox relay and SQS attempt
  reconciliation no longer drop a newer producer's keys. An envelope without unknown keys
  encodes exactly as before.
- **Forbidden-key warnings** (message-envelope.md §10, K-15): `timestamp`,
  `meta.max_retries`, `meta.attempts`, `meta.source` and `meta.ts` are dropped on decode
  (never re-emitted) and reported via the new `Envelope.Warnings()`; decode still succeeds.
- **`Releaser`** optional transport capability plus the `WithRetryBackoff` and
  `WithUnknownURNReleaseDelay` App options: a transport that implements it redelivers a
  retried / unknown-URN-released message in place after the backoff instead of
  re-publishing a copy.
- **sqs:** `Transport.Release` — contract §3 release/nack via
  `ChangeMessageVisibility(ReceiptHandle, VisibilityTimeout = backoff seconds)`, clamped to
  0–43200; the message is neither deleted nor re-sent and `ApproximateReceiveCount` stays
  the attempt counter. Uses the new optional `VisibilityAPI` client interface (the AWS
  client satisfies it; `API` is unchanged). Requires `sqs:ChangeMessageVisibility`.
  Every handler failure with attempts left, and every unknown-URN `release`, now goes
  through it; the default delay is **0 s** for both. If the call fails the message stays
  reserved (redelivered at visibility expiry) — the App never re-publishes a copy. Only a
  client without `ChangeMessageVisibility` (`ErrReleaseUnsupported`, which wraps the new
  core `babelqueue.ErrReleaseUnsupported`) keeps the old re-publish + delete path.
  **Poison-loop risk:** with the 0 s default a message that always fails is redelivered
  immediately until `WithMaxAttempts`; configure a `RedrivePolicy` (`maxReceiveCount`) on
  every SQS queue as a broker-side backstop and consider a non-zero `WithRetryBackoff`.
- **`ReceivedMessage.DeliveryCount`** (broker-native delivery count, 0 when unknown): the
  App floors `attempts` at `DeliveryCount − 1`, so the `WithMaxAttempts` bound and
  dead-lettering hold for an in-place release even when the body cannot carry the counter
  (e.g. it does not decode — such a message is dead-lettered after `WithMaxAttempts`
  deliveries instead of looping at 0 s). The SQS transport sets it from
  `ApproximateReceiveCount`.
- **`WithAckErrorHandler`** App option: reports a failed acknowledgement (e.g. an SQS
  `DeleteMessage` error after a successful handler) distinctly. An ack failure is never
  treated as a handler failure — the message is not retried, released or dead-lettered.
- **`WithReleaseErrorHandler`** App option: reports a failed in-place release (e.g. an
  SQS `ChangeMessageVisibility` `AccessDenied` when the IAM policy lacks
  `sqs:ChangeMessageVisibility`). The message still stays reserved and is redelivered at
  the queue's visibility timeout instead of `WithRetryBackoff`; the hook makes that
  timing change observable. `ErrReleaseUnsupported` is not reported (it selects the
  re-publish + ack fallback).
- **sqs:** the `sqs` module now requires core `v1.9.0` (it uses `Releaser`,
  `ErrReleaseUnsupported` and `DeliveryCount`); tag the core `v1.9.0` before `sqs/v1.9.0`.
- Conformance runners for the `roundtrip`, `data_shape`, `forbidden_keys` and
  `payload_schema_unicode` manifest sections.
- `.github/dependabot.yml` (weekly `gomod` per module + `github-actions`).

### Changed

- **Go 1.24 floor** (was 1.21) for the core, `sqs`, `amqp`, `redis`, `otel`,
  `idempotency-redis` and `idempotency-postgres`, and (was 1.23) for `azureservicebus`,
  `pulsar`, `kafka` and `machinery` — a module cannot declare a lower `go` directive than
  the core it requires. `artemis` stays at 1.25. CI matrix is now Go 1.24 / 1.25.
- A body that does not decode, or names no URN, is no longer released under the
  unknown-URN `StrategyRelease` (no handler can ever claim it, so the release looped
  forever): it takes the bounded `StrategyFail` path — retried up to `WithMaxAttempts`
  (floored by `DeliveryCount`), then dead-lettered (`reason: unknown_urn`) or dropped. The
  other strategies (`fail`, `delete`, `dead_letter`) are unchanged for such bodies.
  **Behaviour change for `StrategyRelease` users:** with `WithDeadLetter` off, "dropped"
  means the body is acked — on SQS permanently deleted (`DeleteMessage`) — once
  `WithMaxAttempts` is reached, so it no longer reaches the queue's broker-side
  `RedrivePolicy`/DLQ and nothing is logged. With `WithDeadLetter` on, the dead-lettered
  envelope is empty (no `job`) and the raw body is not preserved. If you relied on the
  broker DLQ keeping such bodies intact, set `WithMaxAttempts` above the queue's
  `maxReceiveCount` — this is App-wide, so it also routes every handler failure to the
  broker DLQ (raw, without a `dead_letter` block) instead of the App's `WithDeadLetter` queue.

### Fixed

- Decode no longer captures a key that differs from a known envelope key only in case
  (`"JOB"`, `"Attempts"`) as an unknown key — `encoding/json` already decoded it into the
  field, so it was re-emitted twice. Case variants of forbidden keys are warned and dropped.
- **sqs:** `visibilitySeconds` clamps before rounding, so an extreme backoff can no longer
  overflow to a negative `VisibilityTimeout`.
- **schema:** `minLength` counts Unicode code points (`utf8.RuneCountInString`), not
  bytes — a non-ASCII string no longer satisfies a minimum it is too short for.

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
