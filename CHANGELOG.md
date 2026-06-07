# Changelog

All notable changes to `babelqueue-go` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

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

[Unreleased]: https://github.com/BabelQueue/babelqueue-go/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/BabelQueue/babelqueue-go/compare/v0.2.0...v1.0.0
[0.2.0]: https://github.com/BabelQueue/babelqueue-go/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/BabelQueue/babelqueue-go/releases/tag/v0.1.0
