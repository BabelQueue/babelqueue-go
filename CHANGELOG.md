# Changelog

All notable changes to `babelqueue-go` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

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

[Unreleased]: https://github.com/BabelQueue/babelqueue-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/BabelQueue/babelqueue-go/releases/tag/v0.1.0
