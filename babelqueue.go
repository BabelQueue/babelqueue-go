// Package babelqueue is the framework-agnostic Go core of BabelQueue —
// "Polyglot Queues, Simplified".
//
// It implements the canonical wire envelope so a Go service interoperates
// byte-for-byte with the PHP/Laravel, Python, ... SDKs over any broker, with no
// language-specific serialization on the wire:
//
//	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1042})
//	body, _ := env.Encode() // publish body to Redis / RabbitMQ / ...
//
//	in, _ := babelqueue.Decode(body)
//	if in.Accepts() {
//		switch in.URN() {
//		case "urn:babel:orders:created":
//			// handle in.Data ...
//		}
//	}
//
// The package depends only on the standard library.
//
// Full spec: https://babelqueue.com
package babelqueue

import "errors"

// SchemaVersion is the wire envelope schema version this core implements. It is
// versioned independently of the module's release (package) version: the wire
// format only changes when this number does.
const SchemaVersion = 1

// SourceLang is stamped into meta.lang for envelopes produced by this core.
const SourceLang = "go"

// Unknown-URN strategies: what a consumer does with a message whose URN has no
// registered handler. These mirror the constants in every other SDK core.
const (
	StrategyFail       = "fail"        // surface an error; let the worker decide
	StrategyDelete     = "delete"      // drop the message
	StrategyRelease    = "release"     // requeue for another consumer
	StrategyDeadLetter = "dead_letter" // route to the dead-letter queue
)

// Sentinel errors returned by the core.
var (
	// ErrEmptyURN is returned by Make when the URN is blank — a polyglot message
	// must expose a stable, non-empty URN so consumers can identify it without
	// relying on any language-specific class name.
	ErrEmptyURN = errors.New("babelqueue: a polyglot message must expose a stable, non-empty URN")

	// ErrUnknownURN signals that no handler is mapped for a message URN.
	ErrUnknownURN = errors.New("babelqueue: no handler is mapped for the message URN")
)
