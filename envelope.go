package babelqueue

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

// Envelope is the canonical BabelQueue wire message: a strict, language-neutral
// JSON shape ({job, trace_id, data, meta, attempts}) that every SDK produces and
// consumes identically. The field order below is significant — it matches the
// other cores so the envelope frame encodes identically across languages. (Key
// order inside the user-supplied Data map is JSON-insignificant; encoding/json
// emits it sorted, where PHP/Python preserve insertion order — same JSON.)
type Envelope struct {
	Job        string         `json:"job"`      // the message URN (never a class name)
	TraceID    string         `json:"trace_id"` // correlation id, preserved across hops
	Data       map[string]any `json:"data"`     // the message payload
	Meta       Meta           `json:"meta"`
	Attempts   int            `json:"attempts"`              // top-level transport retry counter
	DeadLetter *DeadLetter    `json:"dead_letter,omitempty"` // present only once dead-lettered
}

// Meta is the immutable per-message metadata block.
type Meta struct {
	ID            string `json:"id"`
	Queue         string `json:"queue"`
	Lang          string `json:"lang"`
	SchemaVersion int    `json:"schema_version"`
	CreatedAt     int64  `json:"created_at"` // Unix milliseconds, UTC
}

// Option customizes Make.
type Option func(*makeConfig)

type makeConfig struct {
	queue   string
	traceID string
}

// WithQueue sets the logical queue name recorded in meta.queue (default "default").
func WithQueue(queue string) Option { return func(c *makeConfig) { c.queue = queue } }

// WithTraceID reuses an existing trace id (trace continuation) instead of minting
// a fresh one. A blank value is ignored.
func WithTraceID(traceID string) Option { return func(c *makeConfig) { c.traceID = traceID } }

// Make builds the canonical envelope for a (urn, data) pair. It mints a fresh
// trace id unless WithTraceID is given, starts attempts at 0, and stamps meta
// with a unique id, the source language ("go"), the schema version and a
// millisecond timestamp. It returns ErrEmptyURN when urn is blank.
func Make(urn string, data map[string]any, opts ...Option) (Envelope, error) {
	urn = strings.TrimSpace(urn)
	if urn == "" {
		return Envelope{}, ErrEmptyURN
	}

	cfg := makeConfig{queue: "default"}
	for _, o := range opts {
		o(&cfg)
	}

	traceID := strings.TrimSpace(cfg.traceID)
	if traceID == "" {
		traceID = uuidV4()
	}
	if data == nil {
		data = map[string]any{}
	}

	return Envelope{
		Job:     urn,
		TraceID: traceID,
		Data:    data,
		Meta: Meta{
			ID:            uuidV4(),
			Queue:         cfg.queue,
			Lang:          SourceLang,
			SchemaVersion: SchemaVersion,
			CreatedAt:     time.Now().UnixMilli(),
		},
		Attempts: 0,
	}, nil
}

// Encode renders the envelope as compact UTF-8 JSON with HTML escaping disabled
// (unescaped slashes and unicode) — the canonical wire form shared by every SDK.
// The envelope frame is identical across languages; key order within the Data map
// follows encoding/json (sorted), which is semantically the same JSON.
func (e Envelope) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(e); err != nil {
		return nil, err
	}
	// json.Encoder appends a trailing newline; drop it for an exact body.
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Decode parses a raw JSON body into an Envelope. It accepts "urn" as an inbound
// alias for "job" (resolving it into Job). It does not validate the contents —
// use Accepts for consumer-side validation.
func Decode(raw []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return Envelope{}, err
	}
	if e.Job == "" {
		var alias struct {
			URN string `json:"urn"`
		}
		if json.Unmarshal(raw, &alias) == nil {
			e.Job = strings.TrimSpace(alias.URN)
		}
	}
	return e, nil
}

// URN returns the message URN — the canonical job, with the urn alias already
// resolved by Decode.
func (e Envelope) URN() string { return e.Job }

// Accepts reports whether a consumer should accept this envelope. It rejects a
// missing URN, an unsupported meta.schema_version, a blank trace_id, or missing
// data — the consumer-side counterpart to the producer JSON Schema. (It accepts
// the urn alias, unlike the stricter producer schema.)
func (e Envelope) Accepts() bool {
	if e.Job == "" {
		return false
	}
	if e.Meta.SchemaVersion != SchemaVersion {
		return false
	}
	if e.Data == nil {
		return false
	}
	if strings.TrimSpace(e.TraceID) == "" {
		return false
	}
	return true
}
