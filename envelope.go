package babelqueue

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Sentinel errors for the extension-key rules of ADR-0031. They are returned by
// [Make] (through [WithMeta]) and by [Envelope.Encode] / [Envelope.MarshalJSON];
// test with errors.Is. The wrapped message names the offending key as a JSON
// pointer (e.g. "/meta/ts").
var (
	// ErrReservedMetaKey: a meta entry collides with a core meta key (id, queue,
	// lang, schema_version, created_at). The core never overwrites its own keys.
	ErrReservedMetaKey = errors.New("babelqueue: meta key collides with a core meta key")
	// ErrForbiddenKey: a forbidden non-canonical key (message-envelope.md §10) was
	// supplied — through [WithMeta] or through extras carried by the envelope.
	ErrForbiddenKey = errors.New("babelqueue: forbidden non-canonical envelope key (message-envelope.md §10)")
	// ErrDuplicateMetaKey: [WithMeta] was given the same key more than once. A
	// repeat is rejected, never merged and never last-write-wins.
	ErrDuplicateMetaKey = errors.New("babelqueue: meta key supplied more than once")
	// ErrEmptyMetaKey: [WithMeta] was given an empty key.
	ErrEmptyMetaKey = errors.New("babelqueue: meta key must not be empty")
)

// coreMetaKeys are the keys of [Meta]; extension entries may not reuse them.
var coreMetaKeys = map[string]bool{
	"id": true, "queue": true, "lang": true, "schema_version": true, "created_at": true,
}

// isBlankASCII reports whether s is empty or only ASCII whitespace (space, \t, \n,
// \v, \f, \r) — the closed "blank" set of the conformance suite. It deliberately
// does not use strings.TrimSpace, which also trims Unicode spaces (GR-5).
func isBlankASCII(s string) bool { return strings.Trim(s, " \t\n\v\f\r") == "" }

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

	// extras / metaExtras hold top-level and meta keys this core does not know,
	// captured verbatim by Decode and re-emitted by Encode after the known fields
	// (message-envelope.md §4/§8: unknown keys are forward-compatible and must
	// survive retry, dead-lettering, redrive and relay). The meta extras live here
	// rather than on Meta so Meta stays a comparable value type. Forbidden keys
	// (§10) are never captured — see Warnings.
	extras     map[string]json.RawMessage
	metaExtras map[string]json.RawMessage
	warnings   []string
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
	meta    map[string]json.RawMessage
	metaErr error // first WithMeta violation; surfaced by Make
}

// WithQueue sets the logical queue name recorded in meta.queue (default "default").
func WithQueue(queue string) Option { return func(c *makeConfig) { c.queue = queue } }

// WithTraceID reuses an existing trace id (trace continuation) instead of minting
// a fresh one. A blank value is ignored.
func WithTraceID(traceID string) Option { return func(c *makeConfig) { c.traceID = traceID } }

// WithMeta adds one extension entry to the message's meta object, next to the core
// meta keys (ADR-0031 (b)). Each call supplies one flat meta key; a vendor block is
// supplied as the whole value of "ext" (snake_case, under meta.ext.<vendor>, is the
// recommended shape). The value is JSON-encoded immediately.
//
// The entry is rejected — [Make] returns the error — when the key is empty
// ([ErrEmptyMetaKey]), collides with a core meta key ([ErrReservedMetaKey]), is a
// forbidden key of message-envelope.md §10 ([ErrForbiddenKey]), was already given to
// an earlier WithMeta ([ErrDuplicateMetaKey]), or its value cannot be encoded as
// JSON. Key matching is exact and case-sensitive: "TS" is not "ts".
func WithMeta(key string, value any) Option {
	return func(c *makeConfig) {
		if c.metaErr != nil {
			return // the first violation wins
		}
		switch {
		case key == "":
			c.metaErr = ErrEmptyMetaKey
			return
		case coreMetaKeys[key]:
			c.metaErr = fmt.Errorf("%w: /meta/%s", ErrReservedMetaKey, key)
			return
		case forbiddenMetaKeys[key]:
			c.metaErr = fmt.Errorf("%w: /meta/%s", ErrForbiddenKey, key)
			return
		}
		if _, dup := c.meta[key]; dup {
			c.metaErr = fmt.Errorf("%w: /meta/%s", ErrDuplicateMetaKey, key)
			return
		}
		raw, err := marshalCompact(value)
		if err != nil {
			c.metaErr = fmt.Errorf("babelqueue: meta key /meta/%s: %w", key, err)
			return
		}
		if c.meta == nil {
			c.meta = make(map[string]json.RawMessage)
		}
		c.meta[key] = raw
	}
}

// Make builds the canonical envelope for a (urn, data) pair. It mints a fresh
// trace id unless WithTraceID is given, starts attempts at 0, and stamps meta
// with a unique id, the source language ("go"), the schema version and a
// millisecond timestamp. It returns ErrEmptyURN when urn is blank, and the
// [WithMeta] error when an extension entry is rejected.
func Make(urn string, data map[string]any, opts ...Option) (Envelope, error) {
	urn = strings.TrimSpace(urn)
	if urn == "" {
		return Envelope{}, ErrEmptyURN
	}

	cfg := makeConfig{queue: "default"}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.metaErr != nil {
		return Envelope{}, cfg.metaErr
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
		Attempts:   0,
		metaExtras: cfg.meta,
	}, nil
}

// Encode renders the envelope as compact UTF-8 JSON with HTML escaping disabled
// (unescaped slashes and unicode) — the canonical wire form shared by every SDK.
// The envelope frame is identical across languages; key order within the Data map
// follows encoding/json (sorted), which is semantically the same JSON. Unknown keys
// captured by [Decode] are written after the known fields of their object.
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
// alias for "job" (resolving it into Job) when "job" is missing, empty or only ASCII
// whitespace; when both are present "job" wins and "urn" is dropped with a warning. Unknown top-level and meta keys are
// kept and re-emitted by Encode; forbidden keys (message-envelope.md §10) are
// dropped with a warning (see [Envelope.Warnings]). It does not validate the
// contents — use Accepts for consumer-side validation. A non-object data block
// (e.g. a JSON array) is rejected with an error.
//
// Envelope keys are matched by exact, case-sensitive spelling, like the canonical
// schema and every other SDK core: a wrongly cased key ("JOB", "Data", "META",
// "URN") is an unknown key — preserved and re-emitted, never read as the canonical
// field — so {"JOB":"urn:x"} decodes to an empty Job and fails [Accepts]. A key
// that occurs more than once resolves to its last occurrence (data is replaced,
// never merged).
func Decode(raw []byte) (Envelope, error) {
	e, alias, err := decodeEnvelope(raw)
	if err != nil {
		return Envelope{}, err
	}
	// A missing, empty or ASCII-blank job counts as absent: the URN then comes from
	// the "urn" alias, and the alias key itself is never kept (ADR-0031 (c)). A job
	// that is not blank wins; decodeEnvelope has already warned about a dropped urn.
	if isBlankASCII(e.Job) {
		if urn := strings.TrimSpace(alias); urn != "" {
			e.Job = urn
		}
	}
	return e, nil
}

// Warnings returns the non-fatal problems [Decode] found, such as a forbidden
// non-canonical key (message-envelope.md §10) that was dropped. Each warning names
// the offending key as a JSON pointer (e.g. "/meta/max_retries"). It is empty for a
// clean envelope and for envelopes built with [Make].
//
// Warnings only reflect the body as the consumer received it: a transport that
// reconciles attempts by re-encoding the body on redelivery (SQS, Kafka, Pulsar,
// Artemis, Azure Service Bus) has already dropped any forbidden key there, so a
// redelivered message may carry no warning even though its first delivery did.
func (e Envelope) Warnings() []string {
	if len(e.warnings) == 0 {
		return nil
	}
	return append([]string(nil), e.warnings...)
}

// URN returns the message URN — the canonical job, with the urn alias already
// resolved by Decode.
func (e Envelope) URN() string { return e.Job }

// Accepts reports whether a consumer should accept this envelope. It rejects a
// missing URN, an unsupported meta.schema_version, a blank trace_id, or missing
// data — the consumer-side counterpart to the producer JSON Schema. (It accepts
// the urn alias, unlike the stricter producer schema.)
func (e Envelope) Accepts() bool {
	if isBlankASCII(e.Job) {
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
