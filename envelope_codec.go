package babelqueue

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
)

// Known envelope keys. Anything else is an unknown key: forward-compatible data a
// consumer must tolerate and a re-emitting consumer must carry along unchanged.
var (
	knownTopLevelKeys = map[string]bool{
		"job": true, "trace_id": true, "data": true, "meta": true,
		"attempts": true, "dead_letter": true,
		"urn": true, // inbound alias for job — resolved by Decode, never re-emitted
	}
	knownMetaKeys = map[string]bool{
		"id": true, "queue": true, "lang": true, "schema_version": true, "created_at": true,
	}
	// Non-canonical keys (message-envelope.md §10). Decode warns and drops them;
	// Encode never emits them.
	forbiddenTopLevelKeys = map[string]bool{"timestamp": true}
	forbiddenMetaKeys     = map[string]bool{
		"max_retries": true, "attempts": true, "source": true, "ts": true,
	}
)

// envelopeFields mirrors Envelope's wire fields without its methods, so the
// custom (un)marshalers can delegate the known fields to encoding/json.
type envelopeFields struct {
	Job        string         `json:"job"`
	TraceID    string         `json:"trace_id"`
	Data       map[string]any `json:"data"`
	Meta       Meta           `json:"meta"`
	Attempts   int            `json:"attempts"`
	DeadLetter *DeadLetter    `json:"dead_letter,omitempty"`
}

// envelopeWire is the encode-side shape: meta is pre-rendered so its unknown
// keys can be appended after the known meta fields.
type envelopeWire struct {
	Job        string          `json:"job"`
	TraceID    string          `json:"trace_id"`
	Data       map[string]any  `json:"data"`
	Meta       json.RawMessage `json:"meta"`
	Attempts   int             `json:"attempts"`
	DeadLetter *DeadLetter     `json:"dead_letter,omitempty"`
}

// MarshalJSON renders the known fields in canonical order, then any unknown keys
// captured by Decode (sorted, verbatim). An envelope without unknown keys encodes
// byte-for-byte as the plain struct would.
func (e Envelope) MarshalJSON() ([]byte, error) {
	meta, err := marshalCompact(e.Meta)
	if err != nil {
		return nil, err
	}
	meta = appendRawFields(meta, e.metaExtras)

	body, err := marshalCompact(envelopeWire{
		Job:        e.Job,
		TraceID:    e.TraceID,
		Data:       e.Data,
		Meta:       meta,
		Attempts:   e.Attempts,
		DeadLetter: e.DeadLetter,
	})
	if err != nil {
		return nil, err
	}
	return appendRawFields(body, e.extras), nil
}

// UnmarshalJSON decodes the known fields and captures unknown top-level and meta
// keys for re-emission. Forbidden keys are recorded as warnings and dropped.
func (e *Envelope) UnmarshalJSON(raw []byte) error {
	var known envelopeFields
	if err := json.Unmarshal(raw, &known); err != nil {
		return err
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return err
	}

	*e = Envelope{
		Job:        known.Job,
		TraceID:    known.TraceID,
		Data:       known.Data,
		Meta:       known.Meta,
		Attempts:   known.Attempts,
		DeadLetter: known.DeadLetter,
	}
	e.extras, e.warnings = collectUnknown(top, knownTopLevelKeys, forbiddenTopLevelKeys, "/", e.warnings)

	if rawMeta, ok := top["meta"]; ok {
		var meta map[string]json.RawMessage
		// A null/non-object meta has no keys to carry; the typed decode above
		// already accepted or rejected it.
		if json.Unmarshal(rawMeta, &meta) == nil {
			e.metaExtras, e.warnings = collectUnknown(meta, knownMetaKeys, forbiddenMetaKeys, "/meta/", e.warnings)
		}
	}
	return nil
}

// collectUnknown returns the keys of obj that are neither known nor forbidden,
// and appends a warning naming each forbidden key present (as a JSON pointer).
func collectUnknown(obj map[string]json.RawMessage, known, forbidden map[string]bool, prefix string, warnings []string) (map[string]json.RawMessage, []string) {
	var extras map[string]json.RawMessage
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		// encoding/json matches struct fields case-insensitively, so a key that only
		// differs in case from a known field ("JOB") was already decoded into that
		// field and must not be carried as an extra too; forbidden keys fold the same way.
		switch {
		case hasKeyFold(known, k):
		case hasKeyFold(forbidden, k):
			warnings = append(warnings, "babelqueue: dropped forbidden non-canonical envelope key "+prefix+k+" (message-envelope.md §10)")
		default:
			if extras == nil {
				extras = make(map[string]json.RawMessage)
			}
			extras[k] = append(json.RawMessage(nil), obj[k]...)
		}
	}
	return extras, warnings
}

// hasKeyFold reports whether set contains k, ignoring case (exact match first).
func hasKeyFold(set map[string]bool, k string) bool {
	if set[k] {
		return true
	}
	for name := range set {
		if strings.EqualFold(name, k) {
			return true
		}
	}
	return false
}

// marshalCompact encodes v as compact JSON with HTML escaping disabled.
func marshalCompact(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// appendRawFields inserts fields (sorted by key) before the closing brace of the
// compact JSON object obj.
func appendRawFields(obj []byte, fields map[string]json.RawMessage) []byte {
	if len(fields) == 0 {
		return obj
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]byte, 0, len(obj)+64*len(keys))
	out = append(out, obj[:len(obj)-1]...) // drop the closing '}'
	empty := len(obj) == 2                 // "{}"
	for _, k := range keys {
		if !empty {
			out = append(out, ',')
		}
		empty = false
		name, _ := marshalCompact(k)
		out = append(out, name...)
		out = append(out, ':')
		out = append(out, fields[k]...)
	}
	return append(out, '}')
}
