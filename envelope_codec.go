package babelqueue

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
)

// Non-canonical keys (message-envelope.md §10). Decode warns and drops them;
// Encode never emits them. Like every envelope key they are matched by exact,
// case-sensitive spelling: "Timestamp" is not "timestamp" but an ordinary unknown
// key, carried along unchanged.
var (
	forbiddenTopLevelKeys = map[string]bool{"timestamp": true}
	forbiddenMetaKeys     = map[string]bool{
		"max_retries": true, "attempts": true, "source": true, "ts": true,
	}
)

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
//
// Envelope keys are matched by exact, case-sensitive spelling, as the canonical
// schema and every other SDK core do: "JOB", "Data" or "META" are unknown keys,
// not the canonical fields. A key that occurs more than once resolves to its last
// occurrence (objects are replaced, never merged), though every occurrence is still
// type-checked. The "urn" inbound alias is resolved by [Decode], not here.
func (e *Envelope) UnmarshalJSON(raw []byte) error {
	env, _, err := decodeEnvelope(raw)
	if err != nil {
		return err
	}
	*e = env
	return nil
}

// decodeEnvelope is the single decode path behind [Decode] and
// [Envelope.UnmarshalJSON]. It also returns the value of the (exact-case) "urn"
// alias member, if any, for Decode to resolve.
func decodeEnvelope(raw []byte) (Envelope, string, error) {
	if !json.Valid(raw) {
		// Let encoding/json produce the precise syntax error.
		var skip json.RawMessage
		if err := json.Unmarshal(raw, &skip); err != nil {
			return Envelope{}, "", err
		}
		return Envelope{}, "", errInvalidJSON
	}
	if isJSONNull(raw) {
		return Envelope{}, "", nil
	}

	var (
		env               Envelope
		alias             string
		forbidden         = map[string]bool{}
		metaForbidden     = map[string]bool{}
		extras            map[string]json.RawMessage
		decodedMetaExtras map[string]json.RawMessage
	)
	err := forEachMember(raw, func(name string, value json.RawMessage) error {
		switch name {
		case "job":
			return assignValue(&env.Job, value)
		case "trace_id":
			return assignValue(&env.TraceID, value)
		case "data":
			return assignValue(&env.Data, value)
		case "attempts":
			return assignValue(&env.Attempts, value)
		case "meta":
			// The last meta block replaces any earlier one wholesale, including the
			// forbidden-key warnings of the blocks it displaces.
			env.Meta, decodedMetaExtras = Meta{}, nil
			metaForbidden = map[string]bool{}
			if isJSONNull(value) {
				return nil
			}
			meta, mExtras, mForbidden, err := decodeMeta(value)
			if err != nil {
				return err
			}
			env.Meta, decodedMetaExtras = meta, mExtras
			for _, k := range mForbidden {
				metaForbidden[k] = true
			}
			return nil
		case "dead_letter":
			env.DeadLetter = nil
			if isJSONNull(value) {
				return nil
			}
			dl, err := decodeDeadLetter(value)
			if err != nil {
				return err
			}
			env.DeadLetter = &dl
			return nil
		case "urn":
			// Inbound alias for job: only a string is an alias; any other type is ignored.
			var s string
			if json.Unmarshal(value, &s) != nil {
				s = ""
			}
			alias = s
			return nil
		default:
			if forbiddenTopLevelKeys[name] {
				forbidden[name] = true
				return nil
			}
			if extras == nil {
				extras = make(map[string]json.RawMessage)
			}
			extras[name] = value
			return nil
		}
	})
	if err != nil {
		return Envelope{}, "", err
	}

	env.extras = extras
	env.metaExtras = decodedMetaExtras
	env.warnings = forbiddenWarnings(forbidden, "/")
	env.warnings = append(env.warnings, forbiddenWarnings(metaForbidden, "/meta/")...)
	return env, alias, nil
}

// decodeMeta decodes one meta object by exact member name. It returns the typed
// fields, the unknown members to carry along, and the names of forbidden members
// it dropped.
func decodeMeta(raw []byte) (Meta, map[string]json.RawMessage, []string, error) {
	var (
		m         Meta
		extras    map[string]json.RawMessage
		forbidden []string
	)
	err := forEachMember(raw, func(name string, value json.RawMessage) error {
		switch name {
		case "id":
			return assignValue(&m.ID, value)
		case "queue":
			return assignValue(&m.Queue, value)
		case "lang":
			return assignValue(&m.Lang, value)
		case "schema_version":
			return assignValue(&m.SchemaVersion, value)
		case "created_at":
			return assignValue(&m.CreatedAt, value)
		default:
			if forbiddenMetaKeys[name] {
				forbidden = append(forbidden, name)
				return nil
			}
			if extras == nil {
				extras = make(map[string]json.RawMessage)
			}
			extras[name] = value
			return nil
		}
	})
	if err != nil {
		return Meta{}, nil, nil, err
	}
	return m, extras, forbidden, nil
}

// UnmarshalJSON decodes a meta object by exact, case-sensitive member name; the
// last occurrence of a duplicated member wins. Unknown members are ignored here —
// [Decode] is what carries them along on the envelope.
func (m *Meta) UnmarshalJSON(raw []byte) error {
	if isJSONNull(raw) {
		return nil
	}
	meta, _, _, err := decodeMeta(raw)
	if err != nil {
		return err
	}
	*m = meta
	return nil
}

// decodeDeadLetter decodes one dead_letter object by exact member name.
func decodeDeadLetter(raw []byte) (DeadLetter, error) {
	var d DeadLetter
	err := forEachMember(raw, func(name string, value json.RawMessage) error {
		switch name {
		case "reason":
			return assignValue(&d.Reason, value)
		case "error":
			return assignValue(&d.Error, value)
		case "exception":
			return assignValue(&d.Exception, value)
		case "failed_at":
			return assignValue(&d.FailedAt, value)
		case "original_queue":
			return assignValue(&d.OriginalQueue, value)
		case "attempts":
			return assignValue(&d.Attempts, value)
		case "lang":
			return assignValue(&d.Lang, value)
		}
		return nil // unknown dead_letter members are not carried
	})
	if err != nil {
		return DeadLetter{}, err
	}
	return d, nil
}

// UnmarshalJSON decodes a dead_letter object by exact, case-sensitive member name;
// the last occurrence of a duplicated member wins.
func (d *DeadLetter) UnmarshalJSON(raw []byte) error {
	if isJSONNull(raw) {
		return nil
	}
	dl, err := decodeDeadLetter(raw)
	if err != nil {
		return err
	}
	*d = dl
	return nil
}

var errInvalidJSON = errors.New("babelqueue: invalid JSON")

// forEachMember calls fn for every member of the JSON object in raw, in document
// order, with its exact (unescaped, case-sensitive) name and verbatim value.
// Duplicate names are all delivered. raw must already be valid JSON; a value that
// is not an object yields a *json.UnmarshalTypeError.
func forEachMember(raw []byte, fn func(name string, value json.RawMessage) error) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return &json.UnmarshalTypeError{Value: jsonKind(raw), Type: reflect.TypeOf(map[string]json.RawMessage(nil))}
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return err
		}
		name, _ := keyTok.(string)
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return err
		}
		if err := fn(name, value); err != nil {
			return err
		}
	}
	_, err = dec.Token() // closing brace
	return err
}

// assignValue decodes raw into a fresh value and stores it in *dst, so a later
// occurrence of a duplicated member fully replaces an earlier one: a JSON null
// resets dst to its zero value, and a map is never merged into the previous one.
func assignValue[T any](dst *T, raw json.RawMessage) error {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	*dst = v
	return nil
}

// forbiddenWarnings renders one warning per dropped forbidden key, sorted by name.
func forbiddenWarnings(keys map[string]bool, prefix string) []string {
	if len(keys) == 0 {
		return nil
	}
	names := make([]string, 0, len(keys))
	for k := range keys {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, k := range names {
		out = append(out, "babelqueue: dropped forbidden non-canonical envelope key "+prefix+k+" (message-envelope.md §10)")
	}
	return out
}

func isJSONNull(raw []byte) bool {
	return string(bytes.TrimSpace(raw)) == "null"
}

// jsonKind names the JSON type of the (valid) value in raw, for error messages.
func jsonKind(raw []byte) string {
	b := bytes.TrimSpace(raw)
	if len(b) == 0 {
		return "value"
	}
	switch b[0] {
	case '[':
		return "array"
	case '"':
		return "string"
	case 't', 'f':
		return "bool"
	case 'n':
		return "null"
	default:
		return "number"
	}
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
