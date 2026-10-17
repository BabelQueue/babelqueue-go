package babelqueue_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

type conformanceExpect struct {
	URN           string         `json:"urn"`
	Data          map[string]any `json:"data"`
	Attempts      int            `json:"attempts"`
	Lang          string         `json:"lang"`
	SchemaVersion int            `json:"schema_version"`
	DeadLetter    map[string]any `json:"dead_letter"`
}

type conformanceCase struct {
	Name   string            `json:"name"`
	File   string            `json:"file"`
	Valid  bool              `json:"valid"`
	Reason string            `json:"reason"`
	Expect conformanceExpect `json:"expect"`
}

type conformanceManifest struct {
	SchemaVersion int               `json:"schema_version"`
	Cases         []conformanceCase `json:"cases"`
}

// TestConformance runs the shared cross-SDK suite (vendored under
// testdata/conformance) against this core — the same fixtures every BabelQueue
// SDK must satisfy. Per-message fields (meta.id, trace_id, meta.created_at) are
// intrinsically unique and are checked for presence, not value.
func TestConformance(t *testing.T) {
	suite := filepath.Join("testdata", "conformance")

	raw, err := os.ReadFile(filepath.Join(suite, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m conformanceManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if m.SchemaVersion != babelqueue.SchemaVersion {
		t.Fatalf("manifest schema_version %d != core %d", m.SchemaVersion, babelqueue.SchemaVersion)
	}
	if len(m.Cases) == 0 {
		t.Fatal("manifest has no cases")
	}

	for _, c := range m.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(suite, c.File))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			env, derr := babelqueue.Decode(body)

			if !c.Valid {
				if derr == nil && env.Accepts() {
					t.Fatalf("invalid fixture must be rejected (%s)", c.Reason)
				}
				return
			}

			if derr != nil {
				t.Fatalf("valid fixture failed to decode: %v", derr)
			}
			if !env.Accepts() {
				t.Fatal("valid fixture must be accepted")
			}
			if env.URN() != c.Expect.URN {
				t.Errorf("urn = %q, want %q", env.URN(), c.Expect.URN)
			}
			if env.Attempts != c.Expect.Attempts {
				t.Errorf("attempts = %d, want %d", env.Attempts, c.Expect.Attempts)
			}
			if env.Meta.Lang != c.Expect.Lang {
				t.Errorf("lang = %q, want %q", env.Meta.Lang, c.Expect.Lang)
			}
			if env.Meta.SchemaVersion != c.Expect.SchemaVersion {
				t.Errorf("schema_version = %d, want %d", env.Meta.SchemaVersion, c.Expect.SchemaVersion)
			}
			if c.Expect.Data != nil && !reflect.DeepEqual(env.Data, c.Expect.Data) {
				t.Errorf("data = %#v, want %#v", env.Data, c.Expect.Data)
			}

			// per-message fields are unique but must be present
			if env.TraceID == "" || env.Meta.ID == "" || env.Meta.CreatedAt == 0 {
				t.Error("per-message fields (trace_id, meta.id, meta.created_at) must be present")
			}

			if c.Expect.DeadLetter != nil {
				if env.DeadLetter == nil {
					t.Fatal("expected a dead_letter block")
				}
				if want, ok := c.Expect.DeadLetter["reason"].(string); ok && env.DeadLetter.Reason != want {
					t.Errorf("dead_letter.reason = %q, want %q", env.DeadLetter.Reason, want)
				}
				if want, ok := c.Expect.DeadLetter["original_queue"].(string); ok && env.DeadLetter.OriginalQueue != want {
					t.Errorf("dead_letter.original_queue = %q, want %q", env.DeadLetter.OriginalQueue, want)
				}
			}
		})
	}
}

// loadManifestSection reads one top-level section of the vendored manifest. A
// missing section is a hard failure (never a skip): the suite and the runner
// must move together.
func loadManifestSection(t *testing.T, name string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "conformance", "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(raw, &sections); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	sec, ok := sections[name]
	if !ok {
		t.Fatalf("manifest has no %q section", name)
	}
	if err := json.Unmarshal(sec, into); err != nil {
		t.Fatalf("parse %q section: %v", name, err)
	}
}

func readFixture(t *testing.T, file string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "conformance", file))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return body
}

// jsonPointer resolves an RFC 6901 pointer against a generic JSON document.
func jsonPointer(doc any, ptr string) (any, bool) {
	if ptr == "" {
		return doc, true
	}
	if !strings.HasPrefix(ptr, "/") {
		return nil, false
	}
	cur := doc
	for _, tok := range strings.Split(ptr[1:], "/") {
		tok = strings.ReplaceAll(strings.ReplaceAll(tok, "~1", "/"), "~0", "~")
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[tok]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(tok)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false
			}
			cur = node[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

func parseGeneric(t *testing.T, b []byte) any {
	t.Helper()
	var doc any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("emitted bytes are not JSON: %v\n%s", err, b)
	}
	return doc
}

// pointerViolation returns a message when the value at ptr in doc does not
// deep-equal want, distinguishing JSON types ({} != [], true != 1); "" when it does.
func pointerViolation(doc any, ptr string, want any) string {
	got, ok := jsonPointer(doc, ptr)
	if !ok {
		return ptr + " missing after re-encode"
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Sprintf("%s = %#v, want %#v", ptr, got, want)
	}
	return ""
}

// checkPreservedAbsent evaluates expect_preserved and expect_absent_after_reencode
// against one parsed document and returns every violation.
func checkPreservedAbsent(doc any, preserved map[string]any, absent []string) []string {
	var v []string
	for _, ptr := range sortedPointers(preserved) {
		if msg := pointerViolation(doc, ptr, preserved[ptr]); msg != "" {
			v = append(v, msg)
		}
	}
	for _, ptr := range absent {
		if _, ok := jsonPointer(doc, ptr); ok {
			v = append(v, ptr+" present after re-encode, must be absent")
		}
	}
	return v
}

func sortedPointers(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type roundtripCase struct {
	Name                      string         `json:"name"`
	File                      string         `json:"file"`
	ExpectAttempts            int            `json:"expect_attempts"`
	ExpectPreserved           map[string]any `json:"expect_preserved"`
	ExpectAbsentAfterReencode []string       `json:"expect_absent_after_reencode"`
	ApplyDeadLetter           *struct {
		Reason        string `json:"reason"`
		OriginalQueue string `json:"original_queue"`
	} `json:"apply_dead_letter"`
	ExpectDeadLetter map[string]any `json:"expect_dead_letter"`
}

// checkRoundtrip runs one roundtrip case against body and returns the violations
// (empty = conformant): decode (accepted, no warnings) -> attempts+1 -> encode, or
// -> the SDK's own dead-letter helper when apply_dead_letter is set; every check is
// evaluated on that final document.
func checkRoundtrip(c roundtripCase, body []byte) []string {
	env, err := babelqueue.Decode(body)
	if err != nil {
		return []string{"decode: " + err.Error()}
	}
	var v []string
	if !env.Accepts() {
		v = append(v, "roundtrip fixture must be accepted")
	}
	if w := env.Warnings(); len(w) != 0 {
		v = append(v, fmt.Sprintf("decode must emit no warning, got %q", w))
	}
	env.Attempts++
	if c.ApplyDeadLetter != nil {
		env = babelqueue.Annotate(env, c.ApplyDeadLetter.Reason, c.ApplyDeadLetter.OriginalQueue,
			env.Attempts, errors.New("conformance runner failure"))
	}
	out, err := env.Encode()
	if err != nil {
		return append(v, "encode: "+err.Error())
	}
	var doc any
	if err := json.Unmarshal(out, &doc); err != nil {
		return append(v, "emitted bytes are not JSON: "+err.Error())
	}
	if got, _ := jsonPointer(doc, "/attempts"); got != float64(c.ExpectAttempts) {
		v = append(v, fmt.Sprintf("attempts = %v, want %d", got, c.ExpectAttempts))
	}
	v = append(v, checkPreservedAbsent(doc, c.ExpectPreserved, c.ExpectAbsentAfterReencode)...)
	if c.ApplyDeadLetter != nil && len(c.ExpectDeadLetter) > 0 {
		dl, ok := jsonPointer(doc, "/dead_letter")
		if !ok {
			return append(v, "dead_letter block missing")
		}
		for _, k := range sortedPointers(c.ExpectDeadLetter) {
			got, _ := dl.(map[string]any)[k]
			if !reflect.DeepEqual(got, c.ExpectDeadLetter[k]) {
				v = append(v, fmt.Sprintf("dead_letter.%s = %#v, want %#v", k, got, c.ExpectDeadLetter[k]))
			}
		}
	}
	return v
}

// TestConformanceRoundtrip runs the `roundtrip` section: decode -> attempts+1 ->
// encode (or dead-letter) must preserve every unknown key with the same JSON type
// and value, drop what the case says is absent, and emit no decode warning.
func TestConformanceRoundtrip(t *testing.T) {
	var sec struct {
		Cases []roundtripCase `json:"cases"`
	}
	loadManifestSection(t, "roundtrip", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("roundtrip section has no cases")
	}
	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			for _, msg := range checkRoundtrip(c, readFixture(t, c.File)) {
				t.Error(msg)
			}
		})
	}
}

// TestConformanceDataShape runs the `data_shape` section: data is always a JSON
// object — an empty map encodes as {} and an array is rejected on decode.
func TestConformanceDataShape(t *testing.T) {
	var sec struct {
		Cases []struct {
			Name                  string         `json:"name"`
			Mode                  string         `json:"mode"`
			URN                   string         `json:"urn"`
			Queue                 string         `json:"queue"`
			Data                  map[string]any `json:"data"`
			ExpectEncodedDataJSON string         `json:"expect_encoded_data_json"`
			File                  string         `json:"file"`
			Valid                 bool           `json:"valid"`
			Reason                string         `json:"reason"`
		} `json:"cases"`
	}
	loadManifestSection(t, "data_shape", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("data_shape section has no cases")
	}
	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			switch c.Mode {
			case "encode":
				env, err := babelqueue.Make(c.URN, c.Data, babelqueue.WithQueue(c.Queue))
				if err != nil {
					t.Fatalf("make: %v", err)
				}
				out, err := env.Encode()
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				var top map[string]json.RawMessage
				if err := json.Unmarshal(out, &top); err != nil {
					t.Fatalf("emitted bytes are not a JSON object: %v", err)
				}
				var compact bytes.Buffer
				if err := json.Compact(&compact, top["data"]); err != nil {
					t.Fatalf("compact data: %v", err)
				}
				if compact.String() != c.ExpectEncodedDataJSON {
					t.Errorf("data encoded as %s, want %s", compact.String(), c.ExpectEncodedDataJSON)
				}
			case "decode":
				env, err := babelqueue.Decode(readFixture(t, c.File))
				accepted := err == nil && env.Accepts()
				if accepted != c.Valid {
					t.Errorf("accepted = %v, want %v (%s)", accepted, c.Valid, c.Reason)
				}
			default:
				t.Fatalf("unknown data_shape mode %q", c.Mode)
			}
		})
	}
}

type forbiddenKeyCase struct {
	Name                      string         `json:"name"`
	File                      string         `json:"file"`
	ForbiddenKey              string         `json:"forbidden_key"`
	Expect                    string         `json:"expect"`
	ExpectAbsentAfterReencode []string       `json:"expect_absent_after_reencode"`
	ExpectPreserved           map[string]any `json:"expect_preserved"`
}

// checkForbiddenKey runs one forbidden_keys case against body and returns the
// violations: decode succeeds with a warning naming the key, and the re-encode
// keeps expect_preserved and drops expect_absent_after_reencode.
func checkForbiddenKey(c forbiddenKeyCase, body []byte) []string {
	if c.Expect != "warn" {
		return []string{fmt.Sprintf("unsupported expect %q", c.Expect)}
	}
	env, err := babelqueue.Decode(body)
	if err != nil {
		return []string{"decode must succeed (warn, not reject): " + err.Error()}
	}
	var v []string
	named := false
	for _, w := range env.Warnings() {
		if strings.Contains(w, c.ForbiddenKey) {
			named = true
		}
	}
	if !named {
		v = append(v, fmt.Sprintf("no warning names %s; warnings = %q", c.ForbiddenKey, env.Warnings()))
	}
	out, err := env.Encode()
	if err != nil {
		return append(v, "encode: "+err.Error())
	}
	var doc any
	if err := json.Unmarshal(out, &doc); err != nil {
		return append(v, "emitted bytes are not JSON: "+err.Error())
	}
	return append(v, checkPreservedAbsent(doc, c.ExpectPreserved, c.ExpectAbsentAfterReencode)...)
}

// TestConformanceForbiddenKeys runs the `forbidden_keys` section (K-15): decode
// succeeds with a warning naming the key, and re-encoding never emits it.
func TestConformanceForbiddenKeys(t *testing.T) {
	var sec struct {
		Cases []forbiddenKeyCase `json:"cases"`
	}
	loadManifestSection(t, "forbidden_keys", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("forbidden_keys section has no cases")
	}
	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			for _, msg := range checkForbiddenKey(c, readFixture(t, c.File)) {
				t.Error(msg)
			}
		})
	}
}

// TestConformanceRunnerReportsViolations proves the optional runner fields are
// actually evaluated: on synthetic cases that break each one the runner must report
// a violation (a runner that ignored the field would stay green on the same input).
func TestConformanceRunnerReportsViolations(t *testing.T) {
	const body = `{"job":"urn:acme:a","trace_id":"7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b",` +
		`"data":{"order_id":1},"meta":{"id":"f1e2d3c4-b5a6-4789-90ab-cdef01234567","queue":"orders",` +
		`"lang":"php","schema_version":1,"created_at":1749132727000},"attempts":0,"extra_top":1}`
	const withURN = `{"job":"urn:acme:a","urn":"urn:acme:a","trace_id":"7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b",` +
		`"data":{},"meta":{"id":"f1e2d3c4-b5a6-4789-90ab-cdef01234567","queue":"orders",` +
		`"lang":"php","schema_version":1,"created_at":1749132727000},"attempts":0}`

	t.Run("roundtrip baseline is clean", func(t *testing.T) {
		c := roundtripCase{ExpectAttempts: 1, ExpectPreserved: map[string]any{"/extra_top": float64(1)}}
		if v := checkRoundtrip(c, []byte(body)); len(v) != 0 {
			t.Fatalf("unexpected violations: %q", v)
		}
	})
	t.Run("roundtrip expect_absent_after_reencode", func(t *testing.T) {
		c := roundtripCase{ExpectAttempts: 1, ExpectAbsentAfterReencode: []string{"/extra_top"}}
		if v := checkRoundtrip(c, []byte(body)); len(v) != 1 || !strings.Contains(v[0], "/extra_top present") {
			t.Fatalf("want one /extra_top violation, got %q", v)
		}
	})
	t.Run("roundtrip expect_preserved wrong value", func(t *testing.T) {
		c := roundtripCase{ExpectAttempts: 1, ExpectPreserved: map[string]any{"/extra_top": float64(2)}}
		if v := checkRoundtrip(c, []byte(body)); len(v) != 1 || !strings.Contains(v[0], "/extra_top") {
			t.Fatalf("want one /extra_top violation, got %q", v)
		}
	})
	t.Run("roundtrip expect_dead_letter wrong value", func(t *testing.T) {
		c := roundtripCase{ExpectAttempts: 1, ExpectDeadLetter: map[string]any{"reason": "poison"}}
		c.ApplyDeadLetter = &struct {
			Reason        string `json:"reason"`
			OriginalQueue string `json:"original_queue"`
		}{"failed", "orders"}
		if v := checkRoundtrip(c, []byte(body)); len(v) != 1 || !strings.Contains(v[0], "dead_letter.reason") {
			t.Fatalf("want one dead_letter.reason violation, got %q", v)
		}
	})
	t.Run("roundtrip warning is a violation", func(t *testing.T) {
		if v := checkRoundtrip(roundtripCase{ExpectAttempts: 1}, []byte(withURN)); len(v) != 1 || !strings.Contains(v[0], "no warning") {
			t.Fatalf("want a decode-warning violation, got %q", v)
		}
	})
	t.Run("forbidden_keys expect_preserved", func(t *testing.T) {
		c := forbiddenKeyCase{Expect: "warn", ForbiddenKey: "/urn",
			ExpectAbsentAfterReencode: []string{"/urn"},
			ExpectPreserved:           map[string]any{"/job": "urn:acme:other"}}
		if v := checkForbiddenKey(c, []byte(withURN)); len(v) != 1 || !strings.Contains(v[0], "/job") {
			t.Fatalf("want one /job violation, got %q", v)
		}
	})
	t.Run("forbidden_keys expect_absent_after_reencode", func(t *testing.T) {
		c := forbiddenKeyCase{Expect: "warn", ForbiddenKey: "/urn", ExpectAbsentAfterReencode: []string{"/job"}}
		if v := checkForbiddenKey(c, []byte(withURN)); len(v) != 1 || !strings.Contains(v[0], "/job present") {
			t.Fatalf("want one /job violation, got %q", v)
		}
	})
	t.Run("forbidden_keys baseline is clean", func(t *testing.T) {
		c := forbiddenKeyCase{Expect: "warn", ForbiddenKey: "/urn",
			ExpectAbsentAfterReencode: []string{"/urn"},
			ExpectPreserved:           map[string]any{"/job": "urn:acme:a"}}
		if v := checkForbiddenKey(c, []byte(withURN)); len(v) != 0 {
			t.Fatalf("unexpected violations: %q", v)
		}
	})
}

// TestConformanceExtensionWrite runs the `extension_write` section: the producer
// meta write API lands each entry in meta, and the entries survive a decode ->
// attempts+1 -> encode retry hop.
func TestConformanceExtensionWrite(t *testing.T) {
	var sec struct {
		Cases []struct {
			Name           string         `json:"name"`
			URN            string         `json:"urn"`
			Queue          string         `json:"queue"`
			Data           map[string]any `json:"data"`
			Meta           map[string]any `json:"meta"`
			ExpectEnvelope map[string]any `json:"expect_envelope"`
		} `json:"cases"`
	}
	loadManifestSection(t, "extension_write", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("extension_write section has no cases")
	}
	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			opts := []babelqueue.Option{babelqueue.WithQueue(c.Queue)}
			for _, k := range sortedPointers(c.Meta) {
				opts = append(opts, babelqueue.WithMeta(k, c.Meta[k]))
			}
			env, err := babelqueue.Make(c.URN, c.Data, opts...)
			if err != nil {
				t.Fatalf("make: %v", err)
			}
			out, err := env.Encode()
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			doc := parseGeneric(t, out)
			for _, msg := range checkPreservedAbsent(doc, c.ExpectEnvelope, nil) {
				t.Errorf("first emit: %s", msg)
			}
			// Re-emit chain: decode, attempts+1, encode again.
			back, err := babelqueue.Decode(out)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			back.Attempts++
			out2, err := back.Encode()
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			doc2 := parseGeneric(t, out2)
			for _, msg := range checkPreservedAbsent(doc2, c.ExpectEnvelope, nil) {
				t.Errorf("after retry hop: %s", msg)
			}
			if got, _ := jsonPointer(doc2, "/attempts"); got != float64(1) {
				t.Errorf("attempts = %v, want 1", got)
			}
		})
	}
}

// TestConformanceEncodeReject runs the `encode_reject` section: a forbidden or
// core-colliding key MUST fail the producer/encode path with an error, and no
// envelope JSON may be produced.
func TestConformanceEncodeReject(t *testing.T) {
	var sec struct {
		Cases []struct {
			Name         string         `json:"name"`
			URN          string         `json:"urn"`
			Queue        string         `json:"queue"`
			Data         map[string]any `json:"data"`
			Meta         map[string]any `json:"meta"`
			MetaEntries  [][2]any       `json:"meta_entries"`
			ExtrasTop    map[string]any `json:"extras_top"`
			ExtrasMeta   map[string]any `json:"extras_meta"`
			OffendingKey string         `json:"offending_key"`
			Expect       string         `json:"expect"`
		} `json:"cases"`
	}
	loadManifestSection(t, "encode_reject", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("encode_reject section has no cases")
	}
	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			if c.Expect != "reject" {
				t.Fatalf("unsupported expect %q", c.Expect)
			}
			opts := []babelqueue.Option{babelqueue.WithQueue(c.Queue)}
			for _, k := range sortedPointers(c.Meta) {
				opts = append(opts, babelqueue.WithMeta(k, c.Meta[k]))
			}
			for _, e := range c.MetaEntries {
				opts = append(opts, babelqueue.WithMeta(e[0].(string), e[1]))
			}
			env, err := babelqueue.Make(c.URN, c.Data, opts...)
			var out []byte
			if err == nil {
				if len(c.ExtrasTop) > 0 || len(c.ExtrasMeta) > 0 {
					env = babelqueue.SetExtrasForTest(env, c.ExtrasTop, c.ExtrasMeta)
				}
				out, err = env.Encode()
			}
			if err == nil {
				t.Fatalf("expected a rejection for %s, got envelope %s", c.OffendingKey, out)
			}
			if len(out) != 0 {
				t.Errorf("rejected path produced bytes: %s", out)
			}
			if !strings.Contains(err.Error(), c.OffendingKey) {
				t.Errorf("error %q does not name %s", err, c.OffendingKey)
			}
		})
	}
}
