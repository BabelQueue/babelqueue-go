package babelqueue_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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

// TestConformanceRoundtrip runs the `roundtrip` section: decode -> attempts+1 ->
// encode must preserve every unknown key with the same JSON type and value.
func TestConformanceRoundtrip(t *testing.T) {
	var sec struct {
		Cases []struct {
			Name            string         `json:"name"`
			File            string         `json:"file"`
			ExpectAttempts  int            `json:"expect_attempts"`
			ExpectPreserved map[string]any `json:"expect_preserved"`
		} `json:"cases"`
	}
	loadManifestSection(t, "roundtrip", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("roundtrip section has no cases")
	}
	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			env, err := babelqueue.Decode(readFixture(t, c.File))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !env.Accepts() {
				t.Fatal("roundtrip fixture must be accepted")
			}
			env.Attempts++
			out, err := env.Encode()
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			doc := parseGeneric(t, out)
			if got, _ := jsonPointer(doc, "/attempts"); got != float64(c.ExpectAttempts) {
				t.Errorf("attempts = %v, want %d", got, c.ExpectAttempts)
			}
			for ptr, want := range c.ExpectPreserved {
				got, ok := jsonPointer(doc, ptr)
				if !ok {
					t.Errorf("%s missing after re-encode", ptr)
					continue
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("%s = %#v, want %#v", ptr, got, want)
				}
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

// TestConformanceForbiddenKeys runs the `forbidden_keys` section (K-15): decode
// succeeds with a warning naming the key, and re-encoding never emits it.
func TestConformanceForbiddenKeys(t *testing.T) {
	var sec struct {
		Cases []struct {
			Name                      string   `json:"name"`
			File                      string   `json:"file"`
			ForbiddenKey              string   `json:"forbidden_key"`
			Expect                    string   `json:"expect"`
			ExpectAbsentAfterReencode []string `json:"expect_absent_after_reencode"`
		} `json:"cases"`
	}
	loadManifestSection(t, "forbidden_keys", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("forbidden_keys section has no cases")
	}
	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			if c.Expect != "warn" {
				t.Fatalf("unsupported expect %q", c.Expect)
			}
			env, err := babelqueue.Decode(readFixture(t, c.File))
			if err != nil {
				t.Fatalf("decode must succeed (warn, not reject): %v", err)
			}
			named := false
			for _, w := range env.Warnings() {
				if strings.Contains(w, c.ForbiddenKey) {
					named = true
				}
			}
			if !named {
				t.Errorf("no warning names %s; warnings = %q", c.ForbiddenKey, env.Warnings())
			}
			out, err := env.Encode()
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			doc := parseGeneric(t, out)
			for _, ptr := range c.ExpectAbsentAfterReencode {
				if _, ok := jsonPointer(doc, ptr); ok {
					t.Errorf("%s re-emitted after decode; forbidden keys must be dropped", ptr)
				}
			}
		})
	}
}
