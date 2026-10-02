package schema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestPayloadConformance runs the shared cross-SDK payload-schema cases (ADR-0024) from the
// vendored conformance suite: every BabelQueue SDK's payload validator must agree with this
// one on each case's `valid` flag, so the hand-rolled subset validators cannot drift apart.
func TestPayloadConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "testdata", "conformance", "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance suite not present: %v", err)
	}

	var manifest struct {
		PayloadSchema *struct {
			Schema json.RawMessage `json:"schema"`
			Cases  []struct {
				Name  string         `json:"name"`
				Valid bool           `json:"valid"`
				Data  map[string]any `json:"data"`
			} `json:"cases"`
		} `json:"payload_schema"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.PayloadSchema == nil {
		t.Skip("manifest has no payload_schema section")
	}

	s, err := Parse(manifest.PayloadSchema.Schema)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	if len(manifest.PayloadSchema.Cases) == 0 {
		t.Fatal("payload_schema has no cases")
	}
	for _, c := range manifest.PayloadSchema.Cases {
		got := len(s.Validate(c.Data)) == 0
		if got != c.Valid {
			t.Errorf("case %q: got valid=%v, want %v", c.Name, got, c.Valid)
		}
	}
}

// TestPayloadUnicodeConformance runs the shared payload_schema_unicode cases: minLength counts
// Unicode code points — not UTF-8 bytes, UTF-16 code units or grapheme clusters — so every
// SDK's validator agrees on multi-byte (Turkish), astral (emoji) and combining-mark strings.
func TestPayloadUnicodeConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "testdata", "conformance", "manifest.json"))
	if err != nil {
		t.Fatalf("read vendored conformance manifest: %v", err)
	}

	var manifest struct {
		PayloadSchemaUnicode *struct {
			Schema json.RawMessage `json:"schema"`
			Cases  []struct {
				Name  string         `json:"name"`
				Valid bool           `json:"valid"`
				Data  map[string]any `json:"data"`
			} `json:"cases"`
		} `json:"payload_schema_unicode"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.PayloadSchemaUnicode == nil {
		t.Fatal("manifest has no payload_schema_unicode section")
	}

	s, err := Parse(manifest.PayloadSchemaUnicode.Schema)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	if len(manifest.PayloadSchemaUnicode.Cases) == 0 {
		t.Fatal("payload_schema_unicode has no cases")
	}
	for _, c := range manifest.PayloadSchemaUnicode.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			errs := s.Validate(c.Data)
			if got := len(errs) == 0; got != c.Valid {
				t.Errorf("got valid=%v, want %v (errors: %v)", got, c.Valid, errs)
			}
		})
	}
}
