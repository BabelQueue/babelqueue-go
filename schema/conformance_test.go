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
