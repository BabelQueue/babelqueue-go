package babelqueue_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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
