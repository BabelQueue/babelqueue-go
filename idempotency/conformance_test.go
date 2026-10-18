package idempotency_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"github.com/babelqueue/babelqueue-go/idempotency"
)

func loadIdempotencySection(t *testing.T, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "testdata", "conformance", "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(raw, &sections); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	sec, ok := sections["idempotency"]
	if !ok {
		t.Fatal("manifest has no \"idempotency\" section")
	}
	if err := json.Unmarshal(sec, into); err != nil {
		t.Fatalf("parse idempotency section: %v", err)
	}
}

// TestIdempotencyConformance runs the `idempotency` section (ADR-0022): the dedupe
// key is meta.id verbatim, and each sequence collapses at-least-once deliveries to
// the expected number of effects.
func TestIdempotencyConformance(t *testing.T) {
	var sec struct {
		DedupKey struct {
			EnvelopeFile string `json:"envelope_file"`
			ExpectedKey  string `json:"expected_key"`
		} `json:"dedup_key"`
		Sequences struct {
			Cases []struct {
				Name       string `json:"name"`
				Deliveries []struct {
					MetaID       string `json:"meta_id"`
					ForgetBefore bool   `json:"forget_before"`
					ExpectEffect string `json:"expect_effect"`
					Outcome      string `json:"outcome"`
				} `json:"deliveries"`
				ExpectedEffects int `json:"expected_effects"`
			} `json:"cases"`
		} `json:"sequences"`
	}
	loadIdempotencySection(t, &sec)

	t.Run("dedup_key", func(t *testing.T) {
		body, err := os.ReadFile(filepath.Join("..", "testdata", "conformance", sec.DedupKey.EnvelopeFile))
		if err != nil {
			t.Fatalf("read fixture: %v", err)
		}
		env, err := babelqueue.Decode(body)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		// The wrapper keys its seen-set on exactly meta.id: a redelivery of this
		// envelope must hit the key the suite pins.
		store := idempotency.NewInMemoryStore()
		handler := idempotency.Wrap(store, func(context.Context, babelqueue.Envelope) error { return nil })
		if err := handler(context.Background(), env); err != nil {
			t.Fatalf("handler: %v", err)
		}
		seen, _ := store.Seen(context.Background(), sec.DedupKey.ExpectedKey)
		if !seen {
			t.Errorf("seen-set is not keyed on meta.id %q", sec.DedupKey.ExpectedKey)
		}
		if env.Meta.ID != sec.DedupKey.ExpectedKey {
			t.Errorf("meta.id = %q, want %q", env.Meta.ID, sec.DedupKey.ExpectedKey)
		}
	})

	if len(sec.Sequences.Cases) == 0 {
		t.Fatal("idempotency section has no sequences")
	}
	t.Run("sequences", func(t *testing.T) {
		for _, c := range sec.Sequences.Cases {
			c := c
			t.Run(c.Name, func(t *testing.T) {
				ctx := context.Background()
				store := idempotency.NewInMemoryStore()
				effects := 0
				outcome := "ok"
				invoked := false
				handler := idempotency.Wrap(store, func(context.Context, babelqueue.Envelope) error {
					// An effect is every handler invocation: a throwing handler's
					// side-effect fired too (the id just stays unmarked).
					invoked = true
					effects++
					if outcome == "throw" {
						return errors.New("conformance handler failure")
					}
					return nil
				})
				for i, d := range c.Deliveries {
					if d.ForgetBefore {
						if err := store.Forget(ctx, d.MetaID); err != nil {
							t.Fatalf("delivery %d: forget: %v", i, err)
						}
					}
					outcome = d.Outcome
					invoked = false
					env := babelqueue.Envelope{}
					env.Meta.ID = d.MetaID
					err := handler(ctx, env)
					wantRun := d.ExpectEffect == "run"
					if invoked != wantRun {
						t.Errorf("delivery %d: handler invoked = %v, want %v", i, invoked, wantRun)
					}
					if wantRun && d.Outcome == "throw" && err == nil {
						t.Errorf("delivery %d: a throwing handler must propagate its error", i)
					}
					if (!wantRun || d.Outcome == "ok") && err != nil {
						t.Errorf("delivery %d: unexpected error %v", i, err)
					}
				}
				if effects != c.ExpectedEffects {
					t.Errorf("effects = %d, want %d", effects, c.ExpectedEffects)
				}
			})
		}
	})
}
