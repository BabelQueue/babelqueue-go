package azureservicebus

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// conformanceDir is the core's vendored copy of the canonical suite (synced from the
// conformance/ repo). The Azure Service Bus binding cases live under manifest.json → "asb".
const conformanceDir = "../testdata/conformance"

type asbManifest struct {
	ASB struct {
		PropertyProjection struct {
			EnvelopeFile string `json:"envelope_file"`
			Message      struct {
				Subject       string `json:"subject"`
				CorrelationID string `json:"correlation_id"`
				MessageID     string `json:"message_id"`
				ContentType   string `json:"content_type"`
			} `json:"message"`
			ApplicationProperties map[string]any `json:"application_properties"`
		} `json:"property_projection"`
		AttemptsReconciliation struct {
			Cases []asbReconcileCase `json:"cases"`
		} `json:"attempts_reconciliation"`
	} `json:"asb"`
}

type asbReconcileCase struct {
	Name             string `json:"name"`
	BodyAttempts     int    `json:"body_attempts"`
	DeliveryCount    uint32 `json:"delivery_count"`
	ExpectedAttempts int    `json:"expected_attempts"`
}

func TestAsbConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m asbManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}

	t.Run("property_projection", func(t *testing.T) {
		body, err := os.ReadFile(filepath.Join(conformanceDir, m.ASB.PropertyProjection.EnvelopeFile))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		msg := message(string(body))
		want := m.ASB.PropertyProjection

		if got := derefStr(msg.Subject); got != want.Message.Subject {
			t.Errorf("Subject = %q, want %q", got, want.Message.Subject)
		}
		if got := derefStr(msg.CorrelationID); got != want.Message.CorrelationID {
			t.Errorf("CorrelationID = %q, want %q", got, want.Message.CorrelationID)
		}
		if got := derefStr(msg.MessageID); got != want.Message.MessageID {
			t.Errorf("MessageID = %q, want %q", got, want.Message.MessageID)
		}
		if got := derefStr(msg.ContentType); got != want.Message.ContentType {
			t.Errorf("ContentType = %q, want %q", got, want.Message.ContentType)
		}

		if len(msg.ApplicationProperties) != len(want.ApplicationProperties) {
			t.Fatalf("projected %d properties, want %d", len(msg.ApplicationProperties), len(want.ApplicationProperties))
		}
		for key, w := range want.ApplicationProperties {
			g, ok := msg.ApplicationProperties[key]
			if !ok {
				t.Errorf("missing application property %q", key)
				continue
			}
			if !sameValue(g, w) {
				t.Errorf("%s = %v, want %v", key, g, w)
			}
		}
	})

	for _, c := range m.ASB.AttemptsReconciliation.Cases {
		t.Run("attempts/"+c.Name, func(t *testing.T) {
			env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
			env.Attempts = c.BodyAttempts
			body, _ := env.Encode()

			out := reconcileAttempts(string(body), c.DeliveryCount)
			decoded, _ := babelqueue.Decode([]byte(out))
			if decoded.Attempts != c.ExpectedAttempts {
				t.Errorf("attempts = %d, want %d", decoded.Attempts, c.ExpectedAttempts)
			}
		})
	}
}

// sameValue compares a projected property (int/int64/string) to its golden counterpart
// (JSON numbers decode to float64), numerically when both are numbers.
func sameValue(a, b any) bool {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		return af == bf
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint32:
		return float64(n), true
	}
	return 0, false
}
