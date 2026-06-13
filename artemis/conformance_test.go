package artemis

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// conformanceDir is the core's vendored copy of the canonical suite (synced from the
// conformance/ repo). The Apache ActiveMQ Artemis binding cases live under manifest.json ->
// "artemis".
const conformanceDir = "../testdata/conformance"

type artemisManifest struct {
	Artemis struct {
		PropertyProjection struct {
			EnvelopeFile  string            `json:"envelope_file"`
			JmsType       string            `json:"jms_type"`
			CorrelationID string            `json:"correlation_id"`
			Properties    map[string]string `json:"properties"`
		} `json:"property_projection"`
		AttemptsReconciliation struct {
			Cases []artemisReconcileCase `json:"cases"`
		} `json:"attempts_reconciliation"`
	} `json:"artemis"`
}

type artemisReconcileCase struct {
	Name             string `json:"name"`
	BodyAttempts     int    `json:"body_attempts"`
	DeliveryCount    uint32 `json:"delivery_count"`
	ExpectedAttempts int    `json:"expected_attempts"`
}

func TestArtemisConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m artemisManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}

	t.Run("property_projection", func(t *testing.T) {
		body, err := os.ReadFile(filepath.Join(conformanceDir, m.Artemis.PropertyProjection.EnvelopeFile))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		env, _ := babelqueue.Decode(body)

		got := applicationProperties(env)
		want := m.Artemis.PropertyProjection.Properties
		if len(got) != len(want) {
			t.Fatalf("projected %d properties, want %d", len(got), len(want))
		}
		for k, w := range want {
			if s, _ := got[k].(string); s != w {
				t.Errorf("%s = %q, want %q", k, got[k], w)
			}
		}

		msg := message(string(body))
		if jt, _ := msg.Annotations[jmsTypeKey].(string); jt != m.Artemis.PropertyProjection.JmsType {
			t.Errorf("x-opt-jms-type = %q, want %q", jt, m.Artemis.PropertyProjection.JmsType)
		}
		if cid, _ := msg.Properties.CorrelationID.(string); cid != m.Artemis.PropertyProjection.CorrelationID {
			t.Errorf("correlation-id = %q, want %q", cid, m.Artemis.PropertyProjection.CorrelationID)
		}
	})

	for _, c := range m.Artemis.AttemptsReconciliation.Cases {
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
