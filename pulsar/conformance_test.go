package pulsar

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// conformanceDir is the core's vendored copy of the canonical suite (synced from the
// conformance/ repo). The Apache Pulsar binding cases live under manifest.json -> "pulsar".
const conformanceDir = "../testdata/conformance"

type pulsarManifest struct {
	Pulsar struct {
		PropertyProjection struct {
			EnvelopeFile string            `json:"envelope_file"`
			Properties   map[string]string `json:"properties"`
		} `json:"property_projection"`
		AttemptsReconciliation struct {
			Cases []pulsarReconcileCase `json:"cases"`
		} `json:"attempts_reconciliation"`
		SchemaVersionGate struct {
			Property string `json:"property"`
			Cases    []struct {
				Value  *string `json:"value"`
				Absent bool    `json:"absent"`
				Expect string  `json:"expect"`
			} `json:"cases"`
		} `json:"schema_version_gate"`
	} `json:"pulsar"`
}

type pulsarReconcileCase struct {
	Name             string `json:"name"`
	BodyAttempts     int    `json:"body_attempts"`
	RedeliveryCount  uint32 `json:"redelivery_count"`
	ExpectedAttempts int    `json:"expected_attempts"`
}

func TestPulsarConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m pulsarManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}

	t.Run("property_projection", func(t *testing.T) {
		body, err := os.ReadFile(filepath.Join(conformanceDir, m.Pulsar.PropertyProjection.EnvelopeFile))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		got := message(string(body), "").Properties
		want := m.Pulsar.PropertyProjection.Properties
		if len(got) != len(want) {
			t.Fatalf("projected %d properties, want %d", len(got), len(want))
		}
		for k, w := range want {
			if got[k] != w {
				t.Errorf("%s = %q, want %q", k, got[k], w)
			}
		}
	})

	for _, c := range m.Pulsar.AttemptsReconciliation.Cases {
		t.Run("attempts/"+c.Name, func(t *testing.T) {
			env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
			env.Attempts = c.BodyAttempts
			body, _ := env.Encode()

			out, _ := reconcile(string(body), nil, c.RedeliveryCount, nil)
			decoded, _ := babelqueue.Decode([]byte(out))
			if decoded.Attempts != c.ExpectedAttempts {
				t.Errorf("attempts = %d, want %d", decoded.Attempts, c.ExpectedAttempts)
			}
		})
	}

	// The gate itself lives in pulsar.go; this reader pins its verdicts to the shared table.
	t.Run("schema_version_gate", func(t *testing.T) {
		gate := m.Pulsar.SchemaVersionGate
		if len(gate.Cases) == 0 {
			t.Fatal("manifest has no pulsar.schema_version_gate cases")
		}
		for i, c := range gate.Cases {
			props := map[string]string{}
			label := "absent"
			if !c.Absent && c.Value != nil {
				props[gate.Property] = *c.Value
				label = strconv.Quote(*c.Value)
			}
			_, rejected := unsupportedSchemaVersion(props)
			if rejected != (c.Expect == "reject") {
				t.Errorf("case %d (%s): rejected=%v, want expect=%s", i, label, rejected, c.Expect)
			}
		}
	})
}
