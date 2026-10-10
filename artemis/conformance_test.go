package artemis

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Azure/go-amqp"
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
		SchemaVersionGate struct {
			Property string `json:"property"`
			Fixture  string `json:"fixture"`
			Cases    []struct {
				Value  *string `json:"value"`
				Absent bool    `json:"absent"`
				Expect string  `json:"expect"`
			} `json:"cases"`
		} `json:"schema_version_gate"`
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

	t.Run("schema_version_gate", func(t *testing.T) {
		gate := m.Artemis.SchemaVersionGate
		if len(gate.Cases) == 0 {
			t.Fatal("manifest has no artemis.schema_version_gate cases")
		}
		body, err := os.ReadFile(filepath.Join(conformanceDir, gate.Fixture))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		for i, c := range gate.Cases {
			msg := incoming(string(body), "", 0)
			label := "absent"
			if !c.Absent {
				if c.Value != nil {
					msg.ApplicationProperties = map[string]any{gate.Property: *c.Value}
					label = strconv.Quote(*c.Value)
				} else {
					// JSON null: the key is present with a null value; it must be treated as absent.
					msg.ApplicationProperties = map[string]any{gate.Property: nil}
					label = "null"
				}
			}
			receiver := &fakeReceiver{messages: []*amqp.Message{msg}}
			tr, err := New(context.Background(), WithClient(&fakeClient{sender: &fakeSender{}, receiver: receiver}))
			if err != nil {
				t.Fatal(err)
			}
			got, err := tr.Pop(context.Background(), "orders", 0)
			if err != nil || got == nil {
				t.Fatalf("case %d (%s): Pop: msg=%v err=%v", i, label, got, err)
			}
			rejected := got.RouteURN != ""
			if rejected != (c.Expect == "reject") {
				t.Errorf("case %d (%s): rejected=%v, want expect=%s", i, label, rejected, c.Expect)
			}
			if !rejected {
				if _, err := babelqueue.Decode([]byte(got.Body)); err != nil {
					t.Errorf("case %d (%s): decode: %v", i, label, err)
				}
			}
		}
	})
}
