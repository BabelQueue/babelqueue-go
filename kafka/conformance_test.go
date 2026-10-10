package kafka

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
	kafkago "github.com/segmentio/kafka-go"
)

// conformanceDir is the core's vendored copy of the canonical suite (synced from the
// conformance/ repo). The Apache Kafka binding cases live under manifest.json -> "kafka".
const conformanceDir = "../testdata/conformance"

type kafkaManifest struct {
	Kafka struct {
		PropertyProjection struct {
			EnvelopeFile string            `json:"envelope_file"`
			Headers      map[string]string `json:"headers"`
		} `json:"property_projection"`
		AttemptsReconciliation struct {
			Cases []kafkaReconcileCase `json:"cases"`
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
	} `json:"kafka"`
}

type kafkaReconcileCase struct {
	Name             string `json:"name"`
	BodyAttempts     int    `json:"body_attempts"`
	HeaderAttempts   *int   `json:"header_attempts"`
	ExpectedAttempts int    `json:"expected_attempts"`
}

func TestKafkaConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m kafkaManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}

	t.Run("property_projection", func(t *testing.T) {
		body, err := os.ReadFile(filepath.Join(conformanceDir, m.Kafka.PropertyProjection.EnvelopeFile))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		env, _ := babelqueue.Decode(body)
		got := make(map[string]string)
		for _, h := range headers(env) {
			got[h.Key] = string(h.Value)
		}
		want := m.Kafka.PropertyProjection.Headers
		if len(got) != len(want) {
			t.Fatalf("projected %d headers, want %d", len(got), len(want))
		}
		for k, w := range want {
			if got[k] != w {
				t.Errorf("%s = %q, want %q", k, got[k], w)
			}
		}
	})

	for _, c := range m.Kafka.AttemptsReconciliation.Cases {
		t.Run("attempts/"+c.Name, func(t *testing.T) {
			env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
			env.Attempts = c.BodyAttempts
			body, _ := env.Encode()

			var hdrs []kafkago.Header
			if c.HeaderAttempts != nil {
				hdrs = []kafkago.Header{{Key: "bq-attempts", Value: []byte(strconv.Itoa(*c.HeaderAttempts))}}
			}

			out := reconcileAttempts(string(body), hdrs)
			decoded, _ := babelqueue.Decode([]byte(out))
			if decoded.Attempts != c.ExpectedAttempts {
				t.Errorf("attempts = %d, want %d", decoded.Attempts, c.ExpectedAttempts)
			}
		})
	}

	t.Run("schema_version_gate", func(t *testing.T) {
		gate := m.Kafka.SchemaVersionGate
		if len(gate.Cases) == 0 {
			t.Fatal("manifest has no kafka.schema_version_gate cases")
		}
		body, err := os.ReadFile(filepath.Join(conformanceDir, gate.Fixture))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		for i, c := range gate.Cases {
			var hdrs []kafkago.Header
			label := "absent"
			if !c.Absent && c.Value != nil {
				hdrs = []kafkago.Header{{Key: gate.Property, Value: []byte(*c.Value)}}
				label = strconv.Quote(*c.Value)
			}
			fr := &fakeReader{messages: []kafkago.Message{{Topic: "orders", Value: body, Headers: hdrs}}}
			tr, err := New(WithWriter(&fakeWriter{}), WithReaderFactory(func(string) Reader { return fr }))
			if err != nil {
				t.Fatal(err)
			}
			msg, err := tr.Pop(context.Background(), "orders", 0)
			if err != nil || msg == nil {
				t.Fatalf("case %d (%s): Pop: msg=%v err=%v", i, label, msg, err)
			}
			rejected := msg.RouteURN != ""
			if rejected != (c.Expect == "reject") {
				t.Errorf("case %d (%s): rejected=%v, want expect=%s", i, label, rejected, c.Expect)
			}
			if !rejected {
				if _, err := babelqueue.Decode([]byte(msg.Body)); err != nil {
					t.Errorf("case %d (%s): decode: %v", i, label, err)
				}
			}
		}
	})
}
