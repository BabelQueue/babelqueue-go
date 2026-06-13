package kafka

import (
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
}
