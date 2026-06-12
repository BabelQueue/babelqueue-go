package sqs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

// conformanceDir is the core's vendored copy of the canonical suite (synced from
// the conformance/ repo). The SQS-binding cases live under manifest.json → "sqs".
const conformanceDir = "../testdata/conformance"

type sqsManifest struct {
	SQS struct {
		AttributeProjection struct {
			EnvelopeFile      string                     `json:"envelope_file"`
			MessageAttributes map[string]goldenAttribute `json:"message_attributes"`
		} `json:"attribute_projection"`
		AttemptsReconciliation struct {
			Cases []reconcileCase `json:"cases"`
		} `json:"attempts_reconciliation"`
	} `json:"sqs"`
}

type goldenAttribute struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue"`
}

type reconcileCase struct {
	Name                    string  `json:"name"`
	BodyAttempts            int     `json:"body_attempts"`
	ApproximateReceiveCount *string `json:"approximate_receive_count"`
	ExpectedAttempts        int     `json:"expected_attempts"`
}

func TestSqsConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m sqsManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}

	t.Run("attribute_projection", func(t *testing.T) {
		body, err := os.ReadFile(filepath.Join(conformanceDir, m.SQS.AttributeProjection.EnvelopeFile))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		got := attributes(string(body))
		want := m.SQS.AttributeProjection.MessageAttributes
		if len(got) != len(want) {
			t.Fatalf("projected %d attributes, want %d", len(got), len(want))
		}
		for key, w := range want {
			g, ok := got[key]
			if !ok {
				t.Errorf("missing attribute %q", key)
				continue
			}
			if aws.ToString(g.DataType) != w.DataType || aws.ToString(g.StringValue) != w.StringValue {
				t.Errorf("%s = {%s,%q}, want {%s,%q}",
					key, aws.ToString(g.DataType), aws.ToString(g.StringValue), w.DataType, w.StringValue)
			}
		}
	})

	for _, c := range m.SQS.AttemptsReconciliation.Cases {
		t.Run("attempts/"+c.Name, func(t *testing.T) {
			env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
			env.Attempts = c.BodyAttempts
			body, _ := env.Encode()

			out := string(body)
			if c.ApproximateReceiveCount != nil { // absent count → no reconciliation
				out = reconcileAttempts(out, *c.ApproximateReceiveCount)
			}
			decoded, _ := babelqueue.Decode([]byte(out))
			if decoded.Attempts != c.ExpectedAttempts {
				t.Errorf("attempts = %d, want %d", decoded.Attempts, c.ExpectedAttempts)
			}
		})
	}
}
