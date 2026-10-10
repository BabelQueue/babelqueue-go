package sqs

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
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
		SchemaVersionGate gateTable `json:"schema_version_gate"`
	} `json:"sqs"`
}

// gateTable is the manifest's schema_version_gate table (§3.7): each case supplies the
// bq-schema-version attribute value (or marks it absent) and the expected verdict.
type gateTable struct {
	Property string `json:"property"`
	Fixture  string `json:"fixture"`
	Cases    []struct {
		Value  *string `json:"value"`
		Absent bool    `json:"absent"`
		Expect string  `json:"expect"`
	} `json:"cases"`
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

	t.Run("schema_version_gate", func(t *testing.T) {
		gate := m.SQS.SchemaVersionGate
		if len(gate.Cases) == 0 {
			t.Fatal("manifest has no sqs.schema_version_gate cases")
		}
		body, err := os.ReadFile(filepath.Join(conformanceDir, gate.Fixture))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		for i, c := range gate.Cases {
			fake := newFakeSQS()
			tr := NewWithClient(fake, WithQueueURLPrefix("http://fake"))
			fake.seedWithVersion("http://fake/orders", string(body), c.Value, 1)
			if c.Absent {
				fake.visible["http://fake/orders"][0].MessageAttributes = nil
			}
			label := "absent"
			if !c.Absent && c.Value != nil {
				label = strconv.Quote(*c.Value)
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
