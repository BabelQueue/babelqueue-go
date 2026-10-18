package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

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
		t.Run("attempts_reconciliation/"+c.Name, func(t *testing.T) {
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
			i, c := i, c
			t.Run(strconv.Itoa(i), func(t *testing.T) {
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
			})
		}
	})
}

type sqsDelayManifest struct {
	Delay struct {
		Cases []struct {
			Name         string          `json:"name"`
			QueueType    string          `json:"queue_type"`
			DelaySeconds int             `json:"delay_seconds"`
			Expect       json.RawMessage `json:"expect"`
		} `json:"cases"`
		ReleaseVisibility struct {
			Cases []struct {
				Name         string `json:"name"`
				DelaySeconds int    `json:"delay_seconds"`
				Expect       struct {
					VisibilityTimeout int32 `json:"visibility_timeout"`
				} `json:"expect"`
			} `json:"cases"`
		} `json:"release_visibility"`
	} `json:"delay"`
}

// TestSqsDelayConformance locks broker-bindings.md §3.4 (per-message DelaySeconds, 900 s cap,
// no FIFO per-message delay, never a silent truncate) and §3.5 (release visibility timeout).
func TestSqsDelayConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m sqsDelayManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}

	env, err := babelqueue.Make("urn:babel:test:delay", map[string]any{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	body, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range m.Delay.Cases {
		t.Run(c.Name, func(t *testing.T) {
			fake := newFakeSQS()
			opts := []Option{}
			if c.QueueType == "fifo" {
				opts = append(opts, WithFIFO(true))
			}
			tr := NewWithClient(fake, opts...)
			err := tr.PublishDelayed(context.Background(), "q", string(body), time.Duration(c.DelaySeconds)*time.Second)

			if string(c.Expect) == `"reject"` {
				if !errors.Is(err, ErrInvalidDelay) {
					t.Fatalf("err = %v, want ErrInvalidDelay", err)
				}
				if len(fake.sent) != 0 {
					t.Fatalf("a rejected delay must not send, sent %d", len(fake.sent))
				}
				return
			}
			var want struct {
				DelaySecondsAttribute int32 `json:"delay_seconds_attribute"`
			}
			if err := json.Unmarshal(c.Expect, &want); err != nil {
				t.Fatalf("expect: %v", err)
			}
			if err != nil {
				t.Fatalf("publish: %v", err)
			}
			if len(fake.sent) != 1 || fake.sent[0].DelaySeconds != want.DelaySecondsAttribute {
				t.Fatalf("sent = %v, want one message with DelaySeconds %d", fake.sent, want.DelaySecondsAttribute)
			}
		})
	}

	t.Run("release_visibility", func(t *testing.T) {
		for _, c := range m.Delay.ReleaseVisibility.Cases {
			t.Run(c.Name, func(t *testing.T) {
				fake := newFakeSQS()
				tr := NewWithClient(fake)
				msg := &babelqueue.ReceivedMessage{Queue: "q", Handle: "rh-1"}
				if err := tr.Release(context.Background(), msg, time.Duration(c.DelaySeconds)*time.Second); err != nil {
					t.Fatal(err)
				}
				if len(fake.visibility) != 1 || fake.visibility[0].VisibilityTimeout != c.Expect.VisibilityTimeout {
					t.Fatalf("visibility = %v, want timeout %d", fake.visibility, c.Expect.VisibilityTimeout)
				}
			})
		}
	})
}
