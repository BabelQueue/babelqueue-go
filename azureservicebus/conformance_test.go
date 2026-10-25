package azureservicebus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
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
		SchemaVersionGate struct {
			Fixture string `json:"fixture"`
			Cases   []struct {
				Value     any    `json:"value"`
				ValueType string `json:"value_type"`
				Absent    bool   `json:"absent"`
				Expect    string `json:"expect"`
			} `json:"cases"`
		} `json:"schema_version_gate"`
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
		t.Run("attempts_reconciliation/"+c.Name, func(t *testing.T) {
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
		gate := m.ASB.SchemaVersionGate
		if len(gate.Cases) == 0 {
			t.Fatal("manifest has no asb.schema_version_gate cases")
		}
		body, err := os.ReadFile(filepath.Join(conformanceDir, gate.Fixture))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		for i, c := range gate.Cases {
			i, c := i, c
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				// A JSON number carries no width, so a native AMQP integer is exercised at every
				// width the SDK can hand over; a string or an absent property has exactly one form.
				if c.ValueType != "" && c.ValueType != "float" {
					t.Fatalf("case %d: unknown value_type %q (this reader only understands \"float\")", i, c.ValueType)
				}
				values := []any{c.Value}
				if f, ok := c.Value.(float64); ok && c.ValueType == "float" {
					// An explicit floating-point case (e.g. 1.0): hand over real floats, never integers.
					values = []any{f, float32(f)}
				} else if ok && f == float64(int64(f)) {
					values = []any{int(f), int32(f), int64(f), uint8(f), uint32(f), uint64(f)}
				}
				for _, v := range values {
					receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{gatedMessage(string(body), v, !c.Absent)}}
					tr, err := New(WithClient(&fakeClient{sender: &fakeSender{}, receiver: receiver}))
					if err != nil {
						t.Fatal(err)
					}
					msg, err := tr.Pop(context.Background(), "orders", 0)
					if err != nil || msg == nil {
						t.Fatalf("case %d (%#v): Pop: msg=%v err=%v", i, v, msg, err)
					}
					rejected := msg.RouteURN != ""
					if rejected != (c.Expect == "reject") {
						t.Errorf("case %d (%#v): rejected=%v, want expect=%s", i, v, rejected, c.Expect)
					}
					if !rejected {
						if _, err := babelqueue.Decode([]byte(msg.Body)); err != nil {
							t.Errorf("case %d (%#v): decode: %v", i, v, err)
						}
					}
				}
			})
		}
	})
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

type versionGateCase struct {
	Name         string `json:"name"`
	OnUnknownURN string `json:"on_unknown_urn"`
	DeadLetter   string `json:"dead_letter"`
	DLQWrite     string `json:"dlq_write"`
	BQAttempts   string `json:"bq_attempts"`
	Expect       struct {
		Outcome         string `json:"outcome"`
		Record          string `json:"record"`
		OutcomeNoWriter string `json:"outcome_no_writer"`
	} `json:"expect"`
}

// TestAsbVersionGateConformance runs the `version_gate_asb` section (ADR-0037 decision 1;
// broker-bindings §5.7 / §4.5): a version-gated delivery goes through the real transport and App.
// With dead-lettering off (and not the dead_letter strategy) the outcome is `native`: no Complete,
// no write, no retry - this transport dead-letters it natively (explicit DeadLetter into
// $DeadLetterQueue). `unsettled` is an Abandon. The Go runtime always has a writable DLQ target,
// so `outcome_no_writer` never applies here.
func TestAsbVersionGateConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m struct {
		Section struct {
			Cases          []versionGateCase `json:"cases"`
			IncomingBodies []struct {
				Name string `json:"name"`
				Body string `json:"body"`
			} `json:"incoming_bodies"`
		} `json:"version_gate_asb"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(m.Section.Cases) == 0 || len(m.Section.IncomingBodies) == 0 {
		t.Fatal("manifest has no version_gate_asb cases or incoming bodies")
	}

	for _, c := range m.Section.Cases {
		t.Run(c.Name, func(t *testing.T) {
			for _, in := range m.Section.IncomingBodies {
				t.Run(in.Name, func(t *testing.T) {
					rm := gatedMessage(in.Body, "2", true)
					if c.BQAttempts != "" {
						// The outcome must not depend on the attempts or the native delivery count.
						rm.DeliveryCount = 8
						rm.ApplicationProperties["bq-attempts"] = c.BQAttempts
					}
					receiver := &fakeReceiver{messages: []*azservicebus.ReceivedMessage{rm}}
					sender := &fakeSender{}
					if c.DLQWrite == "fails" {
						sender.err = errors.New("dlq unavailable")
					}
					tr, err := New(WithClient(&fakeClient{sender: sender, receiver: receiver}))
					if err != nil {
						t.Fatal(err)
					}
					opts := []babelqueue.AppOption{
						babelqueue.WithDefaultQueue("orders"), babelqueue.WithMaxAttempts(3),
						babelqueue.WithDeadLetter(c.DeadLetter == "on"), babelqueue.WithRewindPause(0),
					}
					if c.OnUnknownURN != "" {
						opts = append(opts, babelqueue.WithUnknownURNStrategy(c.OnUnknownURN))
					}
					reported := 0
					opts = append(opts, babelqueue.WithDeadLetterErrorHandler(func(context.Context, *babelqueue.ReceivedMessage, error) { reported++ }))
					app := babelqueue.NewApp(tr, opts...)
					handled := 0
					app.Handle("urn:babelqueue:order.created", func(context.Context, babelqueue.Envelope) error { handled++; return nil })
					if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
						t.Fatal(err)
					}
					if handled != 0 {
						t.Fatalf("a handler ran %d times for a version-gated delivery", handled)
					}
					switch c.Expect.Outcome {
					case "ack":
						if len(receiver.completed) != 1 || len(sender.sent) != 0 || len(receiver.deadLettered) != 0 {
							t.Fatalf("completed = %d, sent = %d, dead-lettered = %d; want one Complete and nothing else",
								len(receiver.completed), len(sender.sent), len(receiver.deadLettered))
						}
					case "dlq_raw":
						if len(sender.sent) != 1 || len(receiver.completed) != 1 || len(receiver.deadLettered) != 0 {
							t.Fatalf("sent = %d, completed = %d, dead-lettered = %d; want one DLQ write then one Complete",
								len(sender.sent), len(receiver.completed), len(receiver.deadLettered))
						}
						if c.Expect.Record == "incoming_body" && string(sender.sent[0].Body) != in.Body {
							t.Fatalf("DLQ record is not byte-identical to the incoming body:\n got: %q\nwant: %q", sender.sent[0].Body, in.Body)
						}
					case "unsettled":
						if len(receiver.completed) != 0 || len(receiver.deadLettered) != 0 || len(receiver.abandoned) != 1 || reported == 0 {
							t.Fatalf("completed = %d, dead-lettered = %d, abandoned = %d, reported = %d; want an Abandon and a reported failure only",
								len(receiver.completed), len(receiver.deadLettered), len(receiver.abandoned), reported)
						}
					case "native":
						if len(receiver.completed) != 0 || len(sender.sent) != 0 || len(receiver.deadLettered)+len(receiver.abandoned) != 1 {
							t.Fatalf("completed = %d, sent = %d, dead-lettered = %d, abandoned = %d; native means no Complete, no write and exactly one native settle",
								len(receiver.completed), len(sender.sent), len(receiver.deadLettered), len(receiver.abandoned))
						}
						if len(receiver.deadLettered) == 1 && receiver.dlReasons[0] != "unsupported schema version" {
							t.Fatalf("DeadLetterReason = %q, want %q", receiver.dlReasons[0], "unsupported schema version")
						}
					default:
						t.Fatalf("unhandled expected outcome %q", c.Expect.Outcome)
					}
				})
			}
		})
	}
}
