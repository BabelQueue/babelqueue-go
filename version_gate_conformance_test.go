package babelqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// This file runs the `version_gate_pulsar` and `version_gate_artemis` conformance sections
// (ADR-0037 decision 1; broker-bindings §5.7 "Version-gate settle rule"). Both bindings hand the
// version-gated delivery to the App through the reserved route (the transport does the gating,
// which the bindings' own schema_version_gate sections lock), so these rows run through the App
// over a recording transport that has no NativeSettler capability (Pulsar and Artemis keep
// "ack and log"). The sqs and asb rows run in their transport modules (sqs/, azureservicebus/).

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

type versionGateSection struct {
	Cases          []versionGateCase `json:"cases"`
	IncomingBodies []struct {
		Name string `json:"name"`
		Body string `json:"body"`
	} `json:"incoming_bodies"`
}

func loadVersionGateSection(t *testing.T, section string) versionGateSection {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "conformance", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	var sec versionGateSection
	if err := json.Unmarshal(m[section], &sec); err != nil {
		t.Fatalf("manifest %s: %v", section, err)
	}
	if len(sec.Cases) == 0 || len(sec.IncomingBodies) == 0 {
		t.Fatalf("manifest %s has no cases or incoming bodies", section)
	}
	return sec
}

// versionGateTransport hands the App one reserved-route delivery and records every publish, ack
// and rewind. failDLQ makes the dead-letter write fail.
type versionGateTransport struct {
	*babelqueue.InMemoryTransport
	next    *babelqueue.ReceivedMessage
	failDLQ bool
	queues  []string
	bodies  []string
	acks    int
}

func (v *versionGateTransport) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	m := v.next
	v.next = nil
	return m, nil
}

func (v *versionGateTransport) Publish(_ context.Context, queue, body string) error {
	if v.failDLQ && queue == "orders.dlq" {
		return errors.New("dlq unavailable")
	}
	v.queues = append(v.queues, queue)
	v.bodies = append(v.bodies, body)
	return nil
}

func (v *versionGateTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error {
	v.acks++
	return nil
}

func runVersionGateSection(t *testing.T, section string) {
	sec := loadVersionGateSection(t, section)
	const gateRoute = "babelqueue:unsupported-schema-version:2"
	for _, c := range sec.Cases {
		t.Run(c.Name, func(t *testing.T) {
			for _, in := range sec.IncomingBodies {
				t.Run(in.Name, func(t *testing.T) {
					tr := &versionGateTransport{
						InMemoryTransport: babelqueue.NewInMemoryTransport(),
						next:              &babelqueue.ReceivedMessage{Body: in.Body, Queue: "orders", RouteURN: gateRoute},
						failDLQ:           c.DLQWrite == "fails",
					}
					opts := []babelqueue.AppOption{babelqueue.WithMaxAttempts(3), babelqueue.WithDeadLetter(c.DeadLetter == "on")}
					if c.OnUnknownURN != "" {
						opts = append(opts, babelqueue.WithUnknownURNStrategy(c.OnUnknownURN))
					}
					if c.BQAttempts != "" {
						// The outcome must not depend on the attempts, the native delivery count
						// or the max-attempts setting: feed a high delivery count.
						tr.next.DeliveryCount = 8
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
					for _, q := range tr.queues {
						if q != "orders.dlq" {
							t.Fatalf("published to %q: a version-gated delivery is never retried or re-published", q)
						}
					}
					switch c.Expect.Outcome {
					case "ack":
						if tr.acks != 1 || len(tr.queues) != 0 {
							t.Fatalf("acks = %d, published = %v; want one ack and no write", tr.acks, tr.queues)
						}
					case "dlq_raw":
						if tr.acks != 1 || len(tr.queues) != 1 {
							t.Fatalf("acks = %d, published = %v; want one DLQ write then one ack", tr.acks, tr.queues)
						}
						if c.Expect.Record == "incoming_body" {
							if tr.bodies[0] != in.Body {
								t.Fatalf("DLQ record is not byte-identical to the incoming body:\n got: %q\nwant: %q", tr.bodies[0], in.Body)
							}
							if strings.Contains(tr.bodies[0], "dead_letter") && !strings.Contains(in.Body, "dead_letter") {
								t.Fatalf("DLQ record carries a dead_letter block: %q", tr.bodies[0])
							}
						}
					case "unsettled":
						if tr.acks != 0 || len(tr.queues) != 0 || reported == 0 {
							t.Fatalf("acks = %d, published = %v, reported = %d; want no ack, no write, a reported failure",
								tr.acks, tr.queues, reported)
						}
					default:
						t.Fatalf("unhandled expected outcome %q", c.Expect.Outcome)
					}
				})
			}
		})
	}
}

// TestVersionGatePulsarConformance runs the `version_gate_pulsar` section.
func TestVersionGatePulsarConformance(t *testing.T) { runVersionGateSection(t, "version_gate_pulsar") }

// TestVersionGateArtemisConformance runs the `version_gate_artemis` section.
func TestVersionGateArtemisConformance(t *testing.T) {
	runVersionGateSection(t, "version_gate_artemis")
}
