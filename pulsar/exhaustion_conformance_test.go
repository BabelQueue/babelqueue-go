package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	pulsarsdk "github.com/apache/pulsar-client-go/pulsar"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

// queueRecordingClient hands out one fake producer per queue, so a test can tell a retry (the
// source queue) from a dead-letter record (the DLQ queue).
type queueRecordingClient struct {
	mu        sync.Mutex
	producers map[string]*fakeProducer
	consumer  *fakeConsumer
}

func (c *queueRecordingClient) CreateProducer(queue string) (Producer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.producers == nil {
		c.producers = map[string]*fakeProducer{}
	}
	p, ok := c.producers[queue]
	if !ok {
		p = &fakeProducer{}
		c.producers[queue] = p
	}
	return p, nil
}

func (c *queueRecordingClient) Subscribe(string) (Consumer, error) { return c.consumer, nil }

func (c *queueRecordingClient) sent(queue string) []*pulsarsdk.ProducerMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.producers[queue]; ok {
		return p.sent
	}
	return nil
}

type exhaustionCase struct {
	Name            string  `json:"name"`
	Kind            string  `json:"kind"`
	BodyAttempts    int     `json:"body_attempts"`
	BqAttempts      *string `json:"bq_attempts"`
	RedeliveryCount uint32  `json:"redelivery_count"`
	MaxAttempts     *int    `json:"max_attempts"`
	DeadLetter      bool    `json:"dead_letter"`
	OnUnknownURN    string  `json:"on_unknown_urn"`
	Expect          struct {
		Outcome            string `json:"outcome"`
		Reason             string `json:"reason"`
		DeadLetterAttempts *int   `json:"dead_letter_attempts"`
	} `json:"expect"`
}

// poisonBodies are the bodies a `poison` case is run against: a body that is not JSON and an
// object with no valid job/urn identity (ADR-0036 decision 4). Both take the same route.
var poisonBodies = map[string]string{
	"not json":      "not-json",
	"identity-less": `{"data":{"x":1},"attempts":0}`,
}

// TestConformancePulsarExhaustion runs the cross-SDK `pulsar_exhaustion` section (ADR-0036
// decisions 1 and 2, broker-bindings.md section 5.5) against the real Pulsar [Transport] (fake
// client) and the core [babelqueue.App]: the attempts reconcile, the App decides, and the
// observable outcome is read off the broker fakes. `retry` is a re-publish on the source queue
// (acknowledged), `dlq` a record on `<queue>.dlq` (acknowledged), `drop` an acknowledgement with
// nothing published. This SDK has the `on_unknown_urn` strategy option, so those rows run too.
func TestConformancePulsarExhaustion(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Fatalf("vendored conformance manifest: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	var sec struct {
		Cases []exhaustionCase `json:"cases"`
	}
	if err := json.Unmarshal(m["pulsar_exhaustion"], &sec); err != nil {
		t.Fatalf("pulsar_exhaustion section: %v", err)
	}
	if len(sec.Cases) == 0 {
		t.Fatal("pulsar_exhaustion section has no cases")
	}

	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			switch c.Kind {
			case "failed", "unknown_urn":
				runExhaustionCase(t, c, bodyWith(t, urn, c.BodyAttempts))
			case "poison":
				for name, body := range poisonBodies {
					body := body
					t.Run(name, func(t *testing.T) { runExhaustionCase(t, c, body) })
				}
			default:
				t.Fatalf("unknown kind %q", c.Kind)
			}
		})
	}
}

func runExhaustionCase(t *testing.T, c exhaustionCase, body string) {
	t.Helper()
	props := map[string]string{}
	if c.BqAttempts != nil {
		props["bq-attempts"] = *c.BqAttempts
	}
	consumer := &fakeConsumer{messages: []pulsarsdk.Message{
		fakeMessage{payload: []byte(body), redeliveryCount: c.RedeliveryCount, properties: props},
	}}
	client := &queueRecordingClient{consumer: consumer}
	tr, err := New(WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	opts := []babelqueue.AppOption{babelqueue.WithDeadLetter(c.DeadLetter)}
	if c.MaxAttempts != nil {
		opts = append(opts, babelqueue.WithMaxAttempts(*c.MaxAttempts))
	}
	if c.OnUnknownURN != "" {
		opts = append(opts, babelqueue.WithUnknownURNStrategy(c.OnUnknownURN))
	}
	app := babelqueue.NewApp(tr, opts...)
	handled := 0
	if c.Kind == "failed" {
		app.Handle(urn, func(context.Context, babelqueue.Envelope) error {
			handled++
			return fmt.Errorf("handler failed")
		})
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}

	if c.Kind == "poison" && handled != 0 {
		t.Fatalf("handler ran for a poison body")
	}
	if len(consumer.nacked) != 0 {
		t.Fatalf("message was negatively acknowledged, want it settled")
	}
	retries, dlq := client.sent("orders"), client.sent("orders.dlq")
	if len(consumer.acked) != 1 {
		t.Fatalf("acked %d times (retries %d, dlq %d), want 1", len(consumer.acked), len(retries), len(dlq))
	}

	var outcome string
	switch {
	case len(retries) == 1 && len(dlq) == 0:
		outcome = "retry"
	case len(retries) == 0 && len(dlq) == 1:
		outcome = "dlq"
	case len(retries) == 0 && len(dlq) == 0:
		outcome = "drop"
	default:
		t.Fatalf("published %d retries and %d dead-letter records", len(retries), len(dlq))
	}
	if outcome != c.Expect.Outcome {
		t.Fatalf("outcome = %s, want %s", outcome, c.Expect.Outcome)
	}
	if outcome != "dlq" {
		return
	}

	var rec struct {
		DeadLetter struct {
			Reason   string `json:"reason"`
			Attempts int    `json:"attempts"`
		} `json:"dead_letter"`
	}
	if err := json.Unmarshal(dlq[0].Payload, &rec); err != nil {
		t.Fatalf("dead-letter record is not JSON: %v: %s", err, dlq[0].Payload)
	}
	if rec.DeadLetter.Reason != c.Expect.Reason {
		t.Errorf("dead_letter.reason = %q, want %q", rec.DeadLetter.Reason, c.Expect.Reason)
	}
	if c.Expect.DeadLetterAttempts != nil && rec.DeadLetter.Attempts != *c.Expect.DeadLetterAttempts {
		t.Errorf("dead_letter.attempts = %d, want %d", rec.DeadLetter.Attempts, *c.Expect.DeadLetterAttempts)
	}
}
