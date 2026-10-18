package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	kafkago "github.com/segmentio/kafka-go"
)

// poisonTriggerSet is the cross-SDK table of bodies and whether each one is poison (an
// undecodable body). The same file is vendored in the Python, Java, .NET and Node Kafka tests.
type poisonTriggerCase struct {
	Name   string `json:"name"`
	Body   string `json:"body"`
	Poison bool   `json:"poison"`
}

// TestDecodeMatchesSharedPoisonTable pins the Go core decode to the shared poison table. The
// table classifies the type-level poison of an envelope (wrongly typed members), so a row that
// is poison must fail decode. A row that is not poison must decode, or fail only with
// ErrNoIdentity: ADR-0032 §6 makes a body without a valid job or urn a decode error on the
// consume path too, and most rows are type probes that carry no identity. The one row whose
// verdict §6 itself changes is listed in identityOverrides. Envelope keys are matched with
// their exact spelling, so a wrongly cased key (e.g. "JOB") is an unknown key that leaves the
// identity missing.
func TestDecodeMatchesSharedPoisonTable(t *testing.T) {
	raw, err := os.ReadFile("testdata/poison-trigger-set.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []poisonTriggerCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("empty poison table")
	}
	// A "job" that is not a string is ignored (ADR-0032 §6) and the last duplicate wins, so a
	// valid string job after a numeric one is a valid identity, no longer a type error.
	identityOverrides := map[string]bool{"duplicate job number then string": false}
	for _, c := range cases {
		_, derr := babelqueue.Decode([]byte(c.Body))
		if want, ok := identityOverrides[c.Name]; ok {
			if got := derr != nil; got != want {
				t.Errorf("%q (%s): poison = %v (decode error: %v), ADR-0032 §6 says %v",
					c.Name, c.Body, got, derr, want)
			}
			continue
		}
		if c.Poison && derr == nil {
			t.Errorf("%q (%s): decoded, table says poison", c.Name, c.Body)
		}
		if !c.Poison && derr != nil && !errors.Is(derr, babelqueue.ErrNoIdentity) {
			t.Errorf("%q (%s): decode error %v, table says not poison", c.Name, c.Body, derr)
		}
	}
}

// Every table row that decodes without a type error but names no handler (no valid job and no
// valid urn alias) is poison on the consume path, per ADR-0032 §6.
func TestIdentitylessBodyIsPoison(t *testing.T) {
	for _, body := range []string{`null`, `{}`, `{"job":null}`, `{"job":5}`, `{"urn":5}`, `{"JOB":"urn:x"}`, `{"job":"  ","urn":" "}`} {
		if _, err := babelqueue.Decode([]byte(body)); !errors.Is(err, babelqueue.ErrNoIdentity) {
			t.Errorf("%s: err = %v, want ErrNoIdentity", body, err)
		}
	}
}

// A wrongly cased "JOB" key is an unknown key: the decoded job is empty, so the envelope is not
// accepted, and the Kafka consumer dead-letters it (the unknown-URN path) instead of handling it.
func TestKafkaConsumerDeadLettersWronglyCasedJobKey(t *testing.T) {
	body := `{"JOB":"` + urn + `","trace_id":"t-1","data":{"order_id":1},"meta":{"id":"i-1","queue":"orders","lang":"go","schema_version":1,"created_at":1700000000000},"attempts":0}`

	env, err := babelqueue.Decode([]byte(body))
	if !errors.Is(err, babelqueue.ErrNoIdentity) {
		t.Fatalf("decode err = %v, want ErrNoIdentity for a wrongly cased key (ADR-0032 §6)", err)
	}
	if env.Job != "" || env.URN() != "" {
		t.Fatalf("job = %q, URN() = %q; want both empty for a wrongly cased key", env.Job, env.URN())
	}
	if env.Accepts() {
		t.Fatal("Accepts() = true; a body without an exact-case job must not be accepted")
	}

	log := &offsetLog{records: []kafkago.Message{{Topic: "orders", Partition: 0, Offset: 0, Value: []byte(body)}}}
	w := &dlqWriter{}
	tr, err := New(WithWriter(w), WithReaderFactory(func(string) Reader { return log.newReader() }),
		WithMaxWaitTime(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	handled := 0
	app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true),
		babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDeadLetter))
	app.Handle(urn, func(context.Context, babelqueue.Envelope) error { handled++; return nil })

	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if handled != 0 {
		t.Fatalf("handler ran %d times for a body without an exact-case job", handled)
	}
	if len(w.sent) != 1 || w.sent[0].Topic != "orders.dlq" {
		t.Fatalf("writes = %+v; want exactly one record on orders.dlq", w.sent)
	}
	if len(log.commits) != 1 || log.commits[0] != 0 {
		t.Fatalf("commits = %v; the dead-lettered record must be committed", log.commits)
	}
	out := string(w.sent[0].Value)
	if !strings.Contains(out, `"JOB":"`+urn+`"`) {
		t.Fatalf("DLQ record %s lost the wrongly cased key (unknown keys must be preserved)", out)
	}
	if !strings.Contains(out, `"reason":"poison"`) {
		t.Fatalf("DLQ record %s is not annotated with reason poison", out)
	}
}

// A body that does not decode at all is dead-lettered verbatim as a raw poison record, not
// routed through the unknown-URN path.
func TestKafkaConsumerDeadLettersUndecodableBodyAsRawPoison(t *testing.T) {
	body := `{"job":"` + urn + `","job":5}`
	log := &offsetLog{records: []kafkago.Message{{Topic: "orders", Partition: 0, Offset: 0, Value: []byte(body)}}}
	w := &dlqWriter{}
	tr, err := New(WithWriter(w), WithReaderFactory(func(string) Reader { return log.newReader() }),
		WithMaxWaitTime(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true))
	app.Handle(urn, func(context.Context, babelqueue.Envelope) error { return nil })

	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(w.sent) != 1 || w.sent[0].Topic != "orders.dlq" {
		t.Fatalf("writes = %+v; want exactly one record on orders.dlq", w.sent)
	}
	if !strings.Contains(string(w.sent[0].Value), `"reason":"poison"`) {
		t.Fatalf("DLQ record %s is not a poison record", w.sent[0].Value)
	}
}
