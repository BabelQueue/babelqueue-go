package kafka

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
	kafkago "github.com/segmentio/kafka-go"
)

// kafkaPoisonCase is a row of the conformance manifest's `kafka_poison` section.
type kafkaPoisonCase struct {
	Name   string `json:"name"`
	Body   string `json:"body"`
	Expect string `json:"expect"` // ok | no_identity | poison
}

// TestKafkaPoisonConformance runs the cross-SDK `kafka_poison` section (broker-bindings.md §6.5,
// ADR-0032 §6) against the Go core decoder and the Kafka consume path. `ok` bodies decode with a
// valid identity; `no_identity` bodies parse but carry no valid job/urn (ErrNoIdentity); `poison`
// bodies cannot be decoded at all. On Kafka the last two share one fate: the record is
// dead-lettered to `<topic>.dlq` with `reason: poison` and the handler never runs. Envelope keys
// are matched with their exact spelling and the last duplicate key wins.
func TestKafkaPoisonConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Fatalf("vendored conformance manifest: %v", err)
	}
	var m struct {
		KafkaPoison struct {
			Cases []kafkaPoisonCase `json:"cases"`
		} `json:"kafka_poison"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.KafkaPoison.Cases) == 0 {
		t.Fatal("kafka_poison has no cases")
	}
	for _, c := range m.KafkaPoison.Cases {
		t.Run(c.Name, func(t *testing.T) {
			_, derr := babelqueue.Decode([]byte(c.Body))
			switch c.Expect {
			case "ok":
				if derr != nil {
					t.Fatalf("decode error %v, expect ok", derr)
				}
				return
			case "no_identity":
				if !errors.Is(derr, babelqueue.ErrNoIdentity) {
					t.Fatalf("decode error %v, expect ErrNoIdentity", derr)
				}
			case "poison":
				if derr == nil || errors.Is(derr, babelqueue.ErrNoIdentity) {
					t.Fatalf("decode error %v, expect an undecodable body", derr)
				}
			default:
				t.Fatalf("unknown expect %q", c.Expect)
			}

			// Consume path: the record goes to <topic>.dlq as poison and is committed.
			log := &offsetLog{records: []kafkago.Message{{Topic: "orders", Value: []byte(c.Body)}}}
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
				t.Fatalf("handler ran %d times for a %s body", handled, c.Expect)
			}
			if len(w.sent) != 1 || w.sent[0].Topic != "orders.dlq" {
				t.Fatalf("writes = %+v; want exactly one record on orders.dlq", w.sent)
			}
			if !strings.Contains(string(w.sent[0].Value), `"reason":"poison"`) {
				t.Fatalf("DLQ record %s is not annotated with reason poison", w.sent[0].Value)
			}
			if len(log.commits) != 1 {
				t.Fatalf("commits = %v; the dead-lettered record must be committed", log.commits)
			}
		})
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
