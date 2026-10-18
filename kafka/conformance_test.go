package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

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
		t.Run("attempts_reconciliation/"+c.Name, func(t *testing.T) {
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
			i, c := i, c
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				var hdrs []kafkago.Header
				label := "absent"
				if !c.Absent {
					if c.Value != nil {
						hdrs = []kafkago.Header{{Key: gate.Property, Value: []byte(*c.Value)}}
						label = strconv.Quote(*c.Value)
					} else {
						// JSON null: the header key is present with a null (nil) value; it must be treated as absent.
						hdrs = []kafkago.Header{{Key: gate.Property, Value: nil}}
						label = "null"
					}
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
			})
		}
	})
}

type backoffCase struct {
	Name           string          `json:"name"`
	WorkTopic      string          `json:"work_topic"`
	RetryTiersMs   []int64         `json:"retry_tiers_ms"`
	DelayMs        int64           `json:"delay_ms"`
	Attempts       int             `json:"attempts"`
	ReleaseDelayMs int64           `json:"release_delay_ms"`
	Expect         json.RawMessage `json:"expect"`
}

type backoffExpect struct {
	TierMs       int64             `json:"tier_ms"`
	BodyAttempts int               `json:"body_attempts"`
	Headers      map[string]string `json:"headers"`
}

func (c backoffCase) transport(t *testing.T) (*Transport, *fakeWriter) {
	t.Helper()
	w := &fakeWriter{}
	tiers := make([]time.Duration, len(c.RetryTiersMs))
	for i, ms := range c.RetryTiersMs {
		tiers[i] = time.Duration(ms) * time.Millisecond
	}
	tr, err := New(WithWriter(w), WithReaderFactory(func(string) Reader { return &fakeReader{} }), WithRetryTiers(tiers...))
	if err != nil {
		t.Fatal(err)
	}
	return tr, w
}

// check asserts the single written record against the expectation: the retry topic of the
// expected tier, the listed headers (UTF-8 decimal strings), and the body attempts when pinned.
func (c backoffCase) check(t *testing.T, err error, w *fakeWriter, bodyAttempts *int) {
	t.Helper()
	if string(c.Expect) == `"reject"` {
		if !errors.Is(err, ErrDelayUnsupported) {
			t.Fatalf("err = %v, want ErrDelayUnsupported", err)
		}
		if len(w.sent) != 0 {
			t.Fatalf("a rejected delay must not write, wrote %d", len(w.sent))
		}
		return
	}
	var want backoffExpect
	if err := json.Unmarshal(c.Expect, &want); err != nil {
		t.Fatalf("expect: %v", err)
	}
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(w.sent) != 1 {
		t.Fatalf("wrote %d records, want 1", len(w.sent))
	}
	rec := w.sent[0]
	tier := -1
	for i, ms := range c.RetryTiersMs {
		if ms == want.TierMs {
			tier = i + 1
		}
	}
	if tier < 0 {
		t.Fatalf("tier_ms %d is not one of the configured tiers %v", want.TierMs, c.RetryTiersMs)
	}
	if rec.Topic != RetryTopic(c.WorkTopic, tier) {
		t.Errorf("topic = %q, want %q", rec.Topic, RetryTopic(c.WorkTopic, tier))
	}
	got := map[string]string{}
	for _, h := range rec.Headers {
		got[h.Key] = string(h.Value)
	}
	for k, v := range want.Headers {
		if got[k] != v {
			t.Errorf("header %s = %q, want %q", k, got[k], v)
		}
	}
	if bodyAttempts != nil {
		env, derr := babelqueue.Decode(rec.Value)
		if derr != nil || env.Attempts != *bodyAttempts {
			t.Errorf("body attempts = %d (%v), want %d", env.Attempts, derr, *bodyAttempts)
		}
	}
}

// TestKafkaBackoffConformance locks broker-bindings.md §6.4 (smallest tier >= delay, bq-delay and
// bq-original-topic, reject above the largest tier or with no tiers) and §6.5 (retry republish:
// bq-attempts + 1, body attempts in sync).
//
// SCOPE: these cases prove only tier selection and the write to the retry topic (topic, headers,
// body attempts). They do NOT prove re-injection or redelivery to the work topic: the Go SDK has
// no retry-topic -> work-topic re-inject consumer yet, so that half of §6.4/§6.5 is untested here.
func TestKafkaBackoffConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m struct {
		Backoff struct {
			Cases          []backoffCase `json:"cases"`
			RetryRepublish struct {
				Cases []backoffCase `json:"cases"`
			} `json:"retry_republish"`
		} `json:"backoff"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}

	for _, c := range m.Backoff.Cases {
		t.Run(c.Name, func(t *testing.T) {
			env, err := babelqueue.Make("urn:babel:test:backoff", map[string]any{"n": 1})
			if err != nil {
				t.Fatal(err)
			}
			body, _ := env.Encode()
			tr, w := c.transport(t)
			err = tr.PublishDelayed(context.Background(), c.WorkTopic, string(body), time.Duration(c.DelayMs)*time.Millisecond)
			c.check(t, err, w, nil)
		})
	}

	t.Run("retry_republish", func(t *testing.T) {
		for _, c := range m.Backoff.RetryRepublish.Cases {
			t.Run(c.Name, func(t *testing.T) {
				env, err := babelqueue.Make("urn:babel:test:backoff", map[string]any{"n": 1})
				if err != nil {
					t.Fatal(err)
				}
				env.Attempts = c.Attempts
				body, _ := env.Encode()
				tr, w := c.transport(t)
				from := &babelqueue.ReceivedMessage{Body: string(body), Queue: c.WorkTopic}
				err = tr.PublishRetry(context.Background(), from, time.Duration(c.ReleaseDelayMs)*time.Millisecond)
				var want backoffExpect
				_ = json.Unmarshal(c.Expect, &want)
				c.check(t, err, w, &want.BodyAttempts)
			})
		}
	})
}
