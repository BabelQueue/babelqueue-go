package artemis

// Live-broker integration tests. They are skipped in the normal suite and run only against a
// real Apache ActiveMQ Artemis broker that allows anonymous logins (the SDK dials without SASL
// credentials; the apache/activemq-artemis image needs ANONYMOUS_LOGIN=true):
//
//	BQ_INTEGRATION=1 BQ_ARTEMIS_URL=amqp://localhost:5672 go test ./... -run Integration -v
//
// Scenario 3 of the phase brief (a handler error raises the AMQP delivery-count) does not apply
// to this SDK: the Go App retries by re-publishing the envelope with attempts+1 and acking the
// original, so the broker's delivery-count is never what carries the retry. The attempts counter
// in the body is. The delivery-count paths are covered by the Java, .NET and Node suites.
//
// Artemis routes a plain AMQP address with its default routing type, which may be multicast. A
// multicast address drops messages published before a subscription exists, so every test attaches
// its receivers (the work queue and the DLQ) before anything is published.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

const integrationURN = "urn:babel:integration:artemis"

func integrationURL(t *testing.T) string {
	t.Helper()
	if os.Getenv("BQ_INTEGRATION") != "1" {
		t.Skip("live-broker test: set BQ_INTEGRATION=1 and BQ_ARTEMIS_URL to run")
	}
	url := os.Getenv("BQ_ARTEMIS_URL")
	if url == "" {
		t.Skip("live-broker test: BQ_ARTEMIS_URL is not set")
	}
	return url
}

func newTransport(t *testing.T, url string) *Transport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tr, err := New(ctx, WithURL(url))
	if err != nil {
		t.Fatalf("connect to Artemis: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

// attach opens the receiver link for queue by doing one short, empty Pop.
func attach(t *testing.T, tr *Transport, queue string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	msg, err := tr.Pop(ctx, queue, time.Second)
	if err != nil {
		t.Fatalf("attach receiver on %s: %v", queue, err)
	}
	if msg != nil {
		t.Fatalf("queue %s is not empty", queue)
	}
}

// popWithin returns the next message on queue and accepts (settles) it.
func popWithin(t *testing.T, tr *Transport, queue string, wait time.Duration) *babelqueue.ReceivedMessage {
	t.Helper()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		msg, err := tr.Pop(context.Background(), queue, 2*time.Second)
		if err != nil {
			t.Fatalf("pop %s: %v", queue, err)
		}
		if msg != nil {
			// Settle it: an unaccepted delivery stays in flight on the broker and would be
			// redelivered to a later receiver, which would make a "nothing else arrived" check lie.
			if err := tr.Ack(context.Background(), msg); err != nil {
				t.Fatalf("ack %s: %v", queue, err)
			}
			return msg
		}
	}
	t.Fatalf("no message on %s within %s", queue, wait)
	return nil
}

func decodeObject(t *testing.T, raw string) map[string]any {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		t.Fatalf("DLQ record is not a JSON object: %v: %s", err, raw)
	}
	return obj
}

func startApp(t *testing.T, app *babelqueue.App, queue string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = app.Consume(ctx, queue)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	})
}

// Scenario 1: a failing handler is retried until MaxTries, then the message lands on <queue>.dlq
// with a dead_letter block.
func TestIntegrationHandlerErrorDeadLettersAfterMaxTries(t *testing.T) {
	url := integrationURL(t)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	queue := "bq-it-retry-" + suffix
	dlq := queue + ".dlq"

	consumer := newTransport(t, url)
	dlqReader := newTransport(t, url)
	attach(t, consumer, queue)
	attach(t, dlqReader, dlq)

	const maxTries = 3
	var calls atomic.Int32
	app := babelqueue.NewApp(consumer,
		babelqueue.WithDefaultQueue(queue),
		babelqueue.WithMaxAttempts(maxTries),
		babelqueue.WithDeadLetter(true),
		babelqueue.WithPollTimeout(2*time.Second),
	)
	app.Handle(integrationURN, func(context.Context, babelqueue.Envelope) error {
		calls.Add(1)
		return fmt.Errorf("boom")
	})
	startApp(t, app, queue)

	if _, err := app.Publish(context.Background(), integrationURN, map[string]any{"marker": suffix}, babelqueue.WithQueue(queue)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	rec := decodeObject(t, popWithin(t, dlqReader, dlq, 60*time.Second).Body)
	dl, ok := rec["dead_letter"].(map[string]any)
	if !ok {
		t.Fatalf("DLQ record has no dead_letter block: %v", rec)
	}
	if dl["reason"] != "failed" {
		t.Errorf("dead_letter.reason = %v, want failed", dl["reason"])
	}
	if dl["original_queue"] != queue {
		t.Errorf("dead_letter.original_queue = %v, want %s", dl["original_queue"], queue)
	}
	if got := calls.Load(); got != maxTries {
		t.Errorf("handler calls = %d, want %d", got, maxTries)
	}
}

// Scenario 2: a message whose bq_schema_version property is "2" is rejected by the §7.7 gate on
// the property alone, before any decode. The seeded body is deliberately not a v1 envelope (it is
// not even JSON), so the test proves the body is never needed. The Go App dead-letters a gated
// message directly, with no retry: the DLQ gets exactly one record, annotated "unknown_urn" (the
// record is rebuilt from what Decode could recover, which for this body is nothing, so the
// original body is not asserted), no handler runs, and the source message is settled (accepted,
// so the broker never redelivers it and the gate never fires a second time).
func TestIntegrationVersionGateRejectionReachesDLQ(t *testing.T) {
	url := integrationURL(t)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	queue := "bq-it-gate-" + suffix
	dlq := queue + ".dlq"

	var rejected atomic.Value
	var rejections atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	consumer, err := New(ctx, WithURL(url), WithOnSchemaVersionRejected(func(version, _ string) {
		rejected.Store(version)
		rejections.Add(1)
	}))
	if err != nil {
		t.Fatalf("connect to Artemis: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })
	dlqReader := newTransport(t, url)
	attach(t, consumer, queue)
	attach(t, dlqReader, dlq)

	var calls atomic.Int32
	app := babelqueue.NewApp(consumer,
		babelqueue.WithDefaultQueue(queue),
		babelqueue.WithDeadLetter(true),
		babelqueue.WithPollTimeout(2*time.Second),
	)
	app.Handle(integrationURN, func(context.Context, babelqueue.Envelope) error {
		calls.Add(1)
		return nil
	})
	startApp(t, app, queue)

	conn, err := amqp.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial for seeding: %v", err)
	}
	defer conn.Close()
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := session.NewSender(ctx, queue, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(ctx, &amqp.Message{
		Value:                 "future-format-" + suffix, // not JSON: cannot be decoded as a v1 envelope
		ApplicationProperties: map[string]any{"bq_schema_version": "2"},
	}, nil); err != nil {
		t.Fatalf("seed message: %v", err)
	}

	raw := popWithin(t, dlqReader, dlq, 60*time.Second).Body
	rec := decodeObject(t, raw)
	dl, ok := rec["dead_letter"].(map[string]any)
	if !ok {
		t.Fatalf("DLQ record has no dead_letter block: %v", rec)
	}
	if dl["reason"] != "unknown_urn" {
		t.Errorf("dead_letter.reason = %v, want unknown_urn", dl["reason"])
	}
	if dl["original_queue"] != queue {
		t.Errorf("dead_letter.original_queue = %v, want %s", dl["original_queue"], queue)
	}
	if dl["lang"] != "go" {
		t.Errorf("dead_letter.lang = %v, want go", dl["lang"])
	}
	if attempts, _ := dl["attempts"].(float64); attempts != 0 {
		t.Errorf("dead_letter.attempts = %v, want 0 (a gated message is not retried)", dl["attempts"])
	}
	if msg, _ := dl["error"].(string); !strings.Contains(msg, "unsupported-schema-version:2") {
		t.Errorf("dead_letter.error = %v, want it to name the rejected version", dl["error"])
	}
	if calls.Load() != 0 {
		t.Errorf("handler ran %d times for a version-gated message", calls.Load())
	}
	if v, _ := rejected.Load().(string); v != "2" {
		t.Errorf("OnSchemaVersionRejected version = %q, want 2", v)
	}

	// Source message settled and DLQ holds exactly one record: wait out a redelivery window and
	// check nothing came back to the gate or landed on the DLQ again.
	time.Sleep(5 * time.Second)
	if n := rejections.Load(); n != 1 {
		t.Errorf("version gate rejected the message %d times, want exactly 1", n)
	}
	if extra, err := dlqReader.Pop(context.Background(), dlq, 3*time.Second); err != nil {
		t.Fatalf("pop %s: %v", dlq, err)
	} else if extra != nil {
		t.Errorf("DLQ holds a second record: %s", extra.Body)
	}
}
