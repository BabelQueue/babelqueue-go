package kafka

// Live-broker integration tests. They are skipped in the normal suite and run only against a
// real Kafka broker:
//
//	BQ_INTEGRATION=1 BQ_KAFKA_BROKERS=localhost:9092 go test ./... -run Integration -v

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	kafkago "github.com/segmentio/kafka-go"
)

const integrationURN = "urn:babel:integration:kafka"

func integrationBrokers(t *testing.T) []string {
	t.Helper()
	if os.Getenv("BQ_INTEGRATION") != "1" {
		t.Skip("live-broker test: set BQ_INTEGRATION=1 and BQ_KAFKA_BROKERS to run")
	}
	raw := os.Getenv("BQ_KAFKA_BROKERS")
	if raw == "" {
		t.Skip("live-broker test: BQ_KAFKA_BROKERS is not set")
	}
	return strings.Split(raw, ",")
}

// createTopics creates the topics up front: the transport's writer does not auto-create them.
func createTopics(t *testing.T, brokers []string, topics ...string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("controller: %v", err)
	}
	cc, err := kafkago.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer cc.Close()
	cfgs := make([]kafkago.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		cfgs = append(cfgs, kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	if err := cc.CreateTopics(cfgs...); err != nil {
		t.Fatalf("create topics: %v", err)
	}
}

// readDLQ returns the first record on topic (read without a consumer group, from the start).
func readDLQ(t *testing.T, brokers []string, topic string, wait time.Duration) kafkago.Message {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer r.Close()
	if err := r.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatalf("set offset: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	m, err := r.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("no record on %s within %s: %v", topic, wait, err)
	}
	return m
}

func decodeObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
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

// Scenario 1: a failing handler is retried until MaxTries, then the record lands on
// <queue>.dlq with a dead_letter block.
func TestIntegrationHandlerErrorDeadLettersAfterMaxTries(t *testing.T) {
	brokers := integrationBrokers(t)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	queue := "bq-it-retry-" + suffix
	dlq := queue + ".dlq"
	createTopics(t, brokers, queue, dlq)

	tr, err := New(WithBrokers(brokers...), WithGroupID("bq-it-group-"+suffix))
	if err != nil {
		t.Fatal(err)
	}
	const maxTries = 3
	var calls atomic.Int32
	app := babelqueue.NewApp(tr,
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

	rec := decodeObject(t, readDLQ(t, brokers, dlq, 60*time.Second).Value)
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

// committedOffset returns the consumer group's committed offset on partition 0 of topic, or -1
// when nothing has been committed yet.
func committedOffset(t *testing.T, brokers []string, group, topic string) int64 {
	t.Helper()
	client := &kafkago.Client{Addr: kafkago.TCP(brokers...), Timeout: 10 * time.Second}
	resp, err := client.OffsetFetch(context.Background(), &kafkago.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: {0}},
	})
	if err != nil {
		t.Fatalf("offset fetch: %v", err)
	}
	for _, p := range resp.Topics[topic] {
		if p.Partition == 0 {
			return p.CommittedOffset
		}
	}
	return -1
}

// Scenario 2: a record whose bq-schema-version header is "2" is rejected by the §6.7 gate on the
// header alone, before any decode. The seeded body is deliberately not a v1 envelope (it is not
// even JSON), so the test proves the body is never needed. The Go App dead-letters a gated record
// directly, with no retry: the DLQ gets exactly one record, annotated "unknown_urn" (the record
// is rebuilt from what Decode could recover, which for this body is nothing, so the original body
// is not asserted), no handler runs, and the source record is settled (its offset committed).
func TestIntegrationVersionGateRejectionReachesDLQ(t *testing.T) {
	brokers := integrationBrokers(t)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	queue := "bq-it-gate-" + suffix
	dlq := queue + ".dlq"
	group := "bq-it-group-" + suffix
	createTopics(t, brokers, queue, dlq)

	var rejected atomic.Value
	var rejections atomic.Int32
	tr, err := New(WithBrokers(brokers...), WithGroupID(group),
		WithOnSchemaVersionRejected(func(version, _ string) {
			rejected.Store(version)
			rejections.Add(1)
		}))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue(queue),
		babelqueue.WithDeadLetter(true),
		babelqueue.WithPollTimeout(2*time.Second),
	)
	app.Handle(integrationURN, func(context.Context, babelqueue.Envelope) error {
		calls.Add(1)
		return nil
	})

	w := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: queue}
	defer w.Close()
	if err := w.WriteMessages(context.Background(), kafkago.Message{
		Value:   []byte("future-format-" + suffix), // not JSON: cannot be decoded as a v1 envelope
		Headers: []kafkago.Header{{Key: "bq-job", Value: []byte(integrationURN)}, {Key: "bq-schema-version", Value: []byte("2")}},
	}); err != nil {
		t.Fatalf("seed record: %v", err)
	}
	startApp(t, app, queue)

	raw := readDLQ(t, brokers, dlq, 60*time.Second).Value
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
		t.Errorf("dead_letter.attempts = %v, want 0 (a gated record is not retried)", dl["attempts"])
	}
	if msg, _ := dl["error"].(string); !strings.Contains(msg, "unsupported-schema-version:2") {
		t.Errorf("dead_letter.error = %v, want it to name the rejected version", dl["error"])
	}
	if calls.Load() != 0 {
		t.Errorf("handler ran %d times for a version-gated record", calls.Load())
	}
	if v, _ := rejected.Load().(string); v != "2" {
		t.Errorf("OnSchemaVersionRejected version = %q, want 2", v)
	}

	// Source record settled: its offset is committed and it is never redelivered (and so never
	// re-rejected or dead-lettered a second time).
	deadline := time.Now().Add(30 * time.Second)
	for committedOffset(t, brokers, group, queue) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Second)
	}
	if got := committedOffset(t, brokers, group, queue); got != 1 {
		t.Errorf("committed offset on %s = %d, want 1 (source record not settled)", queue, got)
	}
	time.Sleep(5 * time.Second)
	if n := rejections.Load(); n != 1 {
		t.Errorf("version gate rejected the record %d times, want exactly 1", n)
	}
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlq, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer r.Close()
	if err := r.SetOffset(1); err != nil {
		t.Fatalf("set offset: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if extra, err := r.ReadMessage(ctx); err == nil {
		t.Errorf("DLQ holds a second record: %s", extra.Value)
	}
}
