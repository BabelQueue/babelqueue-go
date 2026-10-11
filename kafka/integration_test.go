package kafka

// Live-broker integration tests. They are skipped in the normal suite and run only against a
// real Kafka broker:
//
//	BQ_INTEGRATION=1 BQ_KAFKA_BROKERS=localhost:9092 go test ./... -run Integration -v

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	// CreateTopics returns once the controller accepted the request, not once every broker's
	// metadata (and the partition leader) is visible; a write issued in that window fails with
	// "[3] Unknown Topic Or Partition". Wait until each topic reports a partition with a leader.
	for _, topic := range topics {
		waitTopicReady(t, brokers[0], topic)
	}
}

// waitTopicReady blocks (bounded) until topic has at least one partition with an elected leader.
func waitTopicReady(t *testing.T, broker, topic string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := kafkago.Dial("tcp", broker)
		if err == nil {
			parts, perr := conn.ReadPartitions(topic)
			_ = conn.Close()
			if perr == nil && len(parts) > 0 && parts[0].Leader.ID >= 0 && parts[0].Leader.Host != "" {
				return
			}
			err = perr
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %s not ready within 30s: %v", topic, err)
		}
		time.Sleep(250 * time.Millisecond)
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

// isolatedWriter returns a Writer (the one the transport would build) on a private Transport.
// kafka-go caches topic metadata in the Transport for several seconds, and the package-level
// default Transport is shared by every Writer/Reader in the process: a metadata response cached
// by an earlier test (before this test's topics existed) makes a later writer fail with "[3]
// Unknown Topic Or Partition" - and the writer does not retry that partition lookup. A private
// Transport starts with a cold cache, so each test sees the topics it created itself.
func isolatedWriter(t *testing.T, brokers []string) *kafkago.Writer {
	t.Helper()
	w, closeWriter := newIsolatedWriter(brokers)
	t.Cleanup(closeWriter)
	return w
}

// newIsolatedWriter builds the writer on a private Transport with a short metadata TTL (so a
// cached "topic not found" answer cannot outlive a retry loop by seconds) and returns it with
// its close function.
func newIsolatedWriter(brokers []string) (*kafkago.Writer, func()) {
	tp := &kafkago.Transport{MetadataTTL: time.Second}
	w := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Balancer: &kafkago.LeastBytes{}, Transport: tp}
	return w, func() {
		_ = w.Close()
		tp.CloseIdleConnections()
	}
}

// isUnknownTopic reports whether err is "[3] Unknown Topic Or Partition", whether it came from
// the partition lookup (a bare kafkago.Error) or from the produce response (wrapped per message
// in kafkago.WriteErrors, which has no Unwrap).
func isUnknownTopic(err error) bool {
	if errors.Is(err, kafkago.UnknownTopicOrPartition) {
		return true
	}
	var we kafkago.WriteErrors
	if errors.As(err, &we) {
		for _, e := range we {
			if errors.Is(e, kafkago.UnknownTopicOrPartition) {
				return true
			}
		}
	}
	return false
}

// seedRecord writes msg to topic with an isolated writer, retrying a transient
// "Unknown Topic Or Partition" for a bounded time in case metadata is still propagating. Each
// attempt uses a fresh writer (cold metadata cache), so a negative answer cached by one attempt
// is never replayed to the next.
func seedRecord(t *testing.T, brokers []string, topic string, msg kafkago.Message) {
	t.Helper()
	msg.Topic = topic
	deadline := time.Now().Add(30 * time.Second)
	for {
		w, closeWriter := newIsolatedWriter(brokers)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := w.WriteMessages(ctx, msg)
		cancel()
		closeWriter()
		if err == nil {
			return
		}
		if !isUnknownTopic(err) || time.Now().After(deadline) {
			t.Fatalf("seed record: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
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

	tr, err := New(WithBrokers(brokers...), WithWriter(isolatedWriter(t, brokers)), WithGroupID("bq-it-group-"+suffix))
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
// directly, with no retry: the DLQ gets exactly one record, the incoming value verbatim
// (byte-identical, never decoded, no dead_letter block), no handler runs, and the source record is
// settled (its offset committed).
func TestIntegrationVersionGateRejectionReachesDLQ(t *testing.T) {
	brokers := integrationBrokers(t)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	queue := "bq-it-gate-" + suffix
	dlq := queue + ".dlq"
	group := "bq-it-group-" + suffix
	createTopics(t, brokers, queue, dlq)

	var rejected atomic.Value
	var rejections atomic.Int32
	tr, err := New(WithBrokers(brokers...), WithWriter(isolatedWriter(t, brokers)), WithGroupID(group),
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

	seedRecord(t, brokers, queue, kafkago.Message{
		Value:   []byte("future-format-" + suffix), // not JSON: cannot be decoded as a v1 envelope
		Headers: []kafkago.Header{{Key: "bq-job", Value: []byte(integrationURN)}, {Key: "bq-schema-version", Value: []byte("2")}},
	})
	startApp(t, app, queue)

	raw := readDLQ(t, brokers, dlq, 60*time.Second).Value
	// §6.7: the gate rejects without decoding, so the DLQ record is the incoming value verbatim -
	// byte-identical, no dead_letter block (the body is not even JSON).
	wantBody := "future-format-" + suffix
	if string(raw) != wantBody {
		t.Errorf("DLQ value = %q, want the incoming value %q byte-for-byte", raw, wantBody)
	}
	if bytes.Contains(raw, []byte("dead_letter")) {
		t.Errorf("DLQ value carries a dead_letter block: %q", raw)
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

// The default Writer (no WithWriter) runs on the module's own Transport with a 1s metadata TTL and
// RequireAll. A topic created right after a failed write to it must accept the next write within
// the TTL window instead of failing for kafka-go's default 6s, and a write to a fresh topic
// succeeds at once.
func TestIntegrationDefaultWriterFirstWriteToNewTopic(t *testing.T) {
	brokers := integrationBrokers(t)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)

	tr, err := New(WithBrokers(brokers...), WithGroupID("bq-it-group-"+suffix))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	body := sampleBody(t, 0)

	// First write to a topic created just before: no retry, no warm-up.
	fresh := "bq-it-fresh-" + suffix
	createTopics(t, brokers, fresh)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := tr.Publish(ctx, fresh, body); err != nil {
		t.Fatalf("first write to a new topic: %v", err)
	}
	t.Logf("first write to a new topic took %s", time.Since(start))

	// A write to a missing topic fails (and caches that answer); once the topic exists the same
	// transport must succeed within a few seconds, well under the 6s default metadata TTL.
	late := "bq-it-late-" + suffix
	if err := tr.Publish(ctx, late, body); err == nil {
		t.Skip("broker auto-created the topic; negative-metadata scenario not reproducible")
	}
	createTopics(t, brokers, late)
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := tr.Publish(ctx, late, body)
		if err == nil {
			break
		}
		if !isUnknownTopic(err) || time.Now().After(deadline) {
			t.Fatalf("write after the topic was created: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
