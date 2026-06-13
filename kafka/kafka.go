// Package kafka is an Apache Kafka-backed [babelqueue.Transport] for the BabelQueue Go
// runtime, on the pure-Go [github.com/segmentio/kafka-go] (no CGo, no librdkafka). Producing
// writes the canonical envelope as the record value and projects the contract fields onto
// native Kafka record headers (bq-job = URN, bq-trace-id, bq-message-id, bq-schema-version,
// bq-source-lang, bq-attempts — all UTF-8 byte strings) with the record timestamp mirroring
// meta.created_at, so a Java/.NET/... peer routes on bq-job without parsing the body.
// Consuming is process-then-commit: FetchMessage reserves a record (no auto-commit), and Ack
// commits the offset only after the handler returns (at-least-once). Kafka has no native
// delivery count, so the bq-attempts header is the authoritative retry counter (the body's
// attempts is the fallback for non-BabelQueue producers); the App owns retry by republishing
// with attempts+1 and dead-letters to <queue>.dlq.
//
//	tr, _ := kafka.New(kafka.WithBrokers("localhost:9092"), kafka.WithGroupID("orders-workers"))
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// This binding implements §6 of the BabelQueue broker-bindings contract. The envelope is
// unchanged (schema_version stays 1); Apache Kafka is purely additive.
//
// Full spec: https://babelqueue.com
package kafka

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	kafkago "github.com/segmentio/kafka-go"
)

// Writer is the subset of *kafkago.Writer the transport uses; the concrete writer satisfies
// it, and a fake satisfies it in tests.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Reader is the subset of *kafkago.Reader the transport uses (manual commit — no auto-commit).
type Reader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Transport implements [babelqueue.Transport] over Apache Kafka. It is safe for concurrent
// Publish; run Pop/Ack from a single goroutine per queue (a Kafka consumer is single-threaded).
type Transport struct {
	writer    Writer
	newReader func(queue string) Reader
	maxWait   time.Duration

	mu      sync.Mutex
	readers map[string]Reader
}

// Option customizes [New].
type Option func(*config)

type config struct {
	brokers   []string
	groupID   string
	writer    Writer
	newReader func(queue string) Reader
	maxWait   time.Duration
}

// WithBrokers sets the bootstrap broker list (host:port).
func WithBrokers(brokers ...string) Option { return func(c *config) { c.brokers = brokers } }

// WithGroupID sets the consumer group used for offset commits (required to consume).
func WithGroupID(groupID string) Option { return func(c *config) { c.groupID = groupID } }

// WithWriter injects a [Writer] (a fake in tests), bypassing all Kafka wiring.
func WithWriter(w Writer) Option { return func(c *config) { c.writer = w } }

// WithReaderFactory injects a per-queue [Reader] factory (a fake in tests).
func WithReaderFactory(f func(queue string) Reader) Option {
	return func(c *config) { c.newReader = f }
}

// WithMaxWaitTime caps how long a single Pop blocks waiting for a record.
func WithMaxWaitTime(d time.Duration) Option { return func(c *config) { c.maxWait = d } }

// New builds a transport. Provide brokers + a group id ([WithBrokers] / [WithGroupID]), or
// inject a [Writer] and a reader factory ([WithWriter] / [WithReaderFactory]) for tests.
func New(opts ...Option) (*Transport, error) {
	c := &config{}
	for _, o := range opts {
		o(c)
	}

	t := &Transport{writer: c.writer, newReader: c.newReader, maxWait: c.maxWait, readers: make(map[string]Reader)}
	if t.writer != nil && t.newReader != nil {
		return t, nil
	}
	if len(c.brokers) == 0 {
		return nil, errors.New("kafka: provide WithBrokers (+ WithGroupID), or WithWriter + WithReaderFactory")
	}
	if t.writer == nil {
		t.writer = &kafkago.Writer{Addr: kafkago.TCP(c.brokers...), Balancer: &kafkago.LeastBytes{}}
	}
	if t.newReader == nil {
		if c.groupID == "" {
			return nil, errors.New("kafka: WithGroupID is required to consume")
		}
		brokers, groupID := c.brokers, c.groupID
		t.newReader = func(queue string) Reader {
			return kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, GroupID: groupID, Topic: queue})
		}
	}
	return t, nil
}

func (t *Transport) reader(queue string) Reader {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r, ok := t.readers[queue]; ok {
		return r
	}
	r := t.newReader(queue)
	t.readers[queue] = r
	return r
}

// Publish writes body to the queue topic with the §6 header projection.
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	return t.writer.WriteMessages(ctx, message(queue, body))
}

// Pop reserves the next record (FetchMessage, no auto-commit), bounded by timeout (and
// WithMaxWaitTime). It reconciles attempts from the authoritative bq-attempts header. Returns
// (nil, nil) when no record arrives before the deadline.
func (t *Transport) Pop(ctx context.Context, queue string, timeout time.Duration) (*babelqueue.ReceivedMessage, error) {
	wait := timeout
	if t.maxWait > 0 && (wait <= 0 || t.maxWait < wait) {
		wait = t.maxWait
	}
	rctx := ctx
	if wait > 0 {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(ctx, wait)
		defer cancel()
	}
	msg, err := t.reader(queue).FetchMessage(rctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, nil
		}
		return nil, err
	}
	body := reconcileAttempts(string(msg.Value), msg.Headers)
	return &babelqueue.ReceivedMessage{Body: body, Queue: queue, Handle: msg}, nil
}

// Ack commits the reserved record's offset (process-then-commit; at-least-once).
func (t *Transport) Ack(ctx context.Context, msg *babelqueue.ReceivedMessage) error {
	km, ok := msg.Handle.(kafkago.Message)
	if !ok {
		return nil
	}
	return t.reader(msg.Queue).CommitMessages(ctx, km)
}

// message builds a Kafka record from an encoded envelope: value = body, headers = the bq-
// projection, timestamp = meta.created_at.
func message(topic, body string) kafkago.Message {
	m := kafkago.Message{Topic: topic, Value: []byte(body)}
	env, err := babelqueue.Decode([]byte(body))
	if err != nil {
		return m
	}
	m.Headers = headers(env)
	if env.Meta.CreatedAt != 0 {
		m.Time = time.UnixMilli(env.Meta.CreatedAt)
	}
	return m
}

// headers projects the envelope's contract fields onto Kafka record headers (UTF-8 byte values).
func headers(env babelqueue.Envelope) []kafkago.Header {
	h := make([]kafkago.Header, 0, 6)
	add := func(key, value string) {
		if value != "" {
			h = append(h, kafkago.Header{Key: key, Value: []byte(value)})
		}
	}
	add("bq-job", env.Job)
	add("bq-trace-id", env.TraceID)
	add("bq-message-id", env.Meta.ID)
	if env.Meta.SchemaVersion != 0 {
		add("bq-schema-version", strconv.Itoa(env.Meta.SchemaVersion))
	}
	add("bq-source-lang", env.Meta.Lang)
	add("bq-attempts", strconv.Itoa(env.Attempts))
	return h
}

// reconcileAttempts sets the envelope's attempts to the authoritative bq-attempts header
// (falling back to the body's own attempts when the header is absent / unparseable — a
// non-BabelQueue producer).
func reconcileAttempts(body string, hdrs []kafkago.Header) string {
	env, err := babelqueue.Decode([]byte(body))
	if err != nil {
		return body
	}
	attempts := env.Attempts
	for _, h := range hdrs {
		if h.Key == "bq-attempts" {
			if n, perr := strconv.Atoi(string(h.Value)); perr == nil {
				attempts = n
			}
			break
		}
	}
	if attempts == env.Attempts {
		return body
	}
	env.Attempts = attempts
	if b, eerr := env.Encode(); eerr == nil {
		return string(b)
	}
	return body
}

var _ babelqueue.Transport = (*Transport)(nil)
