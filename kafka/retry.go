package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// ErrDelayUnsupported is returned by [Transport.PublishDelayed] and [Transport.PublishRetry]
// when the request cannot be honoured: no retry tiers are configured ([WithRetryTiers]), or the
// requested delay exceeds the largest tier. Kafka has no native delayed delivery, so a delay is
// never silently dropped or clamped (broker-bindings.md §6.4).
var ErrDelayUnsupported = errors.New("kafka: delay needs a retry tier that covers it (§6.4)")

// DefaultRetryTiers is the RECOMMENDED retry tier list of broker-bindings.md §6.4 (5s, 30s, 5m,
// 30m), the Go counterpart of the contract's DEFAULT_RETRY_TIERS_MS constant. It is opt-in: no
// tiers are configured unless the caller passes it (or their own list) to [WithRetryTiers]. Use
// [DefaultRetryTiersMs] for the millisecond values.
var DefaultRetryTiers = []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute, 30 * time.Minute}

// DefaultRetryTiersMs is [DefaultRetryTiers] in milliseconds: [5000, 30000, 300000, 1800000].
var DefaultRetryTiersMs = []int64{5000, 30000, 300000, 1800000}

// WithRetryTiers configures the delay tiers of the retry-topic pattern (broker-bindings.md §6.4):
// tier n (1-based, ascending delay) is the topic `<topic>.retry.<n>`. Tiers are sorted; the set of
// retry topics must be declared on the broker by the operator. Without tiers, a delayed publish or
// a retry republish fails with [ErrDelayUnsupported]. Pass [DefaultRetryTiers] for the recommended
// list.
//
// This option enables the publish side. The retry topics are drained back into the work topic by
// the [Reinjector] (§6.4), which a deployment must run for the tiers it configures.
func WithRetryTiers(tiers ...time.Duration) Option {
	return func(c *config) {
		c.retryTiers = append([]time.Duration(nil), tiers...)
		sort.Slice(c.retryTiers, func(i, j int) bool { return c.retryTiers[i] < c.retryTiers[j] })
	}
}

// RetryTopic returns the retry topic name of the 1-based tier n for work topic topic.
func RetryTopic(topic string, n int) string { return topic + ".retry." + strconv.Itoa(n) }

// retryTopicFor picks the smallest tier >= delay and returns its topic.
func (t *Transport) retryTopicFor(topic string, delay time.Duration) (string, error) {
	if len(t.retryTiers) == 0 {
		return "", fmt.Errorf("%w: no retry tiers configured", ErrDelayUnsupported)
	}
	for i, tier := range t.retryTiers {
		if tier >= delay {
			return RetryTopic(topic, i+1), nil
		}
	}
	return "", fmt.Errorf("%w: %s exceeds the largest tier %s", ErrDelayUnsupported, delay, t.retryTiers[len(t.retryTiers)-1])
}

// retryTierForAttempt returns the 1-based tier n = min(attemptsBefore, N-1) + 1 of the N configured
// tiers for a record that already has attemptsBefore attempts, and that tier's delay (§6.5). Once
// the tiers run out the last one is kept.
func (t *Transport) retryTierForAttempt(attemptsBefore int) (n int, tier time.Duration, err error) {
	if len(t.retryTiers) == 0 {
		return 0, 0, fmt.Errorf("%w: no retry tiers configured", ErrDelayUnsupported)
	}
	if attemptsBefore < 0 {
		attemptsBefore = 0
	}
	idx := attemptsBefore
	if last := len(t.retryTiers) - 1; idx > last {
		idx = last
	}
	return idx + 1, t.retryTiers[idx], nil
}

// retryRecord builds the record routed via a retry topic: the §6 projection plus `bq-delay` (ms,
// the requested delay) and `bq-original-topic` (§6.3). The record timestamp is the publish time,
// not meta.created_at: the re-inject consumer starts the wait at `record.timestamp`, so a retry of
// an old message must not look as if its tier had already elapsed (on a LogAppendTime topic the
// broker overrides the producer timestamp with the same clock).
func retryRecord(retryTopic, original, body string, delay time.Duration) kafkago.Message {
	m := message(retryTopic, body)
	m.Time = time.Now()
	m.Headers = append(m.Headers,
		kafkago.Header{Key: "bq-delay", Value: []byte(strconv.FormatInt(delay.Milliseconds(), 10))},
		kafkago.Header{Key: "bq-original-topic", Value: []byte(original)},
	)
	return m
}

// PublishDelayed delays body on queue by publishing it to the smallest retry topic whose tier is at
// least delay, with `bq-delay` and `bq-original-topic` set (§6.4). It returns [ErrDelayUnsupported]
// when no tiers are configured or delay exceeds the largest tier; nothing is written then.
//
// This only writes to the retry topic. The message reaches the work topic again once a re-inject
// consumer ([Reinjector], or another SDK's) reads the retry topic, waits out the tier and writes
// the record back to `bq-original-topic`; without one it is never redelivered.
func (t *Transport) PublishDelayed(ctx context.Context, queue, body string, delay time.Duration) error {
	topic, err := t.retryTopicFor(queue, delay)
	if err != nil {
		return err
	}
	return t.writer.WriteMessages(ctx, retryRecord(topic, queue, body, delay))
}

// PublishRetry republishes the reserved record from to the smallest retry tier that covers delay.
// It carries the pre-1.x delay-to-tier mapping unchanged: the envelope's attempts and the
// `bq-attempts` header both become attempts + 1, `bq-original-topic` and `bq-delay` (= delay) are
// set, and the reserved record's key is carried over. The caller commits the original offset
// afterwards ([Transport.Ack]). It returns [ErrDelayUnsupported] when no tiers are configured or
// delay exceeds the largest tier.
//
// Deprecated: the retry target of broker-bindings.md §6.5 is chosen from the attempt count, not
// from a delay. Use [Transport.PublishRetryForAttempt].
func (t *Transport) PublishRetry(ctx context.Context, from *babelqueue.ReceivedMessage, delay time.Duration) error {
	topic, err := t.retryTopicFor(from.Queue, delay)
	if err != nil {
		return err
	}
	env, err := babelqueue.Decode([]byte(from.Body))
	if err != nil {
		return fmt.Errorf("kafka: retry needs a decodable envelope: %w", err)
	}
	env.Attempts++
	next, err := env.Encode()
	if err != nil {
		return err
	}
	m := retryRecord(topic, from.Queue, string(next), delay)
	if km, ok := from.Handle.(kafkago.Message); ok {
		m.Key = km.Key
	}
	return t.writer.WriteMessages(ctx, m)
}

// PublishRetryForAttempt republishes the reserved record from for a release/retry (§6.5). The
// target tier is n = min(attemptsBefore, N-1) + 1 of the N configured tiers (the last tier is kept
// once they run out) and the delay is that tier's own, so `bq-delay` = the tier in milliseconds.
// The envelope's attempts and the `bq-attempts` header both become attemptsBefore + 1,
// `bq-original-topic` is the reserved record's topic, the record key is carried over byte for byte
// and the W3C trace headers (`traceparent`, `tracestate`) of the reserved record are forwarded. The
// caller commits the original offset afterwards ([Transport.Ack]). It returns [ErrDelayUnsupported]
// when no tiers are configured; nothing is written then.
//
// This only writes to the retry topic; the [Reinjector] returns the record to the work topic once
// the tier has elapsed. Unknown envelope keys survive the republish (ADR-0031).
func (t *Transport) PublishRetryForAttempt(ctx context.Context, from *babelqueue.ReceivedMessage, attemptsBefore int) error {
	n, tier, err := t.retryTierForAttempt(attemptsBefore)
	if err != nil {
		return err
	}
	env, err := babelqueue.Decode([]byte(from.Body))
	if err != nil {
		return fmt.Errorf("kafka: retry needs a decodable envelope: %w", err)
	}
	if attemptsBefore < 0 {
		attemptsBefore = 0
	}
	env.Attempts = attemptsBefore + 1
	next, err := env.Encode()
	if err != nil {
		return err
	}
	m := retryRecord(RetryTopic(from.Queue, n), from.Queue, string(next), tier)
	if km, ok := from.Handle.(kafkago.Message); ok {
		m.Key = km.Key
		for _, h := range km.Headers {
			if h.Key == "traceparent" || h.Key == "tracestate" {
				m.Headers = append(m.Headers, h)
			}
		}
	}
	return t.writer.WriteMessages(ctx, m)
}
