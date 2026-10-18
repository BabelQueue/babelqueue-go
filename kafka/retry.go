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

// WithRetryTiers configures the delay tiers of the retry-topic pattern (broker-bindings.md §6.4):
// tier i (1-based, ascending delay) is the topic `<topic>.retry.<i>`. Tiers are sorted; the set of
// retry topics must be declared on the broker by the operator. Without tiers, a delayed publish or
// a retry republish fails with [ErrDelayUnsupported].
//
// This option only enables the publish side. Draining the retry topics back into the work topic
// after `record.timestamp + tier` (the retry-topic consumer of §6.4) is not part of this transport.
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

// retryRecord builds the record routed via a retry topic: the §6 projection plus `bq-delay` (ms,
// the requested delay) and `bq-original-topic` (§6.3).
func retryRecord(retryTopic, original, body string, delay time.Duration) kafkago.Message {
	m := message(retryTopic, body)
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
// WARNING: this only writes to the retry topic. A message published here is delivered to the work
// topic again only if a re-inject consumer reads the retry topic, waits out `bq-delay` and writes
// the record back to the `bq-original-topic` work topic. The Go SDK does not provide that consumer
// yet, so the caller must run one (or another SDK's) or the message is never redelivered.
func (t *Transport) PublishDelayed(ctx context.Context, queue, body string, delay time.Duration) error {
	topic, err := t.retryTopicFor(queue, delay)
	if err != nil {
		return err
	}
	return t.writer.WriteMessages(ctx, retryRecord(topic, queue, body, delay))
}

// PublishRetry republishes the reserved record from to a retry topic for a release/retry (§6.5):
// the envelope's attempts and the `bq-attempts` header both become attempts + 1, `bq-original-topic`
// and `bq-delay` (= delay) are set, and the reserved record's key is carried over. The caller
// commits the original offset afterwards ([Transport.Ack]). It returns [ErrDelayUnsupported] when
// no tiers are configured or delay exceeds the largest tier.
//
// WARNING: like [Transport.PublishDelayed], this only writes to the retry topic. The record returns
// to the work topic only if a re-inject consumer (retry topic -> work topic, honouring `bq-delay`)
// is running; the Go SDK does not provide one yet, so without it the retried message is not
// redelivered.
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
