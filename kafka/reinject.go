package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Retry-topic headers the re-inject consumer consumes (broker-bindings.md §6.4). Neither is
// forwarded to the work topic.
const (
	headerDelay         = "bq-delay"
	headerOriginalTopic = "bq-original-topic"
)

// retryTopicSuffix matches the `.retry.<n>` suffix of a retry topic name.
var retryTopicSuffix = regexp.MustCompile(`\.retry\.[0-9]+$`)

// reinjectQueueDepth bounds the records buffered per partition between the fetch loop and a
// partition worker; a full buffer applies back-pressure to the fetch loop.
const reinjectQueueDepth = 64

// Reinjector drains the retry topics `<topic>.retry.<n>` back into their work topics
// (broker-bindings.md §6.4). Kafka has no delayed delivery; the retry-topic pattern parks a record
// in a tier topic, and this consumer carries it back once its tier has elapsed. A deployment that
// configures [WithRetryTiers] MUST run one Reinjector per set of work topics, or retried and
// delayed records are never redelivered.
//
// For every record of `<topic>.retry.<n>` it:
//
//   - waits until `record.timestamp + tier_ms`, clamped to [now, now+tier_ms]: the wait starts when
//     the record was written to the retry topic, never when it was consumed, and a skewed
//     timestamp can neither skip the delay (future-dated records wait at most one tier) nor
//     hold the record beyond it (old records are carried at once);
//   - writes the value and the key, byte for byte, to the topic named by its `bq-original-topic`
//     header (no decode, no re-encode, `attempts` untouched; an undecodable record is carried
//     as it is and the work-topic consumer dead-letters it as poison);
//   - drops the `bq-delay` and `bq-original-topic` headers and keeps every other one,
//     `traceparent` included;
//   - commits the retry-topic offset only after the broker acknowledged that write. A failed write
//     is retried and never committed, so delivery is at-least-once.
//
// kafka-go exposes no pause/resume for a group reader's partitions, so the wait is held in one
// worker goroutine per partition fed through a bounded buffer: the fetch loop keeps polling (the
// consumer stays in its group) while a record sleeps, and partitions wait independently. Records
// of one partition are carried in offset order.
//
// The retry topics SHOULD be created with `message.timestamp.type=LogAppendTime`, so that
// `record.timestamp` is the broker's append time and a producer's clock cannot skew the wait. With
// the default CreateTime the producer's clock is trusted (the SDK stamps the publish time).
type Reinjector struct {
	writer    Writer
	newReader func(topic string) Reader
	tiers     map[string]time.Duration // retry topic -> tier delay
	topics    []string                 // retry topics, deterministic order

	// Clock hooks; replaced by tests.
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// NewReinjector builds a [Reinjector] for the given work topics, using the tiers, writer, brokers
// and group id the transport was configured with ([WithRetryTiers], [WithBrokers], [WithGroupID],
// or the injected [WithWriter] / [WithReaderFactory]). It consumes `<topic>.retry.1` ...
// `<topic>.retry.N` for every work topic and returns [ErrDelayUnsupported] when no tiers are
// configured. Use a dedicated group id for the re-inject process.
func (t *Transport) NewReinjector(workTopics ...string) (*Reinjector, error) {
	if len(t.retryTiers) == 0 {
		return nil, fmt.Errorf("%w: no retry tiers configured", ErrDelayUnsupported)
	}
	if len(workTopics) == 0 {
		return nil, errors.New("kafka: NewReinjector needs at least one work topic")
	}
	r := &Reinjector{
		writer:    t.writer,
		newReader: t.newReader,
		tiers:     make(map[string]time.Duration),
		now:       time.Now,
		sleep:     sleepContext,
	}
	for _, topic := range workTopics {
		for i, tier := range t.retryTiers {
			rt := RetryTopic(topic, i+1)
			r.tiers[rt] = tier
			r.topics = append(r.topics, rt)
		}
	}
	return r, nil
}

// Run consumes the retry topics until ctx is cancelled (it then returns nil) or a reader fails
// permanently (it returns that error). Records fetched but not yet committed when it stops are
// redelivered to the next run.
func (r *Reinjector) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}
	for _, rt := range r.topics {
		rd := r.newReader(rt)
		tier := r.tiers[rt]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c, ok := rd.(io.Closer); ok {
				defer c.Close()
			}
			if err := r.consumeTier(ctx, rd, rt, tier); err != nil {
				fail(err)
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// consumeTier is the fetch loop of one retry topic: it dispatches records to per-partition workers.
func (r *Reinjector) consumeTier(ctx context.Context, rd Reader, retryTopic string, tier time.Duration) error {
	var (
		wg      sync.WaitGroup
		workers = map[int]chan kafkago.Message{}
	)
	defer func() {
		for _, ch := range workers {
			close(ch)
		}
		wg.Wait()
	}()

	for {
		m, err := rd.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka: re-inject fetch from %s: %w", retryTopic, err)
		}
		ch, ok := workers[m.Partition]
		if !ok {
			ch = make(chan kafkago.Message, reinjectQueueDepth)
			workers[m.Partition] = ch
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.partitionWorker(ctx, rd, ch, tier)
			}()
		}
		select {
		case ch <- m:
		case <-ctx.Done():
			return nil
		}
	}
}

// partitionWorker carries the records of one partition in order: wait, write, commit.
func (r *Reinjector) partitionWorker(ctx context.Context, rd Reader, in <-chan kafkago.Message, tier time.Duration) {
	for m := range in {
		if ctx.Err() != nil {
			return
		}
		if err := r.sleep(ctx, r.waitFor(m, tier)); err != nil {
			return
		}
		out := reinjectRecord(m)
		if !r.retryUntilDone(ctx, func() error { return r.writer.WriteMessages(ctx, out) }) {
			return
		}
		// Commit only after the broker acknowledged the write above.
		if !r.retryUntilDone(ctx, func() error { return rd.CommitMessages(ctx, m) }) {
			return
		}
	}
}

// waitFor is how long m still has to wait: target = record.timestamp + tier, clamped to
// [now, now+tier] (§6.4).
func (r *Reinjector) waitFor(m kafkago.Message, tier time.Duration) time.Duration {
	now := r.now()
	target := m.Time.Add(tier)
	if target.Before(now) {
		target = now
	}
	if limit := now.Add(tier); target.After(limit) {
		target = limit
	}
	return target.Sub(now)
}

// retryUntilDone runs op until it succeeds (true) or ctx ends (false), pausing between attempts
// with a capped exponential backoff.
func (r *Reinjector) retryUntilDone(ctx context.Context, op func() error) bool {
	pause := 100 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return false
		}
		if op() == nil {
			return true
		}
		if r.sleep(ctx, pause) != nil {
			return false
		}
		if pause *= 2; pause > 5*time.Second {
			pause = 5 * time.Second
		}
	}
}

// reinjectRecord builds the work-topic record for the retry-topic record m: same value and key
// bytes, every header but `bq-delay` / `bq-original-topic`, addressed to `bq-original-topic`
// (falling back to the retry topic's name without its `.retry.<n>` suffix).
func reinjectRecord(m kafkago.Message) kafkago.Message {
	target := ""
	hdrs := make([]kafkago.Header, 0, len(m.Headers))
	for _, h := range m.Headers {
		switch h.Key {
		case headerOriginalTopic:
			target = string(h.Value)
		case headerDelay:
		default:
			hdrs = append(hdrs, h)
		}
	}
	if target == "" {
		target = retryTopicSuffix.ReplaceAllString(m.Topic, "")
	}
	return kafkago.Message{Topic: target, Key: m.Key, Value: m.Value, Headers: hdrs}
}

// sleepContext waits d or until ctx ends (then it returns ctx.Err()).
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
