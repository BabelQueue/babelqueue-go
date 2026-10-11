package babelqueue

import (
	"context"
	"sync"
	"time"
)

// ReceivedMessage is a message reserved from a queue, plus a transport-internal
// handle the transport uses to acknowledge it.
type ReceivedMessage struct {
	Body   string
	Queue  string
	Handle any
	// Headers are out-of-band transport headers a [HeaderPublisher] carried with the
	// message (e.g. the bq-replay-bypass marker). Nil for transports that don't surface
	// them; reads are nil-safe.
	Headers map[string]string
	// DeliveryCount is the broker's native delivery count for this reservation
	// (1 on the first delivery), or 0 when the transport does not track one. The
	// [App] treats DeliveryCount−1 as a floor for the envelope's attempts, so the
	// WithMaxAttempts bound and dead-lettering still hold when a message is released
	// in place and its body cannot carry the counter (e.g. it does not decode).
	DeliveryCount int
	// RouteURN is an optional routing override: when non-empty the [App] looks the
	// handler up by this URN instead of the decoded body's URN. A transport whose
	// broker carries the URN out of band (e.g. the Pulsar bq-job property) fills it so
	// routing follows the property while the body is left untouched — the handler,
	// retries and the dead-letter record all keep the producer's original body.
	// Empty means "route on the body URN" (the default for every other transport).
	RouteURN string
}

// Transport is the minimal broker contract the [App] runtime talks to: publish a
// raw (already-encoded) body, reserve the next message, and acknowledge it. The
// runtime is broker-agnostic — implement this to back it with any broker. The
// github.com/babelqueue/babelqueue-go/redis and .../amqp modules provide Redis and
// RabbitMQ implementations; [InMemoryTransport] backs tests and local runs.
type Transport interface {
	// Publish appends an already-encoded envelope to queue.
	Publish(ctx context.Context, queue, body string) error
	// Pop reserves the next message from queue, blocking up to timeout. It returns
	// (nil, nil) when no message arrives before the timeout.
	Pop(ctx context.Context, queue string, timeout time.Duration) (*ReceivedMessage, error)
	// Ack acknowledges (removes) a previously reserved message.
	Ack(ctx context.Context, msg *ReceivedMessage) error
}

// HeaderPublisher is an optional [Transport] capability: publish a body together with
// out-of-band transport headers (e.g. the replay-bypass marker), for brokers that carry
// per-message metadata. A transport that does not implement it simply does not propagate
// headers — callers fall back to plain Publish (ADR-0027).
type HeaderPublisher interface {
	PublishWithHeaders(ctx context.Context, queue, body string, headers map[string]string) error
}

// RoutedPublisher is an optional [Transport] capability: re-publish a body while keeping
// the routing identity the broker's own routing property carried on the original delivery
// (see [ReceivedMessage.RouteURN]). The [App] uses it for its retry and unknown-URN release
// re-publishes when the received message was routed on a URN that differs from its body, so
// a routing-property mismatch does not flip to the body URN on the next attempt (contract
// §5.2/§5.7). routeURN is non-empty; a transport that does not implement this falls back to
// plain Publish.
type RoutedPublisher interface {
	PublishRouted(ctx context.Context, queue, body, routeURN string) error
}

// KeyedRepublisher is an optional [Transport] capability: re-publish a body on behalf of a
// message the transport itself reserved, carrying whatever native per-message metadata the
// broker attaches that the plain [Transport.Publish] cannot express — for Kafka, the record key
// (contract §6.5). The [App] uses it for every re-publish it makes from a received message
// (retry, unknown-URN release fallback, dead-letter writes). from is the received message the
// republish stands in for, never nil. An implementation that also routes on
// [ReceivedMessage.RouteURN] (see [RoutedPublisher]) must honour that itself. A transport that
// does not implement this falls back to [RoutedPublisher] / plain Publish, so the metadata is
// simply not carried.
type KeyedRepublisher interface {
	PublishFrom(ctx context.Context, queue, body string, from *ReceivedMessage) error
}

// publishKeepingRoute publishes body to queue, carrying routeURN when it is non-empty and t
// implements [RoutedPublisher]; otherwise it is a plain Publish. Shared by the App's re-publishes
// (retry, release, dead-letter) and by [Redrive].
func publishKeepingRoute(ctx context.Context, t Transport, queue, body, routeURN string) error {
	if routeURN != "" {
		if rp, ok := t.(RoutedPublisher); ok {
			return rp.PublishRouted(ctx, queue, body, routeURN)
		}
	}
	return t.Publish(ctx, queue, body)
}

// Releaser is an optional [Transport] capability: return a reserved message to its
// queue in place, to be redelivered after delay, instead of re-publishing a copy and
// acknowledging the original. Brokers whose contract binding mandates an in-place
// release (e.g. SQS §3: ChangeMessageVisibility on the receipt handle, with the
// broker's receive count as the authoritative attempt counter) implement it. When
// Release returns [ErrReleaseUnsupported] the [App] falls back to re-publish + ack;
// any other error leaves the message reserved (not acked), so the broker redelivers
// it once its reservation lapses.
type Releaser interface {
	Release(ctx context.Context, msg *ReceivedMessage, delay time.Duration) error
}

// Rewinder is an optional [Transport] capability: hand an unacknowledged reserved message back to
// the broker so it is redelivered, and make sure nothing the consumer acknowledges afterwards can
// move past it. The [App] uses it when a message could not be dead-lettered (a version-gated
// delivery, an undecodable body, or a message whose retries are exhausted) and must not be
// lost: for an offset-committing broker (Kafka, where a commit is cumulative) it rewinds the
// consumer to the message's offset; for a lock-based broker it releases or abandons the lock.
// A DLQ redrive also calls it when it aborts after a publish failure: msg is then already restored
// and acknowledged, and the call stands for the rest of the drained batch, which was popped but
// never processed. That batch-wide meaning holds for an offset-committing broker (Kafka); on a
// lock-based broker (Artemis, Azure Service Bus, Pulsar) the call only touches the message it is
// given (already acknowledged, so a no-op) and the rest of the batch returns when its lock or
// ack timeout lapses. An implementation must rewind to what is committed, never past it, so an
// acknowledged message is not redelivered.
// A transport whose unacknowledged messages already come back on their own (SQS visibility timeout) need not implement it. It must never delete or acknowledge the message,
// and must not alter its body or attempt counters.
type Rewinder interface {
	Rewind(ctx context.Context, msg *ReceivedMessage) error
}

// InMemoryTransport is an in-process [Transport] for tests and broker-free local
// runs. It is safe for concurrent use. Pop returns immediately (it does not block
// for the timeout) — when the queue is empty it returns (nil, nil).
type inMemoryMessage struct {
	body    string
	headers map[string]string
}

type InMemoryTransport struct {
	mu     sync.Mutex
	queues map[string][]inMemoryMessage
}

// NewInMemoryTransport returns an empty in-process transport.
func NewInMemoryTransport() *InMemoryTransport {
	return &InMemoryTransport{queues: make(map[string][]inMemoryMessage)}
}

// Publish appends body to the in-memory queue.
func (t *InMemoryTransport) Publish(_ context.Context, queue, body string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.queues[queue] = append(t.queues[queue], inMemoryMessage{body: body})
	return nil
}

// PublishWithHeaders appends body plus out-of-band headers ([HeaderPublisher]).
func (t *InMemoryTransport) PublishWithHeaders(_ context.Context, queue, body string, headers map[string]string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.queues[queue] = append(t.queues[queue], inMemoryMessage{body: body, headers: headers})
	return nil
}

// Pop removes and returns the head of queue, or (nil, nil) when it is empty.
func (t *InMemoryTransport) Pop(_ context.Context, queue string, _ time.Duration) (*ReceivedMessage, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	q := t.queues[queue]
	if len(q) == 0 {
		return nil, nil
	}
	m := q[0]
	t.queues[queue] = q[1:]
	return &ReceivedMessage{Body: m.body, Queue: queue, Headers: m.headers}, nil
}

// Ack is a no-op: Pop already removed the message.
func (t *InMemoryTransport) Ack(_ context.Context, _ *ReceivedMessage) error { return nil }

// Size reports how many messages are pending on queue (test helper).
func (t *InMemoryTransport) Size(queue string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.queues[queue])
}
