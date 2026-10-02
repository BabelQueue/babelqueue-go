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
