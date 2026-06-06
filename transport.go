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

// InMemoryTransport is an in-process [Transport] for tests and broker-free local
// runs. It is safe for concurrent use. Pop returns immediately (it does not block
// for the timeout) — when the queue is empty it returns (nil, nil).
type InMemoryTransport struct {
	mu     sync.Mutex
	queues map[string][]string
}

// NewInMemoryTransport returns an empty in-process transport.
func NewInMemoryTransport() *InMemoryTransport {
	return &InMemoryTransport{queues: make(map[string][]string)}
}

// Publish appends body to the in-memory queue.
func (t *InMemoryTransport) Publish(_ context.Context, queue, body string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.queues[queue] = append(t.queues[queue], body)
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
	body := q[0]
	t.queues[queue] = q[1:]
	return &ReceivedMessage{Body: body, Queue: queue}, nil
}

// Ack is a no-op: Pop already removed the message.
func (t *InMemoryTransport) Ack(_ context.Context, _ *ReceivedMessage) error { return nil }

// Size reports how many messages are pending on queue (test helper).
func (t *InMemoryTransport) Size(queue string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.queues[queue])
}
