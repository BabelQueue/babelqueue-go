// Package amqp is a RabbitMQ-backed [babelqueue.Transport] (AMQP 0-9-1) for the
// BabelQueue Go runtime. Producing publishes to a durable queue with persistent
// delivery and the AMQP properties that are part of the cross-language contract
// (type = URN, correlation_id = trace_id, message_id = meta.id, and the
// x-schema-version / x-source-lang / x-attempts headers) — so a PHP/Python/...
// consumer can route on properties.type without parsing the body. Consuming uses
// basic.get + manual ack (at-least-once), matching the PHP RabbitMQ driver.
//
//	tr := amqp.New("amqp://guest:guest@localhost:5672/")
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// The connection is lazy: it (re)connects on first use and after a drop.
package amqp

import (
	"context"
	"sync"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Transport implements babelqueue.Transport over RabbitMQ. It is safe for
// concurrent use; broker operations are serialized on a single channel.
type Transport struct {
	url string

	mu       sync.Mutex
	conn     *amqp091.Connection
	channel  *amqp091.Channel
	declared map[string]bool
}

// New returns a transport for the given AMQP URL. It does not connect until the
// first Publish/Pop.
func New(url string) *Transport {
	return &Transport{url: url, declared: make(map[string]bool)}
}

// ensure (re)establishes the connection and channel. Caller must hold t.mu.
func (t *Transport) ensure() (*amqp091.Channel, error) {
	if t.conn == nil || t.conn.IsClosed() {
		conn, err := amqp091.Dial(t.url)
		if err != nil {
			return nil, err
		}
		t.conn = conn
		t.channel = nil
		t.declared = make(map[string]bool)
	}
	if t.channel == nil || t.channel.IsClosed() {
		ch, err := t.conn.Channel()
		if err != nil {
			return nil, err
		}
		t.channel = ch
	}
	return t.channel, nil
}

// declare idempotently declares a durable queue. Caller must hold t.mu.
func (t *Transport) declare(ch *amqp091.Channel, queue string) error {
	if t.declared[queue] {
		return nil
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	t.declared[queue] = true
	return nil
}

// Publish declares the durable queue and publishes body with persistent delivery
// and the contract AMQP properties.
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch, err := t.ensure()
	if err != nil {
		return err
	}
	if err := t.declare(ch, queue); err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, "", queue, false, false, t.publishing(body))
}

func (t *Transport) publishing(body string) amqp091.Publishing {
	pub := amqp091.Publishing{
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		DeliveryMode:    amqp091.Persistent,
		AppId:           "babelqueue",
		Body:            []byte(body),
	}
	if env, err := babelqueue.Decode([]byte(body)); err == nil {
		pub.Type = env.Job
		pub.CorrelationId = env.TraceID
		pub.MessageId = env.Meta.ID
		headers := amqp091.Table{"x-attempts": env.Attempts}
		if env.Meta.SchemaVersion != 0 {
			headers["x-schema-version"] = env.Meta.SchemaVersion
		}
		if env.Meta.Lang != "" {
			headers["x-source-lang"] = env.Meta.Lang
		}
		pub.Headers = headers
	}
	return pub
}

// Pop reserves the next message with basic.get (manual ack). When the queue is
// empty it waits up to timeout (heartbeat-safe) and returns (nil, nil).
func (t *Transport) Pop(ctx context.Context, queue string, timeout time.Duration) (*babelqueue.ReceivedMessage, error) {
	t.mu.Lock()
	ch, err := t.ensure()
	if err != nil {
		t.mu.Unlock()
		return nil, err
	}
	if err := t.declare(ch, queue); err != nil {
		t.mu.Unlock()
		return nil, err
	}
	delivery, ok, err := ch.Get(queue, false)
	t.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if !ok {
		if timeout > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(timeout):
			}
		}
		return nil, nil
	}
	return &babelqueue.ReceivedMessage{Body: string(delivery.Body), Queue: queue, Handle: delivery}, nil
}

// Ack acknowledges the reserved delivery.
func (t *Transport) Ack(_ context.Context, msg *babelqueue.ReceivedMessage) error {
	delivery, ok := msg.Handle.(amqp091.Delivery)
	if !ok {
		return nil
	}
	return delivery.Ack(false)
}

// Close tears down the channel and connection.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.channel != nil && !t.channel.IsClosed() {
		_ = t.channel.Close()
	}
	if t.conn != nil && !t.conn.IsClosed() {
		return t.conn.Close()
	}
	return nil
}

var _ babelqueue.Transport = (*Transport)(nil)
