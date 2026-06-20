// Package amqp is a RabbitMQ-backed [babelqueue.Transport] (AMQP 0-9-1) for the
// BabelQueue Go runtime. Producing publishes to a durable queue with persistent
// delivery and the AMQP properties that are part of the cross-language contract
// (type = URN, correlation_id = trace_id, message_id = meta.id, and the
// x-schema-version / x-source-lang / x-attempts headers) — so a PHP/Python/...
// consumer can route on properties.type without parsing the body. Consuming uses
// basic.get + manual ack (at-least-once), matching the PHP RabbitMQ driver.
//
// It also implements the optional [babelqueue.HeaderPublisher] capability: out-of-band
// transport headers (e.g. a W3C traceparent for cross-hop span linkage, ADR-0028, or
// the bq-replay-bypass marker, ADR-0027) ride in the AMQP message headers
// (amqp091.Table) beside the frozen envelope — never in it (GR-1) — and are surfaced
// back to the consumer on [babelqueue.ReceivedMessage.Headers].
//
//	tr := amqp.New("amqp://guest:guest@localhost:5672/")
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// The connection is lazy: it (re)connects on first use and after a drop.
package amqp

import (
	"context"
	"fmt"
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
	return t.publish(ctx, queue, body, nil)
}

// PublishWithHeaders publishes body together with out-of-band transport headers
// ([babelqueue.HeaderPublisher]). The headers ride in the AMQP message headers
// (amqp091.Table) beside the frozen envelope (GR-1) — e.g. a W3C traceparent for
// cross-hop span linkage (ADR-0028). They are merged on top of the contract x-*
// headers without clobbering them (a contract header always wins a key collision),
// and empty/blank keys are skipped. A nil/empty map behaves exactly like [Transport.Publish].
func (t *Transport) PublishWithHeaders(ctx context.Context, queue, body string, headers map[string]string) error {
	return t.publish(ctx, queue, body, headers)
}

func (t *Transport) publish(ctx context.Context, queue, body string, headers map[string]string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch, err := t.ensure()
	if err != nil {
		return err
	}
	if err := t.declare(ch, queue); err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, "", queue, false, false, t.publishing(body, headers))
}

// publishing builds the AMQP message for body. extra carries optional out-of-band
// transport headers that are merged into the message header table without overwriting
// the contract x-* headers (the contract wins a key collision).
func (t *Transport) publishing(body string, extra map[string]string) amqp091.Publishing {
	pub := amqp091.Publishing{
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		DeliveryMode:    amqp091.Persistent,
		AppId:           "babelqueue",
		Body:            []byte(body),
	}
	headers := amqp091.Table{}
	mergeHeaders(headers, extra)
	if env, err := babelqueue.Decode([]byte(body)); err == nil {
		pub.Type = env.Job
		pub.CorrelationId = env.TraceID
		pub.MessageId = env.Meta.ID
		// Contract x-* headers are written last so they win any key collision with an
		// out-of-band header — the cross-language projection must never be clobbered.
		headers["x-attempts"] = env.Attempts
		if env.Meta.SchemaVersion != 0 {
			headers["x-schema-version"] = env.Meta.SchemaVersion
		}
		if env.Meta.Lang != "" {
			headers["x-source-lang"] = env.Meta.Lang
		}
	}
	if len(headers) > 0 {
		pub.Headers = headers
	}
	return pub
}

// mergeHeaders writes the out-of-band string headers into table as AMQP values,
// skipping empty keys and values. It never removes or overwrites a key already in
// table, so callers can seed contract headers afterwards and keep them authoritative.
func mergeHeaders(table amqp091.Table, headers map[string]string) {
	for k, v := range headers {
		if k == "" || v == "" {
			continue
		}
		if _, exists := table[k]; exists {
			continue
		}
		table[k] = v
	}
}

// headersFromTable maps an inbound AMQP header table to a flat map[string]string,
// stringifying values defensively (AMQP headers are typed: strings, ints, bytes…).
// It returns nil when no headers are present so a header-less delivery stays
// header-less on [babelqueue.ReceivedMessage.Headers].
func headersFromTable(table amqp091.Table) map[string]string {
	if len(table) == 0 {
		return nil
	}
	out := make(map[string]string, len(table))
	for k, v := range table {
		switch val := v.(type) {
		case string:
			out[k] = val
		case []byte:
			out[k] = string(val)
		case nil:
			// skip nil-valued headers
		default:
			out[k] = fmt.Sprintf("%v", val)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
	return &babelqueue.ReceivedMessage{
		Body:    string(delivery.Body),
		Queue:   queue,
		Handle:  delivery,
		Headers: headersFromTable(delivery.Headers),
	}, nil
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

var (
	_ babelqueue.Transport       = (*Transport)(nil)
	_ babelqueue.HeaderPublisher = (*Transport)(nil)
)
