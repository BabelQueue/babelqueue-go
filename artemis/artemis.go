// Package artemis is an Apache ActiveMQ Artemis-backed [babelqueue.Transport] for the
// BabelQueue Go runtime, speaking AMQP 1.0 on the pure-Go [github.com/Azure/go-amqp] (no CGo).
// Artemis speaks AMQP 1.0 (not RabbitMQ's 0-9-1), so this is a distinct transport from the
// amqp (0-9-1) one.
//
// Producing sends the canonical envelope as the message body and projects the contract
// envelope fields onto the AMQP a JMS peer reads: correlation-id = trace_id (JMSCorrelationID),
// creation-time = meta.created_at (JMSTimestamp), the x-opt-jms-type message annotation = URN
// (JMSType, the AMQP-JMS mapping), plus the bq_ application properties (string-valued; underscored
// names, since JMS property names must be valid Java identifiers) — so a Java/.NET/Node/Python peer can route and
// correlate without parsing the body. Consuming receives one message at a time
// (Receive -> process -> Ack); the broker's native AMQP delivery-count is reconciled onto the
// envelope as attempts = max(body, delivery-count) — no −1, because the AMQP counter is 0-based
// (0 on first delivery). (The Java JMS binding reads the 1-based JMSXDeliveryCount and subtracts
// 1, arriving at the same 0-based attempts.)
//
//	tr, _ := artemis.New(ctx, artemis.WithURL("amqp://localhost:5672"))
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// This binding implements §7 of the BabelQueue broker-bindings contract. The envelope is
// unchanged (schema_version stays 1); Apache ActiveMQ Artemis is purely additive.
//
// Full spec: https://babelqueue.com
package artemis

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

// jmsTypeKey is the message annotation carrying the URN (the AMQP-JMS mapping of JMSType).
const jmsTypeKey = "x-opt-jms-type"

// Sender is the subset of [amqp.Sender] the transport uses; the concrete sender satisfies it,
// and a fake satisfies it in tests.
type Sender interface {
	Send(ctx context.Context, msg *amqp.Message, opts *amqp.SendOptions) error
}

// Receiver is the subset of [amqp.Receiver] the transport uses.
type Receiver interface {
	Receive(ctx context.Context, opts *amqp.ReceiveOptions) (*amqp.Message, error)
	AcceptMessage(ctx context.Context, msg *amqp.Message) error
}

// Client creates per-queue senders and receivers. An [amqp.Session] is wrapped to satisfy it
// (see [WithSession]); a fake satisfies it in tests.
type Client interface {
	Sender(ctx context.Context, queue string) (Sender, error)
	Receiver(ctx context.Context, queue string) (Receiver, error)
}

// sessionClient adapts a real [amqp.Session] to the [Client] seam.
type sessionClient struct{ session *amqp.Session }

func (s sessionClient) Sender(ctx context.Context, queue string) (Sender, error) {
	return s.session.NewSender(ctx, queue, nil)
}

func (s sessionClient) Receiver(ctx context.Context, queue string) (Receiver, error) {
	return s.session.NewReceiver(ctx, queue, nil)
}

// Transport implements [babelqueue.Transport] over Apache ActiveMQ Artemis (AMQP 1.0). It is
// safe for concurrent use; the per-queue sender/receiver caches are guarded by a mutex.
type Transport struct {
	client  Client
	conn    *amqp.Conn
	url     string
	maxWait time.Duration

	mu        sync.Mutex
	senders   map[string]Sender
	receivers map[string]Receiver
}

// Option customizes [New].
type Option func(*Transport)

// WithURL builds the connection from an AMQP 1.0 URL (amqp:// or amqps://).
func WithURL(url string) Option { return func(t *Transport) { t.url = url } }

// WithClient injects a [Client] (a fake in tests), bypassing all AMQP wiring.
func WithClient(c Client) Option { return func(t *Transport) { t.client = c } }

// WithMaxWaitTime caps how long a single Pop blocks waiting for a message.
func WithMaxWaitTime(d time.Duration) Option { return func(t *Transport) { t.maxWait = d } }

// New builds a transport. Provide an AMQP URL ([WithURL]) or an injected [Client] ([WithClient]).
// With a URL it dials the broker and opens a session, both closed by [Transport.Close].
func New(ctx context.Context, opts ...Option) (*Transport, error) {
	t := &Transport{senders: make(map[string]Sender), receivers: make(map[string]Receiver)}
	for _, o := range opts {
		o(t)
	}
	if t.client != nil {
		return t, nil
	}
	if t.url == "" {
		return nil, errors.New("artemis: provide WithURL or WithClient")
	}
	conn, err := amqp.Dial(ctx, t.url, nil)
	if err != nil {
		return nil, err
	}
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	t.conn = conn
	t.client = sessionClient{session: session}
	return t, nil
}

func (t *Transport) sender(ctx context.Context, queue string) (Sender, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.senders[queue]; ok {
		return s, nil
	}
	s, err := t.client.Sender(ctx, queue)
	if err != nil {
		return nil, err
	}
	t.senders[queue] = s
	return s, nil
}

func (t *Transport) receiver(ctx context.Context, queue string) (Receiver, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r, ok := t.receivers[queue]; ok {
		return r, nil
	}
	r, err := t.client.Receiver(ctx, queue)
	if err != nil {
		return nil, err
	}
	t.receivers[queue] = r
	return r, nil
}

// Publish sends body to queue with the §7 AMQP projection.
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	s, err := t.sender(ctx, queue)
	if err != nil {
		return err
	}
	return s.Send(ctx, message(body), nil)
}

// Pop receives the next message, bounded by timeout (and WithMaxWaitTime). It reconciles
// attempts to max(body.attempts, delivery-count). Returns (nil, nil) when no message arrives.
func (t *Transport) Pop(ctx context.Context, queue string, timeout time.Duration) (*babelqueue.ReceivedMessage, error) {
	r, err := t.receiver(ctx, queue)
	if err != nil {
		return nil, err
	}
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
	msg, err := r.Receive(rctx, nil)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, nil
		}
		return nil, err
	}
	body := reconcileAttempts(messageBody(msg), deliveryCount(msg))
	return &babelqueue.ReceivedMessage{Body: body, Queue: queue, Handle: msg}, nil
}

// Ack acknowledges (accepts) the reserved message.
func (t *Transport) Ack(ctx context.Context, msg *babelqueue.ReceivedMessage) error {
	m, ok := msg.Handle.(*amqp.Message)
	if !ok {
		return nil
	}
	r, err := t.receiver(ctx, msg.Queue)
	if err != nil {
		return err
	}
	return r.AcceptMessage(ctx, m)
}

// Close releases the dialed connection (a no-op when a Client was injected).
func (t *Transport) Close() error {
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}

// message projects the envelope's contract fields onto a native AMQP message: body = envelope
// JSON (an AMQP value), correlation-id = trace_id, creation-time = meta.created_at, the
// x-opt-jms-type annotation = URN, plus the bq_ application properties. The body stays
// authoritative.
func message(body string) *amqp.Message {
	m := &amqp.Message{Value: body}
	env, err := babelqueue.Decode([]byte(body))
	if err != nil {
		return m
	}
	m.Properties = &amqp.MessageProperties{}
	if env.TraceID != "" {
		m.Properties.CorrelationID = env.TraceID
	}
	if env.Meta.CreatedAt != 0 {
		ts := time.UnixMilli(env.Meta.CreatedAt).UTC()
		m.Properties.CreationTime = &ts
	}
	m.ApplicationProperties = applicationProperties(env)
	if env.Job != "" {
		m.Annotations = amqp.Annotations{jmsTypeKey: env.Job}
	}
	return m
}

// applicationProperties builds the string-valued bq_* projection (Contract §7.2). The names use
// UNDERSCORES, not hyphens: a JMS property name must be a valid Java identifier, and every
// Artemis SDK uses the same JMS-legal form for cross-protocol parity.
func applicationProperties(env babelqueue.Envelope) map[string]any {
	props := map[string]any{
		"bq_schema_version": strconv.Itoa(env.Meta.SchemaVersion),
		"bq_attempts":       strconv.Itoa(env.Attempts),
		"bq_app_id":         "babelqueue",
	}
	if env.Meta.Lang != "" {
		props["bq_source_lang"] = env.Meta.Lang
	}
	return props
}

// messageBody returns the message body as text (an AMQP value string, or a UTF-8 Data section).
func messageBody(msg *amqp.Message) string {
	if msg.Value != nil {
		switch v := msg.Value.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		}
	}
	if data := msg.GetData(); data != nil {
		return string(data)
	}
	return ""
}

// deliveryCount reads the broker's AMQP delivery-count header (0-based; 0 when absent).
func deliveryCount(msg *amqp.Message) uint32 {
	if msg.Header != nil {
		return msg.Header.DeliveryCount
	}
	return 0
}

// reconcileAttempts sets the envelope's top-level attempts to max(current, delivery-count). The
// AMQP delivery-count is 0-based (0 on first delivery) so it maps directly with no −1, and the
// max never lowers a runtime-incremented counter (the App retries by republishing with
// attempts+1, which resets the broker's delivery-count to 0).
func reconcileAttempts(body string, count uint32) string {
	if count == 0 {
		return body
	}
	native := int(count)
	env, err := babelqueue.Decode([]byte(body))
	if err != nil || native <= env.Attempts {
		return body
	}
	env.Attempts = native
	if b, err := env.Encode(); err == nil {
		return string(b)
	}
	return body
}

var _ babelqueue.Transport = (*Transport)(nil)
