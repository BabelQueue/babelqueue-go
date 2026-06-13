// Package azureservicebus is an Azure Service Bus-backed [babelqueue.Transport] for
// the BabelQueue Go runtime. Producing sends the canonical envelope as the message
// body and projects the contract envelope fields onto native Service Bus fields
// (Subject = URN, CorrelationID = trace_id, MessageID = meta.id, plus the bq-
// application properties) — so a .NET/Java/... peer can route on Subject and correlate
// on CorrelationID without parsing the body. Consuming uses the PeekLock reservation
// model (ReceiveMessages -> process -> CompleteMessage); the broker's native
// DeliveryCount is reconciled onto the envelope as attempts = max(body, DeliveryCount−1)
// so a crash-redelivered message reflects its true delivery count without lowering the
// runtime's own counter.
//
//	tr, _ := azureservicebus.New(azureservicebus.WithConnectionString(cs))
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// This binding implements §4 of the BabelQueue broker-bindings contract. The envelope
// is unchanged (schema_version stays 1); Azure Service Bus is purely additive.
//
// Full spec: https://babelqueue.com
package azureservicebus

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

// Sender is the subset of *azservicebus.Sender the transport uses; the concrete
// sender satisfies it, and a fake satisfies it in tests.
type Sender interface {
	SendMessage(ctx context.Context, message *azservicebus.Message, options *azservicebus.SendMessageOptions) error
}

// Receiver is the subset of *azservicebus.Receiver the transport uses.
type Receiver interface {
	ReceiveMessages(ctx context.Context, maxMessages int, options *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
	CompleteMessage(ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.CompleteMessageOptions) error
}

// Client creates per-entity senders and receivers. A *azservicebus.Client is wrapped
// to satisfy it (see [WithAzureClient]); a fake satisfies it in tests.
type Client interface {
	NewSender(queueOrTopic string) (Sender, error)
	NewReceiver(queue string) (Receiver, error)
}

// azureClient adapts a real *azservicebus.Client to the [Client] seam.
type azureClient struct{ c *azservicebus.Client }

func (a azureClient) NewSender(queueOrTopic string) (Sender, error) {
	s, err := a.c.NewSender(queueOrTopic, nil)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (a azureClient) NewReceiver(queue string) (Receiver, error) {
	r, err := a.c.NewReceiverForQueue(queue, nil)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Transport implements [babelqueue.Transport] over Azure Service Bus. It is safe for
// concurrent use; the per-queue sender/receiver caches are guarded by a mutex.
type Transport struct {
	client  Client
	connStr string
	maxWait time.Duration

	mu        sync.Mutex
	senders   map[string]Sender
	receivers map[string]Receiver
}

// Option customizes [New].
type Option func(*Transport)

// WithConnectionString builds the client from an ASB connection string
// (Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...).
func WithConnectionString(cs string) Option { return func(t *Transport) { t.connStr = cs } }

// WithAzureClient wraps a preconfigured *azservicebus.Client (e.g. built with a
// namespace + azidentity TokenCredential), bypassing connection-string parsing.
func WithAzureClient(c *azservicebus.Client) Option {
	return func(t *Transport) { t.client = azureClient{c} }
}

// WithClient injects a [Client] (a fake in tests), bypassing all Azure wiring.
func WithClient(c Client) Option { return func(t *Transport) { t.client = c } }

// WithMaxWaitTime caps how long a single Pop blocks waiting for a message. The
// runtime's per-iteration poll timeout still bounds it; this caps the upper limit.
func WithMaxWaitTime(d time.Duration) Option { return func(t *Transport) { t.maxWait = d } }

// New builds a transport. Provide a connection string ([WithConnectionString]), a
// preconfigured Azure client ([WithAzureClient]), or an injected [Client]
// ([WithClient]).
func New(opts ...Option) (*Transport, error) {
	t := newTransport(opts...)
	if t.client != nil {
		return t, nil
	}
	if t.connStr != "" {
		c, err := azservicebus.NewClientFromConnectionString(t.connStr, nil)
		if err != nil {
			return nil, err
		}
		t.client = azureClient{c}
		return t, nil
	}
	return nil, errors.New("azureservicebus: provide WithConnectionString, WithAzureClient, or WithClient")
}

func newTransport(opts ...Option) *Transport {
	t := &Transport{senders: make(map[string]Sender), receivers: make(map[string]Receiver)}
	for _, o := range opts {
		o(t)
	}
	return t
}

func (t *Transport) sender(queue string) (Sender, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.senders[queue]; ok {
		return s, nil
	}
	s, err := t.client.NewSender(queue)
	if err != nil {
		return nil, err
	}
	t.senders[queue] = s
	return s, nil
}

func (t *Transport) receiver(queue string) (Receiver, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r, ok := t.receivers[queue]; ok {
		return r, nil
	}
	r, err := t.client.NewReceiver(queue)
	if err != nil {
		return nil, err
	}
	t.receivers[queue] = r
	return r, nil
}

// Publish sends body to queue with the §4 native projection.
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	s, err := t.sender(queue)
	if err != nil {
		return err
	}
	return s.SendMessage(ctx, message(body), nil)
}

// Pop reserves the next message (PeekLock), bounded by timeout (and WithMaxWaitTime).
// It reconciles attempts to max(body.attempts, DeliveryCount − 1). Returns (nil, nil)
// when no message arrives.
func (t *Transport) Pop(ctx context.Context, queue string, timeout time.Duration) (*babelqueue.ReceivedMessage, error) {
	r, err := t.receiver(queue)
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
	msgs, err := r.ReceiveMessages(rctx, 1, nil)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, nil
		}
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	m := msgs[0]
	body := reconcileAttempts(string(m.Body), m.DeliveryCount)
	return &babelqueue.ReceivedMessage{Body: body, Queue: queue, Handle: m}, nil
}

// Ack completes the reserved message (removing it from the entity).
func (t *Transport) Ack(ctx context.Context, msg *babelqueue.ReceivedMessage) error {
	rm, ok := msg.Handle.(*azservicebus.ReceivedMessage)
	if !ok || rm == nil {
		return nil
	}
	r, err := t.receiver(msg.Queue)
	if err != nil {
		return err
	}
	return r.CompleteMessage(ctx, rm, nil)
}

// message projects the envelope's contract fields onto a native Service Bus message.
// They are a redundant, routable view of the body — the body stays authoritative.
func message(body string) *azservicebus.Message {
	m := &azservicebus.Message{Body: []byte(body), ContentType: strPtr("application/json")}
	env, err := babelqueue.Decode([]byte(body))
	if err != nil {
		return m
	}
	if env.Job != "" {
		m.Subject = strPtr(env.Job)
	}
	if env.TraceID != "" {
		m.CorrelationID = strPtr(env.TraceID)
	}
	if env.Meta.ID != "" {
		m.MessageID = strPtr(env.Meta.ID)
	}
	props := make(map[string]any, 3)
	if env.Meta.SchemaVersion != 0 {
		props["bq-schema-version"] = env.Meta.SchemaVersion
	}
	if env.Meta.Lang != "" {
		props["bq-source-lang"] = env.Meta.Lang
	}
	if env.Meta.CreatedAt != 0 {
		props["bq-created-at"] = env.Meta.CreatedAt
	}
	if len(props) > 0 {
		m.ApplicationProperties = props
	}
	return m
}

// reconcileAttempts sets the envelope's top-level attempts to
// max(current, DeliveryCount − 1) so a first delivery reads 0 and a natively
// redelivered message reflects its true count, without ever lowering a runtime-
// incremented counter (the App manages retries by republishing with attempts+1).
func reconcileAttempts(body string, deliveryCount uint32) string {
	if deliveryCount <= 1 {
		return body
	}
	native := int(deliveryCount) - 1
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

func strPtr(s string) *string { return &s }

var _ babelqueue.Transport = (*Transport)(nil)
