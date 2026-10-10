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
// Before the attempts reconcile, a bq-schema-version application property that is present but
// not the core's supported version is version-gated (§4.7): the body is not decoded and the
// message is routed to a reserved unsupported-version URN no handler owns, so the App
// dead-letters it directly, with no retry (whatever the unknown-URN strategy, StrategyRelease
// included), or drops it when dead-lettering is off. The optional [OnSchemaVersionRejected] hook
// names the rejected version.
//
// Full spec: https://babelqueue.com
package azureservicebus

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
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

	onRejected OnSchemaVersionRejected

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

// OnSchemaVersionRejected is called when the version gate (§4.7) quarantines a message whose
// bq-schema-version application property is present but not the core's supported version. version
// is the property value rendered as text and queue the queue it was received from. The gate runs
// before the body is decoded; the App's quarantine error names the reserved route, and this hook
// additionally hands an operator the rejected version and queue directly (including for a body
// that is not valid JSON). It is a diagnostic hook: a panic in it is recovered and ignored, and
// it never changes how the message is delivered, acked or retried.
type OnSchemaVersionRejected func(version, queue string)

// WithOnSchemaVersionRejected registers an optional [OnSchemaVersionRejected] hook (default: none).
func WithOnSchemaVersionRejected(fn OnSchemaVersionRejected) Option {
	return func(t *Transport) { t.onRejected = fn }
}

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
	if v, rejected := unsupportedSchemaVersion(m.ApplicationProperties); rejected {
		// Version gate (§4.7): quarantine before decoding. The body is handed over untouched
		// (no attempts reconcile, which would decode it); the reserved route keeps the App from
		// dispatching it to any handler, and the App dead-letters it directly, with no retry.
		v = babelqueue.TruncateDeclaredVersion(v) // bound what the hook, the route and the DLQ record carry
		notifyRejected(t.onRejected, v, queue)
		return &babelqueue.ReceivedMessage{
			Body:          string(m.Body),
			Queue:         queue,
			Handle:        m,
			DeliveryCount: int(m.DeliveryCount),
			RouteURN:      unsupportedSchemaRoutePrefix + url.QueryEscape(v),
		}, nil
	}
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

// Rewind implements [babelqueue.Rewinder]: it abandons the peek-lock so the message is
// redelivered right away instead of when the lock lapses. Nothing is completed or dead-lettered.
// A receiver without AbandonMessage (a test fake) leaves the lock to lapse on its own.
func (t *Transport) Rewind(ctx context.Context, msg *babelqueue.ReceivedMessage) error {
	rm, ok := msg.Handle.(*azservicebus.ReceivedMessage)
	if !ok || rm == nil {
		return nil
	}
	r, err := t.receiver(msg.Queue)
	if err != nil {
		return err
	}
	if ab, ok := r.(interface {
		AbandonMessage(ctx context.Context, message *azservicebus.ReceivedMessage, options *azservicebus.AbandonMessageOptions) error
	}); ok {
		return ab.AbandonMessage(ctx, rm, nil)
	}
	return nil
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

// unsupportedSchemaRoutePrefix is the reserved [babelqueue.ReceivedMessage.RouteURN] prefix of a
// version-gated delivery (the same string the core App recognises). It is not a "urn:babel:"
// URN, so no handler owns it; the App reports it as an unknown URN, which names the rejected
// bq-schema-version in the logged error. It is an in-process marker only and never reaches the wire.
const unsupportedSchemaRoutePrefix = "babelqueue:unsupported-schema-version:"

// unsupportedSchemaVersion implements the §4.7 gate on the bq-schema-version application
// property. A missing property is not gated, and neither is a blank string (the usual decode path
// runs); blank means the empty string or only ASCII whitespace (space, \t, \n, \v, \f, \r) — the
// same set in every SDK (GR-5). Application properties are AMQP-typed, so the supported version is
// accepted either as the string "1" (compared as-is, never trimmed: "01", " 1", NBSP, U+0085,
// U+FEFF and U+3000 are all rejected) or as a native numeric 1 of any integer width, signed or
// unsigned. A null value counts as absent. Any other number (2, 0, 1.5), every floating-point value
// (1.0 included) and any other type (bool, bytes, ...) is rejected.
// It returns the offending value rendered as text.
func unsupportedSchemaVersion(props map[string]any) (string, bool) {
	raw, ok := props["bq-schema-version"]
	if !ok {
		return "", false
	}
	supported := babelqueue.SchemaVersion
	switch v := raw.(type) {
	case nil:
		// A null-valued property carries no version, i.e. it is absent (same as Java and .NET).
		return "", false
	case string:
		if isBlankASCII(v) || v == strconv.Itoa(supported) {
			return "", false
		}
		return v, true
	case int:
		return fmt.Sprint(v), v != supported
	case int8:
		return fmt.Sprint(v), int64(v) != int64(supported)
	case int16:
		return fmt.Sprint(v), int64(v) != int64(supported)
	case int32:
		return fmt.Sprint(v), int64(v) != int64(supported)
	case int64:
		return fmt.Sprint(v), v != int64(supported)
	case uint:
		return fmt.Sprint(v), uint64(v) != uint64(supported)
	case uint8:
		return fmt.Sprint(v), uint64(v) != uint64(supported)
	case uint16:
		return fmt.Sprint(v), uint64(v) != uint64(supported)
	case uint32:
		return fmt.Sprint(v), uint64(v) != uint64(supported)
	case uint64:
		return fmt.Sprint(v), v != uint64(supported)
	case float32, float64:
		// A floating-point value is never the supported version, 1.0 included: only an integer or
		// the string "1" names it (same as Java and .NET).
		return fmt.Sprint(v), true
	default:
		return fmt.Sprintf("%v", raw), true
	}
}

// isBlankASCII reports whether s is empty or made only of ASCII whitespace (space, \t, \n, \v,
// \f, \r). Unlike strings.TrimSpace it does not treat Unicode spaces (U+0085, U+00A0, U+2028,
// U+3000, ...) as blank, so every SDK agrees on what a blank bq-schema-version is (GR-5).
func isBlankASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ' ', '\t', '\n', '\v', '\f', '\r':
		default:
			return false
		}
	}
	return true
}

// notifyRejected calls the hook, swallowing any panic so a faulty diagnostic can never disturb
// consumption.
func notifyRejected(fn OnSchemaVersionRejected, version, queue string) {
	if fn == nil {
		return
	}
	defer func() { _ = recover() }()
	fn(version, queue)
}
