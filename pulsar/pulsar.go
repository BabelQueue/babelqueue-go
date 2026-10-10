// Package pulsar is an Apache Pulsar-backed [babelqueue.Transport] for the BabelQueue
// Go runtime, on the pure-Go [github.com/apache/pulsar-client-go] (no CGo, no libpulsar).
// Producing sends the canonical envelope as the message payload and projects the contract
// envelope fields onto native Pulsar message properties (string→string): bq-job = URN,
// bq-trace-id = trace_id, bq-message-id = meta.id, plus bq-schema-version / bq-source-lang /
// bq-attempts — so a Java/.NET/... peer can route on bq-job without parsing the body.
// Consuming receives one message at a time (Receive -> process -> Ack). Per §5.2 / §5.7 the
// bq-job property is the routing key (the body's job is used only when bq-job is absent or
// blank; a differing body URN is reported to the optional [OnRoutingMismatch] hook) and
// bq-attempts is the authoritative counter (the body's attempts is used only when the property
// is absent or not a plain non-negative decimal integer). The result is then
// raised to the broker's native RedeliveryCount (no −1, because Pulsar's redelivery count is
// 0-based): attempts = max(bq-attempts, RedeliveryCount), so a redelivered message reflects
// its true count without lowering the runtime's own counter.
//
//	tr, _ := pulsar.New(pulsar.WithURL("pulsar://localhost:6650"))
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// This binding implements §5 of the BabelQueue broker-bindings contract. The envelope is
// unchanged (schema_version stays 1); Apache Pulsar is purely additive.
//
// Full spec: https://babelqueue.com
package pulsar

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	pulsarsdk "github.com/apache/pulsar-client-go/pulsar"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

// Producer is the subset of [pulsarsdk.Producer] the transport uses; the concrete producer
// satisfies it, and a fake satisfies it in tests.
type Producer interface {
	Send(ctx context.Context, msg *pulsarsdk.ProducerMessage) (pulsarsdk.MessageID, error)
}

// Consumer is the subset of [pulsarsdk.Consumer] the transport uses.
type Consumer interface {
	Receive(ctx context.Context) (pulsarsdk.Message, error)
	Ack(msg pulsarsdk.Message) error
	Nack(msg pulsarsdk.Message)
}

// Client creates per-queue producers and subscriptions. A [pulsarsdk.Client] is wrapped to
// satisfy it (see [WithPulsarClient]); a fake satisfies it in tests.
type Client interface {
	CreateProducer(queue string) (Producer, error)
	Subscribe(queue string) (Consumer, error)
}

// pulsarClient adapts a real [pulsarsdk.Client] to the [Client] seam.
type pulsarClient struct {
	c            pulsarsdk.Client
	subscription string
	consumerType pulsarsdk.SubscriptionType
	topicPrefix  string
}

func (p pulsarClient) topic(queue string) string { return p.topicPrefix + queue }

func (p pulsarClient) CreateProducer(queue string) (Producer, error) {
	return p.c.CreateProducer(pulsarsdk.ProducerOptions{Topic: p.topic(queue)})
}

func (p pulsarClient) Subscribe(queue string) (Consumer, error) {
	return p.c.Subscribe(pulsarsdk.ConsumerOptions{
		Topic:            p.topic(queue),
		SubscriptionName: p.subscription,
		Type:             p.consumerType,
	})
}

// Transport implements [babelqueue.Transport] over Apache Pulsar. It is safe for concurrent
// use; the per-queue producer/consumer caches are guarded by a mutex.
type Transport struct {
	client       Client
	rawClient    pulsarsdk.Client
	url          string
	subscription string
	consumerType pulsarsdk.SubscriptionType
	topicPrefix  string
	maxWait      time.Duration
	onMismatch   OnRoutingMismatch

	mu        sync.Mutex
	producers map[string]Producer
	consumers map[string]Consumer
}

// Option customizes [New].
type Option func(*Transport)

// WithURL builds the client from a Pulsar service URL (pulsar:// or pulsar+ssl://).
func WithURL(url string) Option { return func(t *Transport) { t.url = url } }

// WithPulsarClient wraps a preconfigured [pulsarsdk.Client] (e.g. built with TLS or auth),
// bypassing URL parsing.
func WithPulsarClient(c pulsarsdk.Client) Option { return func(t *Transport) { t.rawClient = c } }

// WithClient injects a [Client] (a fake in tests), bypassing all Pulsar wiring.
func WithClient(c Client) Option { return func(t *Transport) { t.client = c } }

// WithSubscription sets the subscription name (default "babelqueue").
func WithSubscription(name string) Option { return func(t *Transport) { t.subscription = name } }

// WithSubscriptionType sets the subscription type (default [pulsarsdk.Shared]).
func WithSubscriptionType(st pulsarsdk.SubscriptionType) Option {
	return func(t *Transport) { t.consumerType = st }
}

// WithTopicPrefix prefixes every queue name to form the topic (e.g.
// "persistent://public/default/"). Empty by default — the bare queue name is the topic.
func WithTopicPrefix(prefix string) Option { return func(t *Transport) { t.topicPrefix = prefix } }

// WithMaxWaitTime caps how long a single Pop blocks waiting for a message.
func WithMaxWaitTime(d time.Duration) Option { return func(t *Transport) { t.maxWait = d } }

// OnRoutingMismatch is called when a message's bq-job property and its body's job name
// different URNs (the property wins and the message is routed on it). propertyURN is the
// trimmed bq-job value, bodyURN the body's job, and env the accepted envelope as the handler
// will see it (body unchanged, attempts reconciled) — treat it as read-only. It is not called
// for an envelope the consumer rejects. It is a diagnostic hook,
// separate from any error handling: a panic in it is recovered and ignored, and it never
// changes how the message is delivered, acked or retried.
type OnRoutingMismatch func(propertyURN, bodyURN string, env *babelqueue.Envelope)

// WithOnRoutingMismatch registers an optional [OnRoutingMismatch] hook (default: none).
func WithOnRoutingMismatch(fn OnRoutingMismatch) Option {
	return func(t *Transport) { t.onMismatch = fn }
}

// New builds a transport. Provide a service URL ([WithURL]), a preconfigured Pulsar client
// ([WithPulsarClient]), or an injected [Client] ([WithClient]).
func New(opts ...Option) (*Transport, error) {
	t := newTransport(opts...)
	if t.client != nil {
		return t, nil
	}
	if t.rawClient == nil && t.url != "" {
		c, err := pulsarsdk.NewClient(pulsarsdk.ClientOptions{URL: t.url})
		if err != nil {
			return nil, err
		}
		t.rawClient = c
	}
	if t.rawClient != nil {
		t.client = pulsarClient{
			c:            t.rawClient,
			subscription: t.subscription,
			consumerType: t.consumerType,
			topicPrefix:  t.topicPrefix,
		}
		return t, nil
	}
	return nil, errors.New("pulsar: provide WithURL, WithPulsarClient, or WithClient")
}

func newTransport(opts ...Option) *Transport {
	t := &Transport{
		subscription: "babelqueue",
		consumerType: pulsarsdk.Shared,
		producers:    make(map[string]Producer),
		consumers:    make(map[string]Consumer),
	}
	for _, o := range opts {
		o(t)
	}
	return t
}

func (t *Transport) producer(queue string) (Producer, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok := t.producers[queue]; ok {
		return p, nil
	}
	p, err := t.client.CreateProducer(queue)
	if err != nil {
		return nil, err
	}
	t.producers[queue] = p
	return p, nil
}

func (t *Transport) consumer(queue string) (Consumer, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.consumers[queue]; ok {
		return c, nil
	}
	c, err := t.client.Subscribe(queue)
	if err != nil {
		return nil, err
	}
	t.consumers[queue] = c
	return c, nil
}

// Publish sends body to queue with the §5 property projection.
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	return t.PublishRouted(ctx, queue, body, "")
}

// PublishRouted is [Publish] with the routing property pinned: a non-empty routeURN is written
// as bq-job instead of the body's job, so an App retry or release re-publish keeps the original
// delivery's routing URN (§5.2/§5.7). A blank routeURN derives bq-job from the body.
func (t *Transport) PublishRouted(ctx context.Context, queue, body, routeURN string) error {
	p, err := t.producer(queue)
	if err != nil {
		return err
	}
	_, err = p.Send(ctx, message(body, routeURN))
	return err
}

// Pop receives the next message, bounded by timeout (and WithMaxWaitTime). It reconciles
// the routing URN (bq-job, falling back to the body; set on ReceivedMessage.RouteURN, the body
// is not rewritten) and attempts (bq-attempts, falling back to the body, floored at
// RedeliveryCount; written into the body). Returns (nil, nil) when no message arrives.
func (t *Transport) Pop(ctx context.Context, queue string, timeout time.Duration) (*babelqueue.ReceivedMessage, error) {
	c, err := t.consumer(queue)
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
	msg, err := c.Receive(rctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, nil
		}
		return nil, err
	}
	body, routeURN := reconcile(string(msg.Payload()), msg.Properties(), msg.RedeliveryCount(), t.onMismatch)
	return &babelqueue.ReceivedMessage{Body: body, Queue: queue, Handle: msg, RouteURN: routeURN}, nil
}

// Ack acknowledges the reserved message.
func (t *Transport) Ack(_ context.Context, msg *babelqueue.ReceivedMessage) error {
	m, ok := msg.Handle.(pulsarsdk.Message)
	if !ok {
		return nil
	}
	c, err := t.consumer(msg.Queue)
	if err != nil {
		return err
	}
	return c.Ack(m)
}

// message projects the envelope's contract fields onto a native Pulsar producer message.
// The properties are a routable view of the body; on the consume side bq-job and bq-attempts
// are authoritative (§5.7).
func message(body, routeURN string) *pulsarsdk.ProducerMessage {
	m := &pulsarsdk.ProducerMessage{Payload: []byte(body)}
	env, err := babelqueue.Decode([]byte(body))
	if err != nil {
		return m
	}
	m.Properties = properties(env)
	if routeURN != "" {
		m.Properties["bq-job"] = routeURN
	}
	return m
}

// properties builds the bq-* string→string projection from the envelope.
func properties(env babelqueue.Envelope) map[string]string {
	props := make(map[string]string, 6)
	if env.Job != "" {
		props["bq-job"] = env.Job
	}
	if env.TraceID != "" {
		props["bq-trace-id"] = env.TraceID
	}
	if env.Meta.ID != "" {
		props["bq-message-id"] = env.Meta.ID
	}
	if env.Meta.SchemaVersion != 0 {
		props["bq-schema-version"] = strconv.Itoa(env.Meta.SchemaVersion)
	}
	if env.Meta.Lang != "" {
		props["bq-source-lang"] = env.Meta.Lang
	}
	props["bq-attempts"] = strconv.Itoa(env.Attempts)
	return props
}

// maxAttempts bounds an accepted bq-attempts value to a signed 32-bit integer, matching the
// Java and .NET consumers so every SDK treats the same property value alike (GR-5).
const maxAttempts = 1<<31 - 1

// parseAttempts reads the bq-attempts property: valid only when it is a non-empty run of ASCII
// digits (no sign, no whitespace) that fits a 32-bit integer.
func parseAttempts(raw string) (int, bool) {
	if raw == "" {
		return 0, false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n > maxAttempts {
		return 0, false
	}
	return int(n), true
}

// reconcile applies §5.2 / §5.7 to a received body and returns the body to hand to the
// [babelqueue.App] plus the routing URN to set on the message:
//
//   - routing: a non-blank bq-job property is the URN the App routes on. It is returned as
//     the routing URN ([babelqueue.ReceivedMessage].RouteURN) and the body's job / urn alias
//     are never rewritten, so the handler, retries and the dead-letter record all keep the
//     producer's original body. A body with no job gets no routing URN — it stays
//     non-conformant and the App rejects it, whatever bq-job says.
//   - attempts: a valid bq-attempts property replaces the body's value, falling back to the
//     body's when absent or invalid; the result is then raised to RedeliveryCount. Pulsar's
//     redelivery count is 0-based (0 on first delivery) so it maps directly with no −1.
//
// onMismatch (optional) is told about a bq-job / body job mismatch, but only for an envelope
// the consumer accepts and after attempts are reconciled (as in the Java and .NET consumers),
// so it sees the same envelope the handler will get; see [OnRoutingMismatch].
//
// An undecodable body is returned unchanged with no routing URN (the App treats it as poison).
func reconcile(body string, props map[string]string, redeliveryCount uint32, onMismatch OnRoutingMismatch) (string, string) {
	env, err := babelqueue.Decode([]byte(body))
	if err != nil {
		return body, ""
	}
	routeURN := ""
	if routed := strings.TrimSpace(props["bq-job"]); routed != "" && env.Job != "" {
		routeURN = routed
	}
	attempts := env.Attempts
	if n, ok := parseAttempts(props["bq-attempts"]); ok {
		attempts = n
	}
	if native := int(redeliveryCount); native > attempts {
		attempts = native
	}
	out := body
	if attempts != env.Attempts {
		env.Attempts = attempts
		if b, err := env.Encode(); err == nil {
			out = string(b)
		}
	}
	if routeURN != "" && routeURN != env.Job && env.Accepts() {
		notifyMismatch(onMismatch, routeURN, env)
	}
	return out, routeURN
}

// notifyMismatch calls the hook, swallowing any panic so a faulty diagnostic can never
// disturb consumption.
func notifyMismatch(fn OnRoutingMismatch, propertyURN string, env babelqueue.Envelope) {
	if fn == nil {
		return
	}
	defer func() { _ = recover() }()
	fn(propertyURN, env.Job, &env)
}

var _ babelqueue.Transport = (*Transport)(nil)
