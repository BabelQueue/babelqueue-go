// Package sqs is an Amazon SQS-backed [babelqueue.Transport] for the BabelQueue
// Go runtime. Producing sends the canonical envelope as the message body and
// projects the contract envelope fields onto native SQS MessageAttributes
// (bq-job = URN, bq-trace-id = trace_id, bq-message-id = meta.id, plus
// bq-schema-version / bq-source-lang / bq-created-at) — so a PHP/Python/... peer
// can route on bq-job and correlate on bq-trace-id without parsing the body.
// Consuming uses the visibility-timeout reservation model (ReceiveMessage →
// process → DeleteMessage); the authoritative attempt count is the broker's
// ApproximateReceiveCount, reconciled onto the envelope as attempts = count − 1.
//
//	tr, _ := sqs.New(ctx, sqs.WithRegion("eu-central-1"))
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// This binding implements §3 of the BabelQueue broker-bindings contract. The
// envelope is unchanged (schema_version stays 1); SQS is purely additive.
//
// Full spec: https://babelqueue.com
package sqs

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	babelqueue "github.com/babelqueue/babelqueue-go"
)

// API is the subset of the AWS SQS client the transport uses. The concrete
// *github.com/aws/aws-sdk-go-v2/service/sqs.Client satisfies it; a fake satisfies
// it in tests.
type API interface {
	SendMessage(ctx context.Context, in *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	GetQueueUrl(ctx context.Context, in *awssqs.GetQueueUrlInput, optFns ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error)
}

// Transport implements [babelqueue.Transport] over Amazon SQS. It is safe for
// concurrent use; the queue-URL cache is guarded by a mutex.
type Transport struct {
	client API

	region            string
	endpoint          string
	queueURLPrefix    string
	waitTimeSeconds   int32
	visibilityTimeout int32
	fifo              bool
	messageGroupID    string
	contentDedup      bool

	mu   sync.Mutex
	urls map[string]string // queue name -> queue URL
}

// Option customizes New / NewWithClient.
type Option func(*Transport)

// WithRegion sets the AWS region (otherwise resolved from the environment/config).
func WithRegion(region string) Option { return func(t *Transport) { t.region = region } }

// WithEndpoint overrides the SQS endpoint — point it at LocalStack/ElasticMQ for
// local/CI (e.g. "http://localhost:4566").
func WithEndpoint(endpoint string) Option { return func(t *Transport) { t.endpoint = endpoint } }

// WithQueueURLPrefix sets a base queue-URL so a queue name resolves to
// "<prefix>/<name>" without a GetQueueUrl call (e.g.
// "https://sqs.eu-central-1.amazonaws.com/123456789012"). When unset, the
// transport resolves names via GetQueueUrl and caches the result.
func WithQueueURLPrefix(prefix string) Option {
	return func(t *Transport) { t.queueURLPrefix = prefix }
}

// WithWaitTimeSeconds caps the ReceiveMessage long-poll wait (0–20). The runtime's
// per-iteration poll timeout still bounds it; this caps the upper limit.
func WithWaitTimeSeconds(seconds int32) Option {
	return func(t *Transport) { t.waitTimeSeconds = seconds }
}

// WithVisibilityTimeout sets the reservation window (seconds) applied on receive.
// Zero leaves the queue default.
func WithVisibilityTimeout(seconds int32) Option {
	return func(t *Transport) { t.visibilityTimeout = seconds }
}

// WithFIFO marks the queue as FIFO (its name must end in ".fifo"). Sends then
// carry a MessageGroupId (see [WithMessageGroupID], default the queue name) and,
// unless content-based dedup is enabled, a MessageDeduplicationId = meta.id.
func WithFIFO(enabled bool) Option { return func(t *Transport) { t.fifo = enabled } }

// WithMessageGroupID sets the FIFO ordering group (default: the queue name).
func WithMessageGroupID(id string) Option { return func(t *Transport) { t.messageGroupID = id } }

// WithContentDedup uses the queue's content-based deduplication instead of
// meta.id as the MessageDeduplicationId (FIFO only).
func WithContentDedup(enabled bool) Option { return func(t *Transport) { t.contentDedup = enabled } }

// WithClient injects a preconfigured SQS client (or a fake in tests), bypassing
// AWS config loading.
func WithClient(client API) Option { return func(t *Transport) { t.client = client } }

// New builds a transport, loading AWS configuration (the default credential
// provider chain) unless a client was injected with [WithClient]. Apply
// [WithRegion]/[WithEndpoint] to target a region or LocalStack/ElasticMQ.
func New(ctx context.Context, opts ...Option) (*Transport, error) {
	t := newTransport(opts...)
	if t.client != nil {
		return t, nil
	}
	var loadOpts []func(*config.LoadOptions) error
	if t.region != "" {
		loadOpts = append(loadOpts, config.WithRegion(t.region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}
	t.client = awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		if t.endpoint != "" {
			o.BaseEndpoint = aws.String(t.endpoint)
		}
	})
	return t, nil
}

// NewWithClient builds a transport over a preconfigured client (or fake). It never
// touches AWS config; useful for tests and dependency injection.
func NewWithClient(client API, opts ...Option) *Transport {
	t := newTransport(opts...)
	t.client = client
	return t
}

func newTransport(opts ...Option) *Transport {
	t := &Transport{urls: make(map[string]string)}
	for _, o := range opts {
		o(t)
	}
	return t
}

// Publish sends body to queue with the contract MessageAttributes projection.
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	url, err := t.resolveURL(ctx, queue)
	if err != nil {
		return err
	}
	in := &awssqs.SendMessageInput{
		QueueUrl:          aws.String(url),
		MessageBody:       aws.String(body),
		MessageAttributes: attributes(body),
	}
	if t.fifo {
		group := t.messageGroupID
		if group == "" {
			group = queue
		}
		in.MessageGroupId = aws.String(group)
		if !t.contentDedup {
			if env, derr := babelqueue.Decode([]byte(body)); derr == nil && env.Meta.ID != "" {
				in.MessageDeduplicationId = aws.String(env.Meta.ID)
			}
		}
	}
	_, err = t.client.SendMessage(ctx, in)
	return err
}

// Pop reserves the next message (long-poll up to timeout, capped at 20s and by
// WithWaitTimeSeconds). It reconciles attempts to max(body.attempts,
// ApproximateReceiveCount − 1) so a crash-redelivered message reflects its true
// delivery count. Returns (nil, nil) when no message arrives.
func (t *Transport) Pop(ctx context.Context, queue string, timeout time.Duration) (*babelqueue.ReceivedMessage, error) {
	url, err := t.resolveURL(ctx, queue)
	if err != nil {
		return nil, err
	}
	wait := int32(timeout / time.Second)
	if wait < 0 {
		wait = 0
	}
	if wait > 20 {
		wait = 20
	}
	if t.waitTimeSeconds > 0 && t.waitTimeSeconds < wait {
		wait = t.waitTimeSeconds
	}
	in := &awssqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(url),
		MaxNumberOfMessages:         1,
		WaitTimeSeconds:             wait,
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	}
	if t.visibilityTimeout > 0 {
		in.VisibilityTimeout = t.visibilityTimeout
	}
	out, err := t.client.ReceiveMessage(ctx, in)
	if err != nil {
		return nil, err
	}
	if len(out.Messages) == 0 {
		return nil, nil
	}
	m := out.Messages[0]
	body := aws.ToString(m.Body)
	if rc, ok := m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; ok {
		body = reconcileAttempts(body, rc)
	}
	return &babelqueue.ReceivedMessage{Body: body, Queue: queue, Handle: aws.ToString(m.ReceiptHandle)}, nil
}

// Ack deletes the reserved message (DeleteMessage on its receipt handle).
func (t *Transport) Ack(ctx context.Context, msg *babelqueue.ReceivedMessage) error {
	handle, _ := msg.Handle.(string)
	if handle == "" {
		return nil
	}
	url, err := t.resolveURL(ctx, msg.Queue)
	if err != nil {
		return err
	}
	_, err = t.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: aws.String(handle),
	})
	return err
}

func (t *Transport) resolveURL(ctx context.Context, name string) (string, error) {
	t.mu.Lock()
	if u, ok := t.urls[name]; ok {
		t.mu.Unlock()
		return u, nil
	}
	t.mu.Unlock()

	var url string
	if t.queueURLPrefix != "" {
		url = strings.TrimRight(t.queueURLPrefix, "/") + "/" + name
	} else {
		out, err := t.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err != nil {
			return "", err
		}
		url = aws.ToString(out.QueueUrl)
	}

	t.mu.Lock()
	t.urls[name] = url
	t.mu.Unlock()
	return url, nil
}

// attributes projects the envelope's contract fields onto SQS MessageAttributes.
// They are a redundant, routable view of the body — the body stays authoritative.
func attributes(body string) map[string]types.MessageAttributeValue {
	env, err := babelqueue.Decode([]byte(body))
	if err != nil {
		return nil
	}
	str := func(v string) types.MessageAttributeValue {
		return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	num := func(v string) types.MessageAttributeValue {
		return types.MessageAttributeValue{DataType: aws.String("Number"), StringValue: aws.String(v)}
	}
	attrs := make(map[string]types.MessageAttributeValue, 6)
	if env.Job != "" {
		attrs["bq-job"] = str(env.Job)
	}
	if env.TraceID != "" {
		attrs["bq-trace-id"] = str(env.TraceID)
	}
	if env.Meta.ID != "" {
		attrs["bq-message-id"] = str(env.Meta.ID)
	}
	if env.Meta.SchemaVersion != 0 {
		attrs["bq-schema-version"] = num(strconv.Itoa(env.Meta.SchemaVersion))
	}
	if env.Meta.Lang != "" {
		attrs["bq-source-lang"] = str(env.Meta.Lang)
	}
	if env.Meta.CreatedAt != 0 {
		attrs["bq-created-at"] = num(strconv.FormatInt(env.Meta.CreatedAt, 10))
	}
	if len(attrs) == 0 {
		return nil
	}
	return attrs
}

// reconcileAttempts sets the envelope's top-level attempts to
// max(current, ApproximateReceiveCount − 1) so a first delivery reads 0 and a
// natively-redelivered message reflects its true count, without ever lowering a
// runtime-incremented counter.
func reconcileAttempts(body, receiveCount string) string {
	rc, err := strconv.Atoi(receiveCount)
	if err != nil || rc <= 1 {
		return body
	}
	env, err := babelqueue.Decode([]byte(body))
	if err != nil {
		return body
	}
	native := rc - 1
	if native <= env.Attempts {
		return body
	}
	env.Attempts = native
	if b, err := env.Encode(); err == nil {
		return string(b)
	}
	return body
}

var _ babelqueue.Transport = (*Transport)(nil)
