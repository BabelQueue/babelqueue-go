// Package redis is a Redis-backed [babelqueue.Transport] for the BabelQueue Go
// runtime. It uses the reliable-queue pattern: RPUSH to produce, BLMOVE the head
// to a per-queue processing list to reserve (so an in-flight message survives a
// worker crash), and LREM from that list to acknowledge.
//
//	tr, _ := redis.New("redis://localhost:6379/0")
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// This is a Go-owned reliable queue; full parity with Laravel's reserved-set
// reservation on a *shared* Redis queue is a separate task.
package redis

import (
	"context"
	"errors"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	goredis "github.com/redis/go-redis/v9"
)

// Transport implements babelqueue.Transport over Redis.
type Transport struct {
	client           *goredis.Client
	processingSuffix string
}

// Option customizes New / NewWithClient.
type Option func(*Transport)

// WithProcessingSuffix overrides the per-queue processing-list suffix
// (default ":processing").
func WithProcessingSuffix(suffix string) Option {
	return func(t *Transport) { t.processingSuffix = suffix }
}

// New connects to Redis from a redis:// or rediss:// URL.
func New(url string, opts ...Option) (*Transport, error) {
	options, err := goredis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return NewWithClient(goredis.NewClient(options), opts...), nil
}

// NewWithClient wraps an existing go-redis client (bring your own pool/TLS/auth).
func NewWithClient(client *goredis.Client, opts ...Option) *Transport {
	t := &Transport{client: client, processingSuffix: ":processing"}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *Transport) processing(queue string) string {
	return queue + t.processingSuffix
}

// Publish appends body to queue (RPUSH).
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	return t.client.RPush(ctx, queue, body).Err()
}

// Pop atomically moves the head of queue to its processing list and returns it,
// blocking up to timeout. It returns (nil, nil) when no message arrives in time.
func (t *Transport) Pop(ctx context.Context, queue string, timeout time.Duration) (*babelqueue.ReceivedMessage, error) {
	body, err := t.client.BLMove(ctx, queue, t.processing(queue), "LEFT", "RIGHT", timeout).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &babelqueue.ReceivedMessage{Body: body, Queue: queue, Handle: body}, nil
}

// Ack removes the reserved message from its processing list (LREM).
func (t *Transport) Ack(ctx context.Context, msg *babelqueue.ReceivedMessage) error {
	handle, _ := msg.Handle.(string)
	return t.client.LRem(ctx, t.processing(msg.Queue), 1, handle).Err()
}

// Close releases the underlying client.
func (t *Transport) Close() error {
	return t.client.Close()
}

var _ babelqueue.Transport = (*Transport)(nil)
