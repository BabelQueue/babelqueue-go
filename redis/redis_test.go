package redis_test

import (
	"context"
	"os"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	bqredis "github.com/babelqueue/babelqueue-go/redis"
)

// brokerURL returns the Redis URL to test against, defaulting to localhost.
func brokerURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://localhost:6379/0"
}

// TestRedisRoundTrip is an integration test: it skips unless a Redis broker is
// reachable (CI provides one as a service container).
func TestRedisRoundTrip(t *testing.T) {
	tr, err := bqredis.New(brokerURL())
	if err != nil {
		t.Skipf("bad redis url: %v", err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const queue = "bq-test-redis"

	env, err := babelqueue.Make("urn:babel:test:ping", map[string]any{"n": 1}, babelqueue.WithQueue(queue))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := env.Encode()

	if err := tr.Publish(ctx, queue, string(body)); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}

	msg, err := tr.Pop(ctx, queue, time.Second)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if msg == nil {
		t.Fatal("expected a message")
	}
	if err := tr.Ack(ctx, msg); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	got, err := babelqueue.Decode([]byte(msg.Body))
	if err != nil || !got.Accepts() {
		t.Fatalf("decoded message not accepted: %v", err)
	}
	if got.URN() != "urn:babel:test:ping" {
		t.Errorf("urn = %q", got.URN())
	}
}

// TestRedisHeaderRoundTrip is an integration test (skips without a broker): a message
// published with a traceparent header (ADR-0028) is delivered back with it on
// ReceivedMessage.Headers, the body is the unchanged wire envelope, and Ack succeeds —
// proving the LREM handle still matches the stored frame.
func TestRedisHeaderRoundTrip(t *testing.T) {
	tr, err := bqredis.New(brokerURL())
	if err != nil {
		t.Skipf("bad redis url: %v", err)
	}
	defer tr.Close()

	// Transport implements the optional HeaderPublisher capability after ADR-0028.
	hp, ok := any(tr).(babelqueue.HeaderPublisher)
	if !ok {
		t.Fatal("redis.Transport must implement babelqueue.HeaderPublisher")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const queue = "bq-test-redis-headers"
	const traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

	env, err := babelqueue.Make("urn:babel:test:ping", map[string]any{"n": 1}, babelqueue.WithQueue(queue))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := env.Encode()

	if err := hp.PublishWithHeaders(ctx, queue, string(body), map[string]string{"traceparent": traceparent}); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}

	msg, err := tr.Pop(ctx, queue, time.Second)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if msg == nil {
		t.Fatal("expected a message")
	}
	if got := msg.Headers["traceparent"]; got != traceparent {
		t.Errorf("Headers[traceparent] = %q, want %q", got, traceparent)
	}
	if msg.Body != string(body) {
		t.Errorf("Body = %q, want the unchanged wire envelope %q", msg.Body, string(body))
	}
	if err := tr.Ack(ctx, msg); err != nil {
		t.Fatalf("Ack (handle must match the stored frame): %v", err)
	}
}

// TestRedisBareValueBackCompat is an integration test (skips without a broker): a value
// produced by plain Publish (a bare envelope, e.g. an older or non-otel publisher) still
// consumes correctly — Headers is nil and Ack matches on the bare value as the handle.
func TestRedisBareValueBackCompat(t *testing.T) {
	tr, err := bqredis.New(brokerURL())
	if err != nil {
		t.Skipf("bad redis url: %v", err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const queue = "bq-test-redis-bare"

	env, err := babelqueue.Make("urn:babel:test:ping", map[string]any{"n": 2}, babelqueue.WithQueue(queue))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := env.Encode()

	if err := tr.Publish(ctx, queue, string(body)); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}

	msg, err := tr.Pop(ctx, queue, time.Second)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if msg == nil {
		t.Fatal("expected a message")
	}
	if msg.Headers != nil {
		t.Errorf("Headers = %v, want nil for a bare value", msg.Headers)
	}
	if msg.Body != string(body) {
		t.Errorf("Body = %q, want the bare envelope verbatim", msg.Body)
	}
	if err := tr.Ack(ctx, msg); err != nil {
		t.Fatalf("Ack (handle must equal the bare value): %v", err)
	}
}

// TestRedisAppEndToEnd runs a publish + drain through the App over Redis.
func TestRedisAppEndToEnd(t *testing.T) {
	tr, err := bqredis.New(brokerURL())
	if err != nil {
		t.Skipf("bad redis url: %v", err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := tr.Publish(ctx, "bq-test-redis-app", ""); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}
	// drain the probe we just published
	_, _ = tr.Pop(ctx, "bq-test-redis-app", time.Second)

	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("bq-test-redis-app"))
	done := make(chan babelqueue.Envelope, 1)
	app.Handle("urn:babel:test:created", func(_ context.Context, env babelqueue.Envelope) error {
		done <- env
		return nil
	})

	if _, err := app.Publish(ctx, "urn:babel:test:created", map[string]any{"order_id": 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(ctx, "bq-test-redis-app", 1); err != nil {
		t.Fatal(err)
	}

	select {
	case env := <-done:
		if env.URN() != "urn:babel:test:created" {
			t.Errorf("urn = %q", env.URN())
		}
	default:
		t.Fatal("handler was not invoked")
	}
}
