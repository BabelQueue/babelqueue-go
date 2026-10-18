package redis

import (
	"context"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// laravelTransport returns a Laravel-layout transport on a scratch queue, or skips when no
// broker is reachable (CI provides a Redis service container).
func laravelTransport(t *testing.T, queue string, opts ...Option) (*Transport, Keys) {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	tr, err := New(url, append([]Option{WithLaravelLayout("")}, opts...)...)
	if err != nil {
		skipOrFailNoRedis(t, "bad redis url: %v", err)
	}
	t.Cleanup(func() { tr.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := tr.client.Ping(ctx).Err(); err != nil {
		skipOrFailNoRedis(t, "no redis broker: %v", err)
	}
	k := tr.Keys(queue)
	clean := func() { tr.client.Del(context.Background(), k.Ready, k.Delayed, k.Reserved, k.Notify) }
	clean()
	t.Cleanup(clean)
	return tr, k
}

func TestLaravelLayoutReserveAck(t *testing.T) {
	tr, k := laravelTransport(t, "bq-laravel-reserve")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := tr.Publish(ctx, "bq-laravel-reserve", `{"job":"urn:babel:t:a"}`); err != nil {
		t.Fatal(err)
	}
	if n, _ := tr.client.LLen(ctx, k.Ready).Result(); n != 1 {
		t.Fatalf("ready length = %d, want 1", n)
	}
	if n, _ := tr.client.LLen(ctx, k.Notify).Result(); n != 1 {
		t.Fatalf("notify length = %d, want 1", n)
	}

	msg, err := tr.Pop(ctx, "bq-laravel-reserve", time.Second)
	if err != nil || msg == nil {
		t.Fatalf("pop: %v %v", msg, err)
	}
	if n, _ := tr.client.ZCard(ctx, k.Reserved).Result(); n != 1 {
		t.Fatalf("reserved card = %d, want 1", n)
	}
	if err := tr.Ack(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if n, _ := tr.client.ZCard(ctx, k.Reserved).Result(); n != 0 {
		t.Fatalf("reserved card after ack = %d, want 0", n)
	}
	if empty, err := tr.Pop(ctx, "bq-laravel-reserve", time.Second); err != nil || empty != nil {
		t.Fatalf("expected an empty queue, got %v %v", empty, err)
	}
}

func TestLaravelLayoutMigratesDelayedAndExpired(t *testing.T) {
	tr, k := laravelTransport(t, "bq-laravel-migrate", WithRetryAfter(-time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	past := float64(time.Now().Add(-time.Minute).Unix())
	if err := tr.client.ZAdd(ctx, k.Delayed, goredis.Z{Score: past, Member: "due-delayed"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := tr.client.ZAdd(ctx, k.Delayed, goredis.Z{Score: float64(time.Now().Add(time.Hour).Unix()), Member: "future"}).Err(); err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(ctx, "bq-laravel-migrate", time.Second)
	if err != nil || msg == nil || msg.Body != "due-delayed" {
		t.Fatalf("delayed pop = %v %v", msg, err)
	}
	// The reservation deadline is already past, so the next pop makes it ready again.
	again, err := tr.Pop(ctx, "bq-laravel-migrate", time.Second)
	if err != nil || again == nil || again.Body != "due-delayed" {
		t.Fatalf("expired reservation was not re-readied: %v %v", again, err)
	}
	if n, _ := tr.client.ZCard(ctx, k.Delayed).Result(); n != 1 {
		t.Fatalf("future member must stay delayed, card = %d", n)
	}
}

// Regression (broker-bindings §1): the notify list is only a wake-up hint. A value on the ready
// list with no notify token must still be reserved, and Pop must not sit out its whole blocking
// timeout waiting for a token that will never come.
func TestLaravelLayoutPopReservesReadyValueWithoutNotifyToken(t *testing.T) {
	tr, k := laravelTransport(t, "bq-laravel-starve")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := tr.client.RPush(ctx, k.Ready, `{"job":"urn:babel:t:a"}`).Err(); err != nil {
		t.Fatal(err)
	}
	if n, _ := tr.client.LLen(ctx, k.Notify).Result(); n != 0 {
		t.Fatalf("precondition: notify length = %d, want 0", n)
	}
	start := time.Now()
	msg, err := tr.Pop(ctx, "bq-laravel-starve", 3*time.Second)
	if err != nil || msg == nil {
		t.Fatalf("a ready value without a notify token was starved: %v %v", msg, err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Errorf("pop took %v; it must reserve the ready value instead of blocking on notify", elapsed)
	}
	if n, _ := tr.client.ZCard(ctx, k.Reserved).Result(); n != 1 {
		t.Fatalf("reserved card = %d, want 1", n)
	}
}

// Regression: the reserve script consumes one notify token per reserved value (like Laravel's
// pop script), so the notify list does not grow without bound under steady publish/consume.
func TestLaravelLayoutNotifyListDoesNotGrow(t *testing.T) {
	tr, k := laravelTransport(t, "bq-laravel-notify")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const n = 5
	for i := 0; i < n; i++ {
		if err := tr.Publish(ctx, "bq-laravel-notify", `{"job":"urn:babel:t:a"}`); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := tr.client.LLen(ctx, k.Notify).Result(); got != n {
		t.Fatalf("notify length after %d publishes = %d, want %d", n, got, n)
	}
	for i := 0; i < n; i++ {
		msg, err := tr.Pop(ctx, "bq-laravel-notify", time.Second)
		if err != nil || msg == nil {
			t.Fatalf("pop %d: %v %v", i, msg, err)
		}
		if err := tr.Ack(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := tr.client.LLen(ctx, k.Notify).Result(); got != 0 {
		t.Fatalf("notify length after consuming everything = %d, want 0", got)
	}
	if got, _ := tr.client.LLen(ctx, k.Ready).Result(); got != 0 {
		t.Fatalf("ready length = %d, want 0", got)
	}
}

// Regression: timeout <= 0 means "wait indefinitely" (as in the default layout's BLMOVE), not
// "return immediately" — an App configured with WithPollTimeout(0) must not spin on an idle queue.
func TestLaravelLayoutZeroTimeoutWaitsForAValue(t *testing.T) {
	tr, _ := laravelTransport(t, "bq-laravel-zero")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = tr.Publish(ctx, "bq-laravel-zero", `{"job":"urn:babel:t:a"}`)
	}()
	start := time.Now()
	msg, err := tr.Pop(ctx, "bq-laravel-zero", 0)
	if err != nil || msg == nil {
		t.Fatalf("zero-timeout pop returned %v %v, want the value published later", msg, err)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Errorf("pop returned after %v; it must have waited for the publish", elapsed)
	}
}

// An indefinite wait still migrates a delayed member that falls due while the queue is idle,
// and still ends with the context.
func TestLaravelLayoutZeroTimeoutMigratesAndHonoursContext(t *testing.T) {
	tr, k := laravelTransport(t, "bq-laravel-zero-migrate")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	due := float64(time.Now().Add(1500 * time.Millisecond).Unix())
	if err := tr.client.ZAdd(ctx, k.Delayed, goredis.Z{Score: due, Member: "soon"}).Err(); err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(ctx, "bq-laravel-zero-migrate", 0)
	if err != nil || msg == nil || msg.Body != "soon" {
		t.Fatalf("delayed member not delivered during an indefinite wait: %v %v", msg, err)
	}

	short, stop := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stop()
	if m, err := tr.Pop(short, "bq-laravel-zero-migrate", 0); err == nil {
		t.Fatalf("pop on an empty queue should end with the context error, got %v", m)
	}
}
