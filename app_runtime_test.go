package babelqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

func TestAppConsumeStopsOnContextCancel(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("q"), babelqueue.WithPollTimeout(time.Millisecond))

	done := make(chan struct{})
	app.Handle("urn:x", func(_ context.Context, _ babelqueue.Envelope) error {
		close(done)
		return nil
	})
	if _, err := app.Publish(context.Background(), "urn:x", map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- app.Consume(ctx) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("message was not consumed")
	}
	cancel()

	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume returned %v, want context.Canceled", err)
	}
}

func TestAppRunProcessesThenStops(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr, babelqueue.WithPollTimeout(time.Millisecond)) // default queue

	done := make(chan struct{})
	app.Handle("urn:y", func(_ context.Context, _ babelqueue.Envelope) error {
		close(done)
		return nil
	})
	if _, err := app.Publish(context.Background(), "urn:y", map[string]any{}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- app.Run(ctx) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("message was not consumed by Run")
	}
	cancel()
	<-errCh
}

func TestAppDeadLetterQueueOverride(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithMaxAttempts(1),
		babelqueue.WithDeadLetter(true),
		babelqueue.WithDeadLetterQueue("graveyard"),
		babelqueue.WithPollTimeout(time.Millisecond),
	)
	app.Handle("urn:boom", func(_ context.Context, _ babelqueue.Envelope) error {
		return errors.New("boom")
	})
	if _, err := app.Publish(context.Background(), "urn:boom", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 0); err != nil {
		t.Fatal(err)
	}

	if tr.Size("graveyard") != 1 {
		t.Fatalf("explicit dead-letter queue size = %d, want 1", tr.Size("graveyard"))
	}
	if tr.Size("orders.dlq") != 0 {
		t.Error("must not use the default suffix queue when an explicit DLQ is set")
	}
}

func TestAppDeadLetterSuffixOverride(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithMaxAttempts(1),
		babelqueue.WithDeadLetter(true),
		babelqueue.WithDeadLetterSuffix(".dead"),
	)
	app.Handle("urn:boom", func(_ context.Context, _ babelqueue.Envelope) error {
		return errors.New("boom")
	})
	if _, err := app.Publish(context.Background(), "urn:boom", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 0); err != nil {
		t.Fatal(err)
	}

	if tr.Size("orders.dead") != 1 {
		t.Fatalf("custom-suffix dead-letter queue size = %d, want 1", tr.Size("orders.dead"))
	}
}

func TestAppUnknownURNRelease(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("q"),
		babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease),
	)
	if _, err := app.Publish(context.Background(), "urn:unmapped", map[string]any{}); err != nil {
		t.Fatal(err)
	}

	n, err := app.Drain(context.Background(), "q", 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("processed %d, want 1", n)
	}
	if tr.Size("q") != 1 {
		t.Fatalf("release must re-publish to the same queue; size = %d, want 1", tr.Size("q"))
	}
}

func TestAppUnknownURNFailRequeues(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("q")) // default StrategyFail, maxAttempts 3

	if _, err := app.Publish(context.Background(), "urn:unmapped", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "q", 1); err != nil {
		t.Fatal(err)
	}

	// StrategyFail routes through the retry path: attempts 1 < 3 → requeued.
	if tr.Size("q") != 1 {
		t.Fatalf("fail strategy should requeue the message; size = %d, want 1", tr.Size("q"))
	}
}
