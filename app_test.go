package babelqueue_test

import (
	"context"
	"errors"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

func TestAppPublishAndConsume(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))

	var got babelqueue.Envelope
	calls := 0
	app.Handle("urn:babel:orders:created", func(_ context.Context, env babelqueue.Envelope) error {
		calls++
		got = env
		return nil
	})

	id, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{"order_id": 1042})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id == "" {
		t.Fatal("Publish must return the message id")
	}

	n, err := app.Drain(context.Background(), "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || calls != 1 {
		t.Fatalf("processed %d (calls=%d), want 1", n, calls)
	}
	if got.URN() != "urn:babel:orders:created" || got.Meta.Queue != "orders" {
		t.Errorf("handler got %+v", got)
	}
	if oid, ok := got.Data["order_id"].(float64); !ok || oid != 1042 {
		t.Errorf("data.order_id = %v", got.Data["order_id"])
	}
	if tr.Size("orders") != 0 {
		t.Error("queue must be empty after a successful ack")
	}
}

func TestAppRetriesThenDeadLetters(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithMaxAttempts(3),
		babelqueue.WithDeadLetter(true),
	)

	calls := 0
	app.Handle("urn:babel:orders:created", func(_ context.Context, _ babelqueue.Envelope) error {
		calls++
		return errors.New("boom")
	})

	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{"order_id": 1}); err != nil {
		t.Fatal(err)
	}

	if _, err := app.Drain(context.Background(), "orders", 0); err != nil {
		t.Fatal(err)
	}

	if calls != 3 {
		t.Fatalf("handler called %d times, want 3 (maxAttempts)", calls)
	}
	if tr.Size("orders") != 0 {
		t.Error("source queue must be empty")
	}
	if tr.Size("orders.dlq") != 1 {
		t.Fatalf("expected 1 message on orders.dlq, got %d", tr.Size("orders.dlq"))
	}

	dl, err := tr.Pop(context.Background(), "orders.dlq", 0)
	if err != nil || dl == nil {
		t.Fatal("could not pop the dead-lettered message")
	}
	env, _ := babelqueue.Decode([]byte(dl.Body))
	if env.DeadLetter == nil || env.DeadLetter.Reason != "failed" {
		t.Errorf("dead_letter = %+v", env.DeadLetter)
	}
	if env.DeadLetter.OriginalQueue != "orders" {
		t.Errorf("original_queue = %q, want orders", env.DeadLetter.OriginalQueue)
	}
	if env.DeadLetter.Error == nil || *env.DeadLetter.Error != "boom" {
		t.Errorf("dead_letter.error = %v", env.DeadLetter.Error)
	}
}

func TestAppRetriesThenDropsWithoutDLQ(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("q"), babelqueue.WithMaxAttempts(2))

	calls := 0
	app.Handle("urn:babel:x", func(_ context.Context, _ babelqueue.Envelope) error {
		calls++
		return errors.New("nope")
	})
	if _, err := app.Publish(context.Background(), "urn:babel:x", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "q", 0); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("handler called %d, want 2", calls)
	}
	if tr.Size("q") != 0 || tr.Size("q.dlq") != 0 {
		t.Error("message should be dropped (no DLQ configured)")
	}
}

func TestAppUnknownURNDeadLetter(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDeadLetter),
		babelqueue.WithDeadLetter(true),
	)
	// no handler registered for this URN
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 0); err != nil {
		t.Fatal(err)
	}
	if tr.Size("orders.dlq") != 1 {
		t.Fatalf("expected unknown-URN message on DLQ, got %d", tr.Size("orders.dlq"))
	}
	dl, _ := tr.Pop(context.Background(), "orders.dlq", 0)
	env, _ := babelqueue.Decode([]byte(dl.Body))
	if env.DeadLetter == nil || env.DeadLetter.Reason != "unknown_urn" {
		t.Errorf("dead_letter = %+v", env.DeadLetter)
	}
}

func TestAppUnknownURNDelete(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr,
		babelqueue.WithDefaultQueue("orders"),
		babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete),
	)
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	n, err := app.Drain(context.Background(), "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || tr.Size("orders") != 0 {
		t.Error("unknown-URN message should be deleted")
	}
}
