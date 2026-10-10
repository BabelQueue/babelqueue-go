package babelqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

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

// routeTransport hands the App one message that carries a RouteURN override, and records
// what is published back (the retry path).
type routeTransport struct {
	*babelqueue.InMemoryTransport
	next      *babelqueue.ReceivedMessage
	published []string
}

func (r *routeTransport) Pop(_ context.Context, _ string, _ time.Duration) (*babelqueue.ReceivedMessage, error) {
	m := r.next
	r.next = nil
	return m, nil
}

func (r *routeTransport) Publish(_ context.Context, _ string, body string) error {
	r.published = append(r.published, body)
	return nil
}

func TestAppRoutesOnReceivedMessageRouteURNWithoutRewritingTheBody(t *testing.T) {
	const bodyURN, routeURN = "urn:babel:orders:created", "urn:babel:orders:refunded"
	env, err := babelqueue.Make(bodyURN, map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()
	tr := &routeTransport{
		InMemoryTransport: babelqueue.NewInMemoryTransport(),
		next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders", RouteURN: routeURN},
	}
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3))

	var bodyHandlerCalls int
	var routed []string
	app.Handle(bodyURN, func(context.Context, babelqueue.Envelope) error { bodyHandlerCalls++; return nil })
	app.Handle(routeURN, func(_ context.Context, e babelqueue.Envelope) error {
		routed = append(routed, e.URN())
		return errors.New("boom")
	})
	if n, err := app.Drain(context.Background(), "orders", 1); err != nil || n != 1 {
		t.Fatalf("Drain = (%d, %v)", n, err)
	}
	if bodyHandlerCalls != 0 || len(routed) != 1 {
		t.Fatalf("body handler calls = %d, route handler calls = %d; want 0 and 1", bodyHandlerCalls, len(routed))
	}
	if routed[0] != bodyURN {
		t.Errorf("handler envelope job = %q, want the untouched body URN %q", routed[0], bodyURN)
	}
	if len(tr.published) != 1 {
		t.Fatalf("retry published %d bodies, want 1", len(tr.published))
	}
	retried, _ := babelqueue.Decode([]byte(tr.published[0]))
	if retried.Job != bodyURN || retried.Attempts != 1 {
		t.Errorf("retry body job=%q attempts=%d, want %q / 1", retried.Job, retried.Attempts, bodyURN)
	}
}

func TestAppEmptyRouteURNRoutesOnTheBodyURN(t *testing.T) {
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
	calls := 0
	app.Handle("urn:babel:orders:created", func(context.Context, babelqueue.Envelope) error { calls++; return nil })
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 0); err != nil || calls != 1 {
		t.Fatalf("calls = %d, err = %v", calls, err)
	}
}

// routedPublishTransport adds the optional RoutedPublisher capability to routeTransport.
type routedPublishTransport struct {
	routeTransport
	routes []string
	queues []string
}

func (r *routedPublishTransport) PublishRouted(_ context.Context, queue, body, routeURN string) error {
	r.queues = append(r.queues, queue)
	r.published = append(r.published, body)
	r.routes = append(r.routes, routeURN)
	return nil
}

func TestAppRetryAndReleaseRepublishCarryTheRouteURN(t *testing.T) {
	const bodyURN, routeURN = "urn:babel:orders:created", "urn:babel:orders:refunded"
	env, err := babelqueue.Make(bodyURN, map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()

	t.Run("retry", func(t *testing.T) {
		tr := &routedPublishTransport{routeTransport: routeTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders", RouteURN: routeURN},
		}}
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3))
		app.Handle(routeURN, func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if len(tr.routes) != 1 || tr.routes[0] != routeURN {
			t.Errorf("retry route URNs = %v, want [%s]", tr.routes, routeURN)
		}
	})

	t.Run("unknown_urn_release", func(t *testing.T) {
		tr := &routedPublishTransport{routeTransport: routeTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders", RouteURN: routeURN},
		}}
		app := babelqueue.NewApp(tr, babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease))
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if len(tr.routes) != 1 || tr.routes[0] != routeURN || tr.published[0] != string(raw) {
			t.Errorf("release routes = %v published = %v; want one untouched body routed on %s", tr.routes, tr.published, routeURN)
		}
	})

	t.Run("no_route_uses_plain_publish", func(t *testing.T) {
		tr := &routedPublishTransport{routeTransport: routeTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders"},
		}}
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3))
		app.Handle(bodyURN, func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if len(tr.routes) != 0 || len(tr.published) != 1 {
			t.Errorf("routes = %v published = %d; want plain Publish only", tr.routes, len(tr.published))
		}
	})
}

// The dead-letter record is written through the same seam: it goes to the DLQ queue carrying the
// original route URN; without a route it is a plain Publish.
func TestAppDeadLetterWriteCarriesTheRouteURN(t *testing.T) {
	const bodyURN, routeURN = "urn:babel:orders:created", "urn:babel:orders:refunded"
	env, err := babelqueue.Make(bodyURN, map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()

	tr := &routedPublishTransport{routeTransport: routeTransport{
		InMemoryTransport: babelqueue.NewInMemoryTransport(),
		next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders", RouteURN: routeURN},
	}}
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true))
	app.Handle(routeURN, func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(tr.routes) != 1 || tr.routes[0] != routeURN || len(tr.published) != 1 {
		t.Fatalf("dead-letter routes = %v published = %d; want one record routed on %s", tr.routes, len(tr.published), routeURN)
	}
	if tr.queues[0] != "orders.dlq" {
		t.Errorf("dead-letter queue = %q, want orders.dlq", tr.queues[0])
	}

	plain := &routedPublishTransport{routeTransport: routeTransport{
		InMemoryTransport: babelqueue.NewInMemoryTransport(),
		next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders"},
	}}
	app2 := babelqueue.NewApp(plain, babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true))
	app2.Handle(bodyURN, func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
	if _, err := app2.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(plain.routes) != 0 || len(plain.published) != 1 {
		t.Errorf("no-route dead-letter: routes = %v published = %d; want plain Publish only", plain.routes, len(plain.published))
	}
}

// A RouteURN never rescues an undecodable body: it takes the poison path, not a handler.
func TestAppUndecodableBodyWithRouteURNIsPoison(t *testing.T) {
	const routeURN = "urn:babel:orders:refunded"
	tr := &routeTransport{
		InMemoryTransport: babelqueue.NewInMemoryTransport(),
		next:              &babelqueue.ReceivedMessage{Body: "not-json", Queue: "orders", RouteURN: routeURN},
	}
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3))
	calls := 0
	app.Handle(routeURN, func(context.Context, babelqueue.Envelope) error { calls++; return nil })
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Errorf("handler ran %d times for an undecodable body", calls)
	}
}
