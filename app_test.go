package babelqueue_test

import (
	"context"
	"errors"
	"strings"
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

// A transport-supplied RouteURN names the handler for a body that carries no identity of its
// own (ADR-0032 §6 leaves the identity to the transport, e.g. the pulsar bq-job property).
func TestAppRouteURNHandlesAnIdentitylessBody(t *testing.T) {
	const routeURN = "urn:babel:orders:refunded"
	for _, body := range []string{`{"trace_id":"t","data":{"order_id":1}}`, `{}`, `{"job":5}`} {
		tr := &routeTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: body, Queue: "orders", RouteURN: routeURN},
		}
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3), babelqueue.WithDeadLetter(true))
		calls := 0
		app.Handle(routeURN, func(context.Context, babelqueue.Envelope) error { calls++; return nil })
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Errorf("%s: handler ran %d times, want 1", body, calls)
		}
	}
}

// Without a RouteURN the same bodies have no identity and are dead-lettered raw, never handled.
func TestAppIdentitylessBodiesArePoisonOnTheConsumePath(t *testing.T) {
	for _, body := range []string{`{}`, `{"job":null}`, `{"job":5}`, `{"urn":5}`, `null`, `{"job":" ","urn":"\t"}`} {
		tr := babelqueue.NewInMemoryTransport()
		app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"), babelqueue.WithDeadLetter(true))
		calls := 0
		app.Handle("", func(context.Context, babelqueue.Envelope) error { calls++; return nil })
		if err := tr.Publish(context.Background(), "orders", body); err != nil {
			t.Fatal(err)
		}
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if calls != 0 {
			t.Errorf("%s: a handler ran for a body without identity", body)
		}
		if tr.Size("orders") != 0 || tr.Size("orders.dlq") != 1 {
			t.Errorf("%s: orders=%d dlq=%d, want 0/1", body, tr.Size("orders"), tr.Size("orders.dlq"))
		}
	}
}

// queueRecordingTransport hands the App one message and records every (queue, body) it
// re-publishes, so a test can tell a retry (original queue) from a dead-letter (the DLQ).
type queueRecordingTransport struct {
	*babelqueue.InMemoryTransport
	next   *babelqueue.ReceivedMessage
	queues []string
	bodies []string
	acks   int
}

func (q *queueRecordingTransport) Pop(_ context.Context, _ string, _ time.Duration) (*babelqueue.ReceivedMessage, error) {
	m := q.next
	q.next = nil
	return m, nil
}

func (q *queueRecordingTransport) Publish(_ context.Context, queue, body string) error {
	q.queues = append(q.queues, queue)
	q.bodies = append(q.bodies, body)
	return nil
}

func (q *queueRecordingTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error {
	q.acks++
	return nil
}

// A transport-version-gated delivery (reserved unsupported-schema-version RouteURN, e.g. the
// bq-schema-version gate of the SQS/Kafka/Artemis/ASB/Pulsar transports) can never be handled
// and a retry cannot change that: it goes straight to the dead-letter queue, with no retry,
// whatever the unknown-URN strategy and however many attempts are allowed (Python parity).
func TestAppVersionGatedRouteIsDeadLetteredWithoutRetry(t *testing.T) {
	const bodyURN = "urn:babel:orders:created"
	const gateRoute = "babelqueue:unsupported-schema-version:2?bq-job=urn%3Ababel%3Aorders%3Acreated"
	env, err := babelqueue.Make(bodyURN, map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()

	strategies := map[string]string{
		"release":     babelqueue.StrategyRelease,
		"fail":        babelqueue.StrategyFail,
		"dead-letter": babelqueue.StrategyDeadLetter,
	}
	for name, strategy := range strategies {
		t.Run("maxAttempts 5 and "+name+" strategy: handler never runs, one DLQ record, no retry", func(t *testing.T) {
			tr := &queueRecordingTransport{
				InMemoryTransport: babelqueue.NewInMemoryTransport(),
				next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders", RouteURN: gateRoute},
			}
			app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5), babelqueue.WithDeadLetter(true),
				babelqueue.WithUnknownURNStrategy(strategy))
			calls := 0
			app.Handle(bodyURN, func(context.Context, babelqueue.Envelope) error { calls++; return nil })
			if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
				t.Fatal(err)
			}
			if calls != 0 {
				t.Fatalf("handler ran %d times for a version-gated delivery", calls)
			}
			if len(tr.queues) != 1 || tr.queues[0] != "orders.dlq" {
				t.Fatalf("re-published to %v; want exactly one record on orders.dlq (no retry on orders)", tr.queues)
			}
			if tr.acks != 1 {
				t.Errorf("acks = %d, want 1", tr.acks)
			}
			// §3.7 etc.: the gate rejects without decoding, so the DLQ record is the incoming body
			// byte for byte, with no dead_letter block.
			if tr.bodies[0] != string(raw) {
				t.Errorf("dead-letter body is not byte-identical to the incoming body:\n got: %s\nwant: %s", tr.bodies[0], raw)
			}
			if strings.Contains(tr.bodies[0], "dead_letter") {
				t.Errorf("dead-letter record carries a dead_letter block: %s", tr.bodies[0])
			}
		})
	}

	t.Run("routed publisher keeps the gate route on the DLQ record", func(t *testing.T) {
		tr := &routedPublishTransport{routeTransport: routeTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders", RouteURN: gateRoute},
		}}
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5), babelqueue.WithDeadLetter(true),
			babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease))
		app.Handle(bodyURN, func(context.Context, babelqueue.Envelope) error { return nil })
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if len(tr.published) != 1 || tr.queues[0] != "orders.dlq" || tr.routes[0] != gateRoute {
			t.Fatalf("queues = %v routes = %v; want one orders.dlq record on %s", tr.queues, tr.routes, gateRoute)
		}
	})

	t.Run("an undecodable gated body is dead-lettered too", func(t *testing.T) {
		tr := &queueRecordingTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: "not-json", Queue: "orders", RouteURN: gateRoute},
		}
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5), babelqueue.WithDeadLetter(true),
			babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease))
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if len(tr.queues) != 1 || tr.queues[0] != "orders.dlq" {
			t.Fatalf("re-published to %v; want one orders.dlq record", tr.queues)
		}
		if tr.bodies[0] != "not-json" {
			t.Errorf("dead-letter body = %q, want the raw body %q unchanged", tr.bodies[0], "not-json")
		}
	})

	t.Run("dead-lettering off: dropped with an ack, nothing re-published", func(t *testing.T) {
		tr := &queueRecordingTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders", RouteURN: gateRoute},
		}
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5), babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease))
		calls := 0
		app.Handle(bodyURN, func(context.Context, babelqueue.Envelope) error { calls++; return nil })
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if calls != 0 || len(tr.queues) != 0 || tr.acks != 1 {
			t.Fatalf("calls = %d, re-published = %v, acks = %d; want 0, none, 1", calls, tr.queues, tr.acks)
		}
	})
}

// The genuine unknown-URN StrategyRelease behaviour is unchanged: a RouteURN no handler owns
// (but that is not a version-gate route) is re-published verbatim, attempts untouched.
func TestAppUnknownRouteURNStillReleasesVerbatim(t *testing.T) {
	const bodyURN, routeURN = "urn:babel:orders:created", "urn:babel:orders:unregistered"
	env, err := babelqueue.Make(bodyURN, map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()
	tr := &routedPublishTransport{routeTransport: routeTransport{
		InMemoryTransport: babelqueue.NewInMemoryTransport(),
		next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders", RouteURN: routeURN},
	}}
	app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3), babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease))
	app.Handle(bodyURN, func(context.Context, babelqueue.Envelope) error { return nil })
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(tr.published) != 1 || tr.published[0] != string(raw) || tr.routes[0] != routeURN {
		t.Fatalf("published = %v routes = %v; want the original body re-published verbatim on %s", tr.published, tr.routes, routeURN)
	}
}

// flakyDLQTransport redelivers its one message until it is acknowledged, and fails the first
// failPublishes publishes (dead-letter writes, or retry/release re-publishes), recording every
// publish, ack and rewind.
type flakyDLQTransport struct {
	*babelqueue.InMemoryTransport
	msg            *babelqueue.ReceivedMessage
	acked          bool
	failPublishes  int
	publishQueues  []string
	acks           int
	rewinds        int
	rewindErr      error
	polls          int
	deadLettered   int
	deadLetterBody string
}

func (f *flakyDLQTransport) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	f.polls++
	if f.acked {
		return nil, nil
	}
	m := *f.msg
	return &m, nil
}

func (f *flakyDLQTransport) Publish(_ context.Context, queue, body string) error {
	f.publishQueues = append(f.publishQueues, queue)
	if f.failPublishes > 0 {
		f.failPublishes--
		return errors.New("dlq unavailable")
	}
	f.deadLettered++
	f.deadLetterBody = body
	return nil
}

func (f *flakyDLQTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error {
	f.acks++
	f.acked = true
	return nil
}

// rewindingFlakyDLQTransport adds the optional Rewinder capability.
type rewindingFlakyDLQTransport struct{ *flakyDLQTransport }

func (r rewindingFlakyDLQTransport) Rewind(context.Context, *babelqueue.ReceivedMessage) error {
	r.rewinds++
	return r.rewindErr
}

// A version-gated delivery whose dead-letter write fails is never lost: it is not acknowledged,
// the failure is reported to the hook, the message is handed back for redelivery (Rewinder), the
// handler never runs and nothing is re-published to the source queue (no retry). When the DLQ
// recovers, the redelivered message is dead-lettered and only then acknowledged.
func TestAppVersionGatedDeadLetterFailureIsNotAcked(t *testing.T) {
	const bodyURN = "urn:babel:orders:created"
	const gateRoute = "babelqueue:unsupported-schema-version:2"
	env, err := babelqueue.Make(bodyURN, map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()

	newFlaky := func(failures int) *flakyDLQTransport {
		return &flakyDLQTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			msg:               &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders", RouteURN: gateRoute},
			failPublishes:     failures,
		}
	}

	t.Run("failed write: no ack, hook fired, rewound, handler never runs, no retry publish", func(t *testing.T) {
		base := newFlaky(1000)
		tr := rewindingFlakyDLQTransport{base}
		var hookErrs []error
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5), babelqueue.WithDeadLetter(true),
			babelqueue.WithRewindPause(0),
			babelqueue.WithDeadLetterErrorHandler(func(_ context.Context, _ *babelqueue.ReceivedMessage, err error) {
				hookErrs = append(hookErrs, err)
			}))
		calls := 0
		app.Handle(bodyURN, func(context.Context, babelqueue.Envelope) error { calls++; return nil })
		if _, err := app.Drain(context.Background(), "orders", 3); err != nil {
			t.Fatal(err)
		}
		if calls != 0 {
			t.Fatalf("handler ran %d times for a version-gated delivery", calls)
		}
		if base.acks != 0 {
			t.Fatalf("acks = %d; a message whose dead-letter write failed must not be acknowledged", base.acks)
		}
		for _, q := range base.publishQueues {
			if q != "orders.dlq" {
				t.Fatalf("published to %q; only the DLQ may be written (no retry re-publish): %v", q, base.publishQueues)
			}
		}
		if len(hookErrs) != 3 {
			t.Fatalf("hook reports = %d, want 3 (one per failed write)", len(hookErrs))
		}
		if base.rewinds != 3 {
			t.Fatalf("rewinds = %d, want 3", base.rewinds)
		}
	})

	t.Run("DLQ recovers: redelivered message is dead-lettered once, then acknowledged", func(t *testing.T) {
		base := newFlaky(2)
		var rewound int
		tr := &countingRewinder{flakyDLQTransport: base, rewound: &rewound}
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5), babelqueue.WithDeadLetter(true),
			babelqueue.WithRewindPause(0))
		calls := 0
		app.Handle(bodyURN, func(context.Context, babelqueue.Envelope) error { calls++; return nil })
		if _, err := app.Drain(context.Background(), "orders", 5); err != nil {
			t.Fatal(err)
		}
		if calls != 0 {
			t.Fatalf("handler ran %d times", calls)
		}
		if rewound != 2 {
			t.Errorf("rewinds = %d, want 2 (one per failed write)", rewound)
		}
		if base.deadLettered != 1 || base.acks != 1 {
			t.Fatalf("dead-lettered = %d, acks = %d; want exactly one DLQ record and one ack", base.deadLettered, base.acks)
		}
		if len(base.publishQueues) != 3 {
			t.Errorf("publishes = %v, want 3 DLQ attempts", base.publishQueues)
		}
	})

	t.Run("a transport without Rewinder leaves the message unacked and does not panic", func(t *testing.T) {
		base := newFlaky(1000)
		app := babelqueue.NewApp(base, babelqueue.WithMaxAttempts(5), babelqueue.WithDeadLetter(true),
			babelqueue.WithRewindPause(0))
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if base.acks != 0 {
			t.Fatalf("acks = %d, want 0", base.acks)
		}
	})

	t.Run("a failing rewind is reported to the hook too", func(t *testing.T) {
		base := newFlaky(1000)
		var rewound int
		tr := &countingRewinder{flakyDLQTransport: base, rewound: &rewound, err: errors.New("rewind failed")}
		var hookErrs []error
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true), babelqueue.WithRewindPause(0),
			babelqueue.WithDeadLetterErrorHandler(func(_ context.Context, _ *babelqueue.ReceivedMessage, err error) {
				hookErrs = append(hookErrs, err)
			}))
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if len(hookErrs) != 2 || base.acks != 0 {
			t.Fatalf("hook errors = %d (want write + rewind = 2), acks = %d (want 0)", len(hookErrs), base.acks)
		}
	})

	t.Run("DLQ record is the raw incoming body; the rejected value never reaches it", func(t *testing.T) {
		base := newFlaky(0)
		bounded := babelqueue.TruncateDeclaredVersion(strings.Repeat("9", 5000))
		base.msg.RouteURN = "babelqueue:unsupported-schema-version:" + bounded
		app := babelqueue.NewApp(base, babelqueue.WithDeadLetter(true))
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if base.deadLettered != 1 || base.acks != 1 {
			t.Fatalf("dead-lettered = %d, acks = %d", base.deadLettered, base.acks)
		}
		if base.deadLetterBody != string(raw) {
			t.Fatalf("DLQ record is not byte-identical to the incoming body (len %d vs %d)", len(base.deadLetterBody), len(raw))
		}
		if strings.Contains(base.deadLetterBody, "99999") || strings.Contains(base.deadLetterBody, "dead_letter") {
			t.Fatalf("DLQ record must stay the raw body: %s", base.deadLetterBody)
		}
	})

	t.Run("StrategyDelete drops a gated delivery with an ack even with dead-lettering on (Python parity)", func(t *testing.T) {
		base := newFlaky(0)
		app := babelqueue.NewApp(base, babelqueue.WithDeadLetter(true),
			babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete))
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if base.acks != 1 || len(base.publishQueues) != 0 {
			t.Fatalf("acks = %d, publishes = %v; want a plain drop", base.acks, base.publishQueues)
		}
	})
}

type countingRewinder struct {
	*flakyDLQTransport
	rewound *int
	err     error
}

func (c *countingRewinder) Rewind(context.Context, *babelqueue.ReceivedMessage) error {
	*c.rewound++
	return c.err
}

func TestTruncateDeclaredVersion(t *testing.T) {
	long := strings.Repeat("9", 5000)
	if got, want := babelqueue.TruncateDeclaredVersion(long), strings.Repeat("9", 64)+"..."; got != want {
		t.Fatalf("5000-char value: len %d, want %d", len(got), len(want))
	}
	exact := strings.Repeat("v", 64)
	if got := babelqueue.TruncateDeclaredVersion(exact); got != exact {
		t.Fatalf("exactly 64 characters must be unchanged, got %q", got)
	}
	if got := babelqueue.TruncateDeclaredVersion(exact + "x"); got != exact+"..." {
		t.Fatalf("65 characters: got %q", got)
	}
	multibyte := strings.Repeat("ğ", 70)
	if got := babelqueue.TruncateDeclaredVersion(multibyte); got != strings.Repeat("ğ", 64)+"..." {
		t.Fatalf("multibyte values are cut on a rune boundary, got %q", got)
	}
	if got := babelqueue.TruncateDeclaredVersion(""); got != "" {
		t.Fatalf("empty: %q", got)
	}
}

// The dead-letter error hook runs after the message is handed back, and a panic in it is
// recovered: Rewind is still called, nothing is acknowledged and the consume loop carries on.
func TestAppDeadLetterErrorHookPanicDoesNotSkipRewind(t *testing.T) {
	env, err := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()
	base := &flakyDLQTransport{
		InMemoryTransport: babelqueue.NewInMemoryTransport(),
		msg: &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders",
			RouteURN: "babelqueue:unsupported-schema-version:2"},
		failPublishes: 1000,
	}
	var order []string
	tr := &orderedRewinder{flakyDLQTransport: base, order: &order}
	app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true), babelqueue.WithRewindPause(0),
		babelqueue.WithDeadLetterErrorHandler(func(context.Context, *babelqueue.ReceivedMessage, error) {
			order = append(order, "hook")
			panic("boom")
		}))
	n, err := app.Drain(context.Background(), "orders", 3)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("processed %d deliveries, want 3 (the loop must survive a panicking hook)", n)
	}
	if base.acks != 0 {
		t.Fatalf("acks = %d, want 0", base.acks)
	}
	want := []string{"rewind", "hook", "rewind", "hook", "rewind", "hook"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v (rewind first, then the hook)", order, want)
	}
}

type orderedRewinder struct {
	*flakyDLQTransport
	order *[]string
}

func (o *orderedRewinder) Rewind(context.Context, *babelqueue.ReceivedMessage) error {
	*o.order = append(*o.order, "rewind")
	return nil
}
