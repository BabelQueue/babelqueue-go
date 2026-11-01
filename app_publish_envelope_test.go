package babelqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// The producer seam (phase 03 scope 6): PublishEnvelopeWithHeaders returns the envelope that
// was handed to the transport, also when the transport fails, and DefaultQueue reads the
// WithDefaultQueue value.

// pubTransport records what reaches it and can fail every publish.
type pubTransport struct {
	err        error
	publishes  int
	lastQueue  string
	lastBody   string
	headerPubs int
	lastHdrs   map[string]string
}

func (p *pubTransport) Publish(_ context.Context, queue, body string) error {
	p.publishes++
	p.lastQueue, p.lastBody = queue, body
	return p.err
}

func (p *pubTransport) PublishWithHeaders(_ context.Context, queue, body string, headers map[string]string) error {
	p.headerPubs++
	p.lastQueue, p.lastBody, p.lastHdrs = queue, body, headers
	return p.err
}

func (p *pubTransport) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	return nil, nil
}

func (p *pubTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error { return nil }

func TestPublishEnvelopeWithHeadersSuccess(t *testing.T) {
	tr := &pubTransport{}
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("billing"))

	env, err := app.PublishEnvelopeWithHeaders(context.Background(), "urn:bq:orders:created",
		map[string]any{"n": 1}, map[string]string{"traceparent": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if env.Meta.ID == "" || env.TraceID == "" || env.Meta.Queue != "billing" || env.URN() != "urn:bq:orders:created" {
		t.Fatalf("envelope = %+v", env)
	}
	if tr.headerPubs != 1 || tr.lastQueue != "billing" || tr.lastHdrs["traceparent"] != "x" {
		t.Errorf("transport saw queue=%q headers=%v headerPubs=%d", tr.lastQueue, tr.lastHdrs, tr.headerPubs)
	}
}

func TestPublishEnvelopeWithHeadersTransportErrorReturnsEnvelope(t *testing.T) {
	boom := errors.New("broker down")
	for name, headers := range map[string]map[string]string{
		"plain publish":  nil,
		"header publish": {"traceparent": "x"},
	} {
		t.Run(name, func(t *testing.T) {
			tr := &pubTransport{err: boom}
			app := babelqueue.NewApp(tr)

			env, err := app.PublishEnvelopeWithHeaders(context.Background(), "urn:bq:orders:created",
				map[string]any{"n": 1}, headers, babelqueue.WithTraceID("trace-abc"))
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want the transport error", err)
			}
			if env.Meta.ID == "" || env.TraceID != "trace-abc" || env.Meta.Queue != "default" {
				t.Fatalf("envelope on transport failure = %+v, want the envelope handed to the transport", env)
			}
			// It is the very envelope the transport was given.
			got, derr := babelqueue.Decode([]byte(tr.lastBody))
			if derr != nil {
				t.Fatal(derr)
			}
			if got.Meta.ID != env.Meta.ID || got.TraceID != env.TraceID || got.Meta.Queue != env.Meta.Queue {
				t.Errorf("returned envelope %+v differs from the published body %+v", env, got)
			}
		})
	}
}

func TestPublishEnvelopeWithHeadersPerCallQueueWins(t *testing.T) {
	tr := &pubTransport{err: errors.New("broker down")}
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("billing"))

	env, err := app.PublishEnvelopeWithHeaders(context.Background(), "urn:bq:orders:created", nil, nil,
		babelqueue.WithQueue("orders"))
	if err == nil {
		t.Fatal("expected the transport error")
	}
	if env.Meta.Queue != "orders" || tr.lastQueue != "orders" {
		t.Fatalf("meta.queue = %q, transport queue = %q, want orders (the per-call value, not the default)",
			env.Meta.Queue, tr.lastQueue)
	}
	if app.DefaultQueue() != "billing" {
		t.Errorf("DefaultQueue = %q, want billing", app.DefaultQueue())
	}
}

func TestPublishEnvelopeWithHeadersBuildFailuresReturnZeroEnvelope(t *testing.T) {
	t.Run("Make fails", func(t *testing.T) {
		tr := &pubTransport{}
		app := babelqueue.NewApp(tr)
		env, err := app.PublishEnvelopeWithHeaders(context.Background(), "  ", nil, nil)
		if !errors.Is(err, babelqueue.ErrEmptyURN) {
			t.Fatalf("err = %v, want ErrEmptyURN", err)
		}
		if env.Meta.ID != "" || env.TraceID != "" || env.Job != "" || env.Meta.Queue != "" {
			t.Errorf("envelope = %+v, want the zero Envelope", env)
		}
		if tr.publishes+tr.headerPubs != 0 {
			t.Error("nothing may reach the transport when Make fails")
		}
	})

	t.Run("Encode fails after a successful Make", func(t *testing.T) {
		tr := &pubTransport{}
		app := babelqueue.NewApp(tr)
		// A channel cannot be serialised: Make accepts the map, Encode rejects it.
		data := map[string]any{"bad": make(chan int)}
		if _, err := babelqueue.Make("urn:bq:orders:created", data); err != nil {
			t.Fatalf("precondition: Make must succeed, got %v", err)
		}
		env, err := app.PublishEnvelopeWithHeaders(context.Background(), "urn:bq:orders:created", data, nil)
		if err == nil {
			t.Fatal("expected an encode error")
		}
		if env.Meta.ID != "" || env.TraceID != "" || env.Job != "" || env.Meta.Queue != "" {
			t.Errorf("envelope = %+v, want the zero Envelope: it was never handed to the transport", env)
		}
		if tr.publishes+tr.headerPubs != 0 {
			t.Error("nothing may reach the transport when Encode fails")
		}
	})
}

func TestDefaultQueue(t *testing.T) {
	if got := babelqueue.NewApp(&pubTransport{}).DefaultQueue(); got != "default" {
		t.Errorf("DefaultQueue() = %q, want default", got)
	}
	if got := babelqueue.NewApp(&pubTransport{}, babelqueue.WithDefaultQueue("billing")).DefaultQueue(); got != "billing" {
		t.Errorf("DefaultQueue() = %q, want billing", got)
	}
}

// The existing methods keep their return values and error semantics on top of the seam.
func TestPublishAndPublishWithHeadersKeepSemantics(t *testing.T) {
	boom := errors.New("broker down")
	failing := babelqueue.NewApp(&pubTransport{err: boom})
	ctx := context.Background()

	if id, err := failing.Publish(ctx, "urn:bq:orders:created", nil); id != "" || !errors.Is(err, boom) {
		t.Errorf("Publish on failure = (%q, %v), want (\"\", transport error)", id, err)
	}
	if id, err := failing.PublishWithHeaders(ctx, "urn:bq:orders:created", nil, map[string]string{"a": "b"}); id != "" || !errors.Is(err, boom) {
		t.Errorf("PublishWithHeaders on failure = (%q, %v), want (\"\", transport error)", id, err)
	}
	if id, err := failing.Publish(ctx, "", nil); id != "" || !errors.Is(err, babelqueue.ErrEmptyURN) {
		t.Errorf("Publish with an empty URN = (%q, %v)", id, err)
	}

	tr := &pubTransport{}
	ok := babelqueue.NewApp(tr)
	id, err := ok.Publish(ctx, "urn:bq:orders:created", nil)
	if err != nil || id == "" {
		t.Fatalf("Publish = (%q, %v)", id, err)
	}
	if tr.publishes != 1 || tr.headerPubs != 0 {
		t.Errorf("Publish must use the plain transport call: publishes=%d headerPubs=%d", tr.publishes, tr.headerPubs)
	}
	id2, err := ok.PublishWithHeaders(ctx, "urn:bq:orders:created", nil, map[string]string{"a": "b"})
	if err != nil || id2 == "" || tr.headerPubs != 1 {
		t.Fatalf("PublishWithHeaders = (%q, %v) headerPubs=%d", id2, err, tr.headerPubs)
	}
}
