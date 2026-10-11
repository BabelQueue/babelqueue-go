package babelqueue_test

import (
	"context"
	"errors"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// A retry or a release that falls back to re-publish + ack must never ack when the re-publish
// failed: the message is not acknowledged, the failure is reported to the release error hook and
// the message is handed back to the broker (Rewinder), so it is redelivered instead of lost.
func TestAppRepublishFailureIsNotAcked(t *testing.T) {
	const urn = "urn:babel:orders:created"
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()

	newFlaky := func(failures int) *flakyDLQTransport {
		return &flakyDLQTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			msg:               &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders"},
			failPublishes:     failures,
		}
	}

	paths := []struct {
		name  string
		opts  []babelqueue.AppOption
		setup func(*babelqueue.App)
	}{
		{
			name: "retry path (handler error)",
			opts: []babelqueue.AppOption{babelqueue.WithMaxAttempts(5)},
			setup: func(app *babelqueue.App) {
				app.Handle(urn, func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
			},
		},
		{
			name: "unknown URN, StrategyFail (retry path)",
			opts: []babelqueue.AppOption{babelqueue.WithMaxAttempts(5), babelqueue.WithUnknownURNStrategy(babelqueue.StrategyFail)},
		},
		{
			name: "unknown URN, StrategyRelease",
			opts: []babelqueue.AppOption{babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease)},
		},
	}

	for _, p := range paths {
		t.Run(p.name+": failed re-publish: no ack, hook fired, rewound", func(t *testing.T) {
			base := newFlaky(1000)
			tr := rewindingFlakyDLQTransport{base}
			var hookErrs []error
			opts := append([]babelqueue.AppOption{
				babelqueue.WithRewindPause(0),
				babelqueue.WithReleaseErrorHandler(func(_ context.Context, _ *babelqueue.ReceivedMessage, err error) {
					hookErrs = append(hookErrs, err)
				}),
			}, p.opts...)
			app := babelqueue.NewApp(tr, opts...)
			if p.setup != nil {
				p.setup(app)
			}
			if _, err := app.Drain(context.Background(), "orders", 3); err != nil {
				t.Fatal(err)
			}
			if base.acks != 0 {
				t.Fatalf("acks = %d; a message whose re-publish failed must not be acknowledged", base.acks)
			}
			if base.rewinds != 3 {
				t.Fatalf("rewinds = %d, want 3 (one per failed re-publish)", base.rewinds)
			}
			if len(hookErrs) != 3 {
				t.Fatalf("hook reports = %d, want 3", len(hookErrs))
			}
		})

		t.Run(p.name+": transport without Rewinder leaves it unacked", func(t *testing.T) {
			base := newFlaky(1000)
			opts := append([]babelqueue.AppOption{babelqueue.WithRewindPause(0)}, p.opts...)
			app := babelqueue.NewApp(base, opts...)
			if p.setup != nil {
				p.setup(app)
			}
			if _, err := app.Drain(context.Background(), "orders", 2); err != nil {
				t.Fatal(err)
			}
			if base.acks != 0 {
				t.Fatalf("acks = %d, want 0", base.acks)
			}
		})

		t.Run(p.name+": a failing rewind is reported too", func(t *testing.T) {
			base := newFlaky(1000)
			var rewound int
			tr := &countingRewinder{flakyDLQTransport: base, rewound: &rewound, err: errors.New("rewind failed")}
			var hookErrs []error
			opts := append([]babelqueue.AppOption{
				babelqueue.WithRewindPause(0),
				babelqueue.WithReleaseErrorHandler(func(_ context.Context, _ *babelqueue.ReceivedMessage, err error) {
					hookErrs = append(hookErrs, err)
				}),
			}, p.opts...)
			app := babelqueue.NewApp(tr, opts...)
			if p.setup != nil {
				p.setup(app)
			}
			if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
				t.Fatal(err)
			}
			if rewound != 1 {
				t.Fatalf("rewinds = %d, want 1", rewound)
			}
			if len(hookErrs) != 2 || base.acks != 0 {
				t.Fatalf("hook errors = %d (want re-publish + rewind = 2), acks = %d (want 0)", len(hookErrs), base.acks)
			}
		})

		t.Run(p.name+": successful re-publish is acked once", func(t *testing.T) {
			base := newFlaky(0)
			opts := append([]babelqueue.AppOption{babelqueue.WithRewindPause(0)}, p.opts...)
			app := babelqueue.NewApp(base, opts...)
			if p.setup != nil {
				p.setup(app)
			}
			if _, err := app.Drain(context.Background(), "orders", 5); err != nil {
				t.Fatal(err)
			}
			if base.acks != 1 || len(base.publishQueues) != 1 || base.publishQueues[0] != "orders" {
				t.Fatalf("acks = %d, publishes = %v; want one re-publish to orders and one ack", base.acks, base.publishQueues)
			}
		})
	}
}
