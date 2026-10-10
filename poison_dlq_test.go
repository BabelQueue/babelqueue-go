package babelqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

const poisonBody = "not-json \x00 {\"keep\":  \"me\"}" // deliberately not canonical JSON

// requirePoisonRecord asserts body is a wrapped poison DLQ record: the original text carried as
// "raw" plus a dead_letter block with the poison reason, the source queue and a decode error.
func requirePoisonRecord(t *testing.T, body, wantRaw string) map[string]any {
	t.Helper()
	var rec map[string]any
	if err := json.Unmarshal([]byte(body), &rec); err != nil {
		t.Fatalf("DLQ record is not a JSON object: %v: %q", err, body)
	}
	if wantRaw != "" && rec["raw"] != wantRaw {
		t.Fatalf("raw = %v, want the original body %q", rec["raw"], wantRaw)
	}
	dl, _ := rec["dead_letter"].(map[string]any)
	if dl == nil || dl["reason"] != "poison" || dl["original_queue"] != "orders" ||
		dl["lang"] != babelqueue.SourceLang || dl["error"] == nil || dl["failed_at"] == nil {
		t.Fatalf("dead_letter = %v, want reason poison, original_queue orders, lang, error and failed_at", dl)
	}
	return rec
}

// An undecodable body is written to the DLQ wrapped as {"raw": <body>} with a dead_letter block
// (reason poison), and acknowledged only afterwards, with no retry (nothing is re-published to
// the source queue), whatever the unknown-URN strategy.
func TestAppPoisonBodyIsDeadLetteredWrappedWithoutRetry(t *testing.T) {
	cases := []struct {
		name string
		opts []babelqueue.AppOption
	}{
		{"release", []babelqueue.AppOption{babelqueue.WithDeadLetter(true), babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease)}},
		{"fail", []babelqueue.AppOption{babelqueue.WithDeadLetter(true), babelqueue.WithUnknownURNStrategy(babelqueue.StrategyFail)}},
		{"dead_letter strategy", []babelqueue.AppOption{babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDeadLetter)}},
		{"dead_letter strategy and option", []babelqueue.AppOption{babelqueue.WithDeadLetter(true), babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDeadLetter)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &queueRecordingTransport{
				InMemoryTransport: babelqueue.NewInMemoryTransport(),
				next:              &babelqueue.ReceivedMessage{Body: poisonBody, Queue: "orders"},
			}
			opts := append([]babelqueue.AppOption{babelqueue.WithMaxAttempts(5)}, tc.opts...)
			app := babelqueue.NewApp(tr, opts...)
			if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
				t.Fatal(err)
			}
			if len(tr.queues) != 1 || tr.queues[0] != "orders.dlq" {
				t.Fatalf("published queues = %v bodies = %q; want exactly one record on orders.dlq", tr.queues, tr.bodies)
			}
			requirePoisonRecord(t, tr.bodies[0], poisonBody)
			if tr.acks != 1 {
				t.Fatalf("acks = %d, want 1", tr.acks)
			}
		})
	}
}

// StrategyDelete still drops an undecodable body, and with dead-lettering off the pre-existing
// bounded-retry behaviour is untouched (retried via re-publish up to WithMaxAttempts, then dropped).
func TestAppPoisonBodyWithoutDeadLetterKeepsExistingBehaviour(t *testing.T) {
	t.Run("delete strategy drops with an ack", func(t *testing.T) {
		tr := &queueRecordingTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: poisonBody, Queue: "orders"},
		}
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true), babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete))
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if len(tr.queues) != 0 || tr.acks != 1 {
			t.Fatalf("published = %v acks = %d; want a plain drop", tr.queues, tr.acks)
		}
	})

	t.Run("dead-lettering off: retried through the source queue, no DLQ record", func(t *testing.T) {
		tr := &queueRecordingTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: poisonBody, Queue: "orders"},
		}
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(3))
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if len(tr.queues) != 1 || tr.queues[0] != "orders" || tr.acks != 1 {
			t.Fatalf("published = %v acks = %d; want the existing retry re-publish on orders + ack", tr.queues, tr.acks)
		}
	})
}

// A poison body whose dead-letter write fails is never lost: not acknowledged, reported to the
// hook, handed back (Rewinder), never retried through the source queue. Once the DLQ recovers it
// is written raw and then acknowledged.
func TestAppPoisonDeadLetterFailureIsNotAcked(t *testing.T) {
	newFlaky := func(failures int) *flakyDLQTransport {
		return &flakyDLQTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			msg:               &babelqueue.ReceivedMessage{Body: poisonBody, Queue: "orders"},
			failPublishes:     failures,
		}
	}

	t.Run("failed write: no ack, hook fired, rewound, only the DLQ is written", func(t *testing.T) {
		base := newFlaky(1000)
		tr := rewindingFlakyDLQTransport{base}
		var hookErrs []error
		app := babelqueue.NewApp(tr, babelqueue.WithMaxAttempts(5), babelqueue.WithDeadLetter(true),
			babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease), babelqueue.WithRewindPause(0),
			babelqueue.WithDeadLetterErrorHandler(func(_ context.Context, _ *babelqueue.ReceivedMessage, err error) {
				hookErrs = append(hookErrs, err)
			}))
		if _, err := app.Drain(context.Background(), "orders", 3); err != nil {
			t.Fatal(err)
		}
		if base.acks != 0 {
			t.Fatalf("acks = %d; a poison message whose DLQ write failed must not be acknowledged", base.acks)
		}
		for _, q := range base.publishQueues {
			if q != "orders.dlq" {
				t.Fatalf("published to %q; only the DLQ may be written: %v", q, base.publishQueues)
			}
		}
		if len(hookErrs) != 3 || base.rewinds != 3 {
			t.Fatalf("hook reports = %d, rewinds = %d; want 3 and 3", len(hookErrs), base.rewinds)
		}
	})

	t.Run("a transport without Rewinder leaves the message unacked", func(t *testing.T) {
		base := newFlaky(1000)
		app := babelqueue.NewApp(base, babelqueue.WithDeadLetter(true), babelqueue.WithRewindPause(0))
		if _, err := app.Drain(context.Background(), "orders", 2); err != nil {
			t.Fatal(err)
		}
		if base.acks != 0 || len(base.publishQueues) != 2 {
			t.Fatalf("acks = %d publishes = %v; want 0 acks and 2 DLQ attempts", base.acks, base.publishQueues)
		}
	})

	t.Run("DLQ recovers: wrapped body written once, then acknowledged", func(t *testing.T) {
		base := newFlaky(2)
		var rewound int
		tr := &countingRewinder{flakyDLQTransport: base, rewound: &rewound}
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true), babelqueue.WithRewindPause(0))
		if _, err := app.Drain(context.Background(), "orders", 5); err != nil {
			t.Fatal(err)
		}
		if rewound != 2 || base.deadLettered != 1 || base.acks != 1 || !strings.Contains(base.deadLetterBody, `"dead_letter"`) {
			t.Fatalf("rewound = %d dead-lettered = %d acks = %d body = %q; want 2, 1, 1, a wrapped poison record",
				rewound, base.deadLettered, base.acks, base.deadLetterBody)
		}
		requirePoisonRecord(t, base.deadLetterBody, poisonBody)
	})
}

// deadLetterMessage no longer swallows a failed write: a handler failure that exhausted its
// retries (and an unknown URN under StrategyDeadLetter) is not acknowledged when the DLQ write
// fails, and is acknowledged once the write succeeds.
func TestAppDeadLetterMessageFailureIsNotAcked(t *testing.T) {
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
	paths := map[string]struct {
		opts   []babelqueue.AppOption
		handle bool
	}{
		"exhausted handler failure": {opts: []babelqueue.AppOption{babelqueue.WithMaxAttempts(1)}, handle: true},
		"unknown URN, dead_letter strategy": {opts: []babelqueue.AppOption{
			babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDeadLetter)}},
	}
	for name, p := range paths {
		t.Run(name+": failed write is not acked and is rewound", func(t *testing.T) {
			base := newFlaky(1000)
			var rewound int
			tr := &countingRewinder{flakyDLQTransport: base, rewound: &rewound}
			var hookErrs []error
			opts := append([]babelqueue.AppOption{babelqueue.WithDeadLetter(true), babelqueue.WithRewindPause(0),
				babelqueue.WithDeadLetterErrorHandler(func(_ context.Context, _ *babelqueue.ReceivedMessage, err error) {
					hookErrs = append(hookErrs, err)
				})}, p.opts...)
			app := babelqueue.NewApp(tr, opts...)
			if p.handle {
				app.Handle(urn, func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
			}
			if _, err := app.Drain(context.Background(), "orders", 2); err != nil {
				t.Fatal(err)
			}
			if base.acks != 0 || rewound != 2 || len(hookErrs) != 2 {
				t.Fatalf("acks = %d rewinds = %d hook reports = %d; want 0, 2, 2", base.acks, rewound, len(hookErrs))
			}
		})
		t.Run(name+": recovered DLQ writes once then acks", func(t *testing.T) {
			base := newFlaky(1)
			app := babelqueue.NewApp(base, append([]babelqueue.AppOption{babelqueue.WithDeadLetter(true), babelqueue.WithRewindPause(0)}, p.opts...)...)
			if p.handle {
				app.Handle(urn, func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
			}
			if _, err := app.Drain(context.Background(), "orders", 5); err != nil {
				t.Fatal(err)
			}
			if base.deadLettered != 1 || base.acks != 1 {
				t.Fatalf("dead-lettered = %d acks = %d; want 1 and 1", base.deadLettered, base.acks)
			}
		})
	}
}

// A body that is a JSON object the codec rejects (a list "data") keeps its keys and number
// formatting verbatim; only the dead_letter block is added.
func TestAppPoisonJSONObjectIsKeptVerbatim(t *testing.T) {
	const body = `{"job":"urn:x","trace_id":"t","data":[1,2],"amount":1.50}`
	tr := &queueRecordingTransport{
		InMemoryTransport: babelqueue.NewInMemoryTransport(),
		next:              &babelqueue.ReceivedMessage{Body: body, Queue: "orders"},
	}
	app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true))
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(tr.bodies) != 1 || tr.acks != 1 {
		t.Fatalf("bodies = %q acks = %d; want one record and one ack", tr.bodies, tr.acks)
	}
	rec := requirePoisonRecord(t, tr.bodies[0], "")
	if _, wrapped := rec["raw"]; wrapped {
		t.Errorf("a JSON object must not be wrapped in raw: %q", tr.bodies[0])
	}
	if rec["job"] != "urn:x" || rec["trace_id"] != "t" || !strings.Contains(tr.bodies[0], `"data":[1,2]`) ||
		!strings.Contains(tr.bodies[0], `"amount":1.50`) {
		t.Errorf("original keys not preserved verbatim: %q", tr.bodies[0])
	}
}

// Anything that is not exactly one JSON object (a list, a scalar, trailing data) is wrapped.
func TestAppPoisonNonObjectBodiesAreWrapped(t *testing.T) {
	for _, body := range []string{`[1,2]`, `"text"`, `42`, `{"a":1} {"b":2}`, `{"a":`} {
		tr := &queueRecordingTransport{
			InMemoryTransport: babelqueue.NewInMemoryTransport(),
			next:              &babelqueue.ReceivedMessage{Body: body, Queue: "orders"},
		}
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true))
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
		if len(tr.bodies) != 1 || tr.acks != 1 {
			t.Fatalf("%q: bodies = %q acks = %d; want one record and one ack", body, tr.bodies, tr.acks)
		}
		requirePoisonRecord(t, tr.bodies[0], body)
	}
}
