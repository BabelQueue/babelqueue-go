package babelqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// ADR-0039 section 5.1: the dead-letter observer. These tests drive the App with fake
// transports and assert exactly when the observer is called and with what event.

// obsBase hands the App its scripted messages and records plain publishes and acks.
type obsBase struct {
	msgs []*babelqueue.ReceivedMessage

	published map[string][]string
	failTo    string // a Publish to this queue fails
	acks      int
}

func newObsBase(msgs ...*babelqueue.ReceivedMessage) *obsBase {
	return &obsBase{msgs: msgs, published: map[string][]string{}}
}

func (b *obsBase) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	if len(b.msgs) == 0 {
		return nil, nil
	}
	m := b.msgs[0]
	b.msgs = b.msgs[1:]
	return m, nil
}

func (b *obsBase) Publish(_ context.Context, queue, body string) error {
	if b.failTo != "" && queue == b.failTo {
		return errors.New("dlq publish failed")
	}
	b.published[queue] = append(b.published[queue], body)
	return nil
}

func (b *obsBase) Ack(context.Context, *babelqueue.ReceivedMessage) error {
	b.acks++
	return nil
}

// obsDLTransport adds the DeadLetterer capability.
type obsDLTransport struct {
	*obsBase
	err   error
	calls int
}

func (t *obsDLTransport) DeadLetter(context.Context, *babelqueue.ReceivedMessage, string, string) error {
	t.calls++
	return t.err
}

// obsNativeOnlyTransport has only NativeSettler (the SQS shape: a no-op that leaves the message to
// the broker's RedrivePolicy).
type obsNativeOnlyTransport struct {
	*obsBase
	calls int
}

func (t *obsNativeOnlyTransport) SettleNative(context.Context, *babelqueue.ReceivedMessage, string, string) error {
	t.calls++
	return nil
}

// obsNativeDLTransport has NativeSettler and the NativeDeadLetterSettler variant (the Azure Service
// Bus shape).
type obsNativeDLTransport struct {
	*obsBase
	deadLettered bool
	err          error
	variantCalls int
	plainCalls   int
}

func (t *obsNativeDLTransport) SettleNative(context.Context, *babelqueue.ReceivedMessage, string, string) error {
	t.plainCalls++
	return nil
}

func (t *obsNativeDLTransport) SettleNativeDeadLetter(context.Context, *babelqueue.ReceivedMessage, string, string) (bool, error) {
	t.variantCalls++
	return t.deadLettered, t.err
}

func obsBody(t *testing.T, urn string) string {
	t.Helper()
	env, err := babelqueue.Make(urn, map[string]any{"n": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func obsMsg(body string) *babelqueue.ReceivedMessage {
	return &babelqueue.ReceivedMessage{Body: body, Queue: "orders"}
}

func gatedMsg() *babelqueue.ReceivedMessage {
	return &babelqueue.ReceivedMessage{
		Body:     `{"not":"decoded"}`,
		Queue:    "orders",
		RouteURN: "babelqueue:unsupported-schema-version:2",
	}
}

type obsRecorder struct{ events []babelqueue.DeadLetterEvent }

func (r *obsRecorder) observe(_ context.Context, ev babelqueue.DeadLetterEvent) {
	r.events = append(r.events, ev)
}

func drain(t *testing.T, app *babelqueue.App) {
	t.Helper()
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
}

func failingHandler(context.Context, babelqueue.Envelope) error { return errors.New("boom") }

func TestDeadLetterObserverBabelQueueDLQWrites(t *testing.T) {
	cases := []struct {
		name       string
		msg        func(t *testing.T) *babelqueue.ReceivedMessage
		opts       []babelqueue.AppOption
		wantReason string
	}{
		{
			name: "retries exhausted",
			msg:  func(t *testing.T) *babelqueue.ReceivedMessage { return obsMsg(obsBody(t, "urn:bq:orders:created")) },
			opts: []babelqueue.AppOption{babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true)},

			wantReason: "failed",
		},
		{
			name: "unknown urn under dead_letter",
			msg:  func(t *testing.T) *babelqueue.ReceivedMessage { return obsMsg(obsBody(t, "urn:bq:orders:unknown")) },
			opts: []babelqueue.AppOption{babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDeadLetter)},

			wantReason: "unknown_urn",
		},
		{
			name:       "undecodable poison body",
			msg:        func(*testing.T) *babelqueue.ReceivedMessage { return obsMsg("not json at all") },
			opts:       []babelqueue.AppOption{babelqueue.WithDeadLetter(true)},
			wantReason: "poison",
		},
		{
			name: "transport-flagged poison",
			msg: func(*testing.T) *babelqueue.ReceivedMessage {
				m := obsMsg(`{"job":"urn:bq:orders:created","attempts":null,"data":{"n":1}}`)
				m.Poison = true
				return m
			},
			wantReason: "poison",
		},
		{
			name:       "raw version-gated body has no reason",
			msg:        func(*testing.T) *babelqueue.ReceivedMessage { return gatedMsg() },
			opts:       []babelqueue.AppOption{babelqueue.WithDeadLetter(true)},
			wantReason: "",
		},
	}

	// Each case runs through both write paths: the plain re-publish and the DeadLetterer capability.
	for _, tc := range cases {
		for _, path := range []string{"republish", "DeadLetterer"} {
			t.Run(tc.name+"/"+path, func(t *testing.T) {
				rec := &obsRecorder{}
				base := newObsBase(tc.msg(t))
				var tr babelqueue.Transport = base
				if path == "DeadLetterer" {
					tr = &obsDLTransport{obsBase: base}
				}
				opts := append([]babelqueue.AppOption{babelqueue.WithDeadLetterObserver(rec.observe)}, tc.opts...)
				app := babelqueue.NewApp(tr, opts...)
				app.Handle("urn:bq:orders:created", failingHandler)
				drain(t, app)

				if len(rec.events) != 1 {
					t.Fatalf("observer called %d times, want exactly once: %+v", len(rec.events), rec.events)
				}
				want := babelqueue.DeadLetterEvent{Queue: "orders", DeadLetterQueue: "orders.dlq", Reason: tc.wantReason}
				if rec.events[0] != want {
					t.Errorf("event = %+v, want %+v", rec.events[0], want)
				}
			})
		}
	}
}

func TestDeadLetterObserverNotCalledOnFailedWrite(t *testing.T) {
	t.Run("republish to the DLQ fails", func(t *testing.T) {
		rec := &obsRecorder{}
		base := newObsBase(obsMsg(obsBody(t, "urn:bq:orders:created")))
		base.failTo = "orders.dlq"
		app := babelqueue.NewApp(base, babelqueue.WithDeadLetterObserver(rec.observe),
			babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true), babelqueue.WithRewindPause(0))
		app.Handle("urn:bq:orders:created", failingHandler)
		drain(t, app)
		if len(rec.events) != 0 {
			t.Fatalf("observer called after a failed write: %+v", rec.events)
		}
		if base.acks != 0 {
			t.Errorf("acks = %d: a message whose DLQ write failed must not be acknowledged", base.acks)
		}
	})

	t.Run("DeadLetterer fails", func(t *testing.T) {
		rec := &obsRecorder{}
		tr := &obsDLTransport{obsBase: newObsBase(gatedMsg()), err: errors.New("lost the reservation")}
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetterObserver(rec.observe),
			babelqueue.WithDeadLetter(true), babelqueue.WithRewindPause(0))
		drain(t, app)
		if tr.calls != 1 {
			t.Fatalf("DeadLetter calls = %d, want 1", tr.calls)
		}
		if len(rec.events) != 0 {
			t.Fatalf("observer called after a failed write: %+v", rec.events)
		}
	})
}

func TestDeadLetterObserverNotCalledWhenDLQIsOff(t *testing.T) {
	rec := &obsRecorder{}
	base := newObsBase(obsMsg(obsBody(t, "urn:bq:orders:created")))
	app := babelqueue.NewApp(base, babelqueue.WithDeadLetterObserver(rec.observe), babelqueue.WithMaxAttempts(1))
	app.Handle("urn:bq:orders:created", failingHandler)
	drain(t, app)
	if len(rec.events) != 0 {
		t.Fatalf("observer called for a dropped message: %+v", rec.events)
	}
	if base.acks != 1 {
		t.Errorf("acks = %d, want the dropped message acknowledged once", base.acks)
	}
}

func TestDeadLetterObserverNativeSettle(t *testing.T) {
	// Dead-lettering off, strategy not delete: a version-gated delivery takes the native settle.
	t.Run("variant reports true: observer called once, no BabelQueue DLQ", func(t *testing.T) {
		rec := &obsRecorder{}
		tr := &obsNativeDLTransport{obsBase: newObsBase(gatedMsg()), deadLettered: true}
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetterObserver(rec.observe))
		drain(t, app)

		if tr.variantCalls != 1 || tr.plainCalls != 0 {
			t.Fatalf("variant calls = %d, SettleNative calls = %d, want 1 and 0 (the variant is preferred)",
				tr.variantCalls, tr.plainCalls)
		}
		if len(rec.events) != 1 {
			t.Fatalf("observer called %d times, want once: %+v", len(rec.events), rec.events)
		}
		want := babelqueue.DeadLetterEvent{Queue: "orders"}
		if rec.events[0] != want {
			t.Errorf("event = %+v, want %+v", rec.events[0], want)
		}
		if rec.events[0].DeadLetterQueue != "" {
			t.Errorf("dead_letter_queue = %q, want empty for a native dead-letter", rec.events[0].DeadLetterQueue)
		}
		if len(tr.published) != 0 || tr.acks != 0 {
			t.Errorf("a native settle must publish and ack nothing: published=%v acks=%d", tr.published, tr.acks)
		}
	})

	t.Run("native settle of a poison body is counted too", func(t *testing.T) {
		rec := &obsRecorder{}
		tr := &obsNativeDLTransport{obsBase: newObsBase(obsMsg("not json at all")), deadLettered: true}
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetterObserver(rec.observe))
		drain(t, app)
		if len(rec.events) != 1 || rec.events[0].DeadLetterQueue != "" {
			t.Fatalf("events = %+v, want one event with no DLQ", rec.events)
		}
	})

	t.Run("variant reports false (abandon fallback): not called", func(t *testing.T) {
		rec := &obsRecorder{}
		tr := &obsNativeDLTransport{obsBase: newObsBase(gatedMsg()), deadLettered: false}
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetterObserver(rec.observe))
		drain(t, app)
		if tr.variantCalls != 1 {
			t.Fatalf("variant calls = %d, want 1", tr.variantCalls)
		}
		if len(rec.events) != 0 {
			t.Fatalf("observer called for a non-dead-lettering native settle: %+v", rec.events)
		}
	})

	t.Run("variant fails: not called", func(t *testing.T) {
		rec := &obsRecorder{}
		tr := &obsNativeDLTransport{obsBase: newObsBase(gatedMsg()), deadLettered: true, err: errors.New("lock lost")}
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetterObserver(rec.observe))
		drain(t, app)
		if tr.variantCalls != 1 {
			t.Fatalf("variant calls = %d, want 1", tr.variantCalls)
		}
		if len(rec.events) != 0 {
			t.Fatalf("observer called for a failed native call: %+v", rec.events)
		}
	})

	t.Run("NativeSettler only (SQS no-op): not called", func(t *testing.T) {
		rec := &obsRecorder{}
		tr := &obsNativeOnlyTransport{obsBase: newObsBase(gatedMsg())}
		app := babelqueue.NewApp(tr, babelqueue.WithDeadLetterObserver(rec.observe))
		drain(t, app)
		if tr.calls != 1 {
			t.Fatalf("SettleNative calls = %d, want 1", tr.calls)
		}
		if len(rec.events) != 0 {
			t.Fatalf("observer called for a native no-op: %+v", rec.events)
		}
	})
}

func TestDeadLetterObserverPanicDoesNotChangeSettle(t *testing.T) {
	base := newObsBase(obsMsg(obsBody(t, "urn:bq:orders:created")))
	app := babelqueue.NewApp(base,
		babelqueue.WithDeadLetterObserver(func(context.Context, babelqueue.DeadLetterEvent) { panic("observer bug") }),
		babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true))
	app.Handle("urn:bq:orders:created", failingHandler)
	drain(t, app)

	if got := len(base.published["orders.dlq"]); got != 1 {
		t.Fatalf("DLQ records = %d, want 1: a panicking observer must not undo the write", got)
	}
	if base.acks != 1 {
		t.Errorf("acks = %d, want the message acknowledged once as without an observer", base.acks)
	}
}

func TestDeadLetterObserverNilIsInert(t *testing.T) {
	base := newObsBase(obsMsg(obsBody(t, "urn:bq:orders:created")))
	app := babelqueue.NewApp(base, babelqueue.WithDeadLetterObserver(nil),
		babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true))
	app.Handle("urn:bq:orders:created", failingHandler)
	drain(t, app)
	if got := len(base.published["orders.dlq"]); got != 1 {
		t.Fatalf("DLQ records = %d, want 1", got)
	}
}

// The observer event's Queue falls back to the delivery's queue when meta.queue is blank (empty or
// only the ADR-0031 ASCII whitespace set). Any other value, a Unicode space included, is a queue
// name and is kept as is.
func TestDeadLetterEventQueueFallbackUsesTheASCIIBlankSet(t *testing.T) {
	for _, tc := range blankQueueCases {
		t.Run(tc.name, func(t *testing.T) {
			rec, _ := deadLetterUnknownURN(t, tc.metaQueue)
			if len(rec.events) != 1 || rec.events[0].Queue != tc.wantEventQueue {
				t.Fatalf("events = %+v, want one with Queue %q", rec.events, tc.wantEventQueue)
			}
		})
	}
}

// The wire is unchanged: the record's dead_letter.original_queue keeps its old rule (meta.queue when
// non-empty, else the delivery's queue), so a blank-but-not-empty meta.queue is written as is, whatever
// the observer event reports.
func TestDeadLetterRecordOriginalQueueIsUnchangedByTheEventFallback(t *testing.T) {
	for _, tc := range blankQueueCases {
		t.Run(tc.name, func(t *testing.T) {
			_, published := deadLetterUnknownURN(t, tc.metaQueue)
			if len(published) != 1 {
				t.Fatalf("DLQ records = %d, want 1", len(published))
			}
			rec, err := babelqueue.Decode([]byte(published[0]))
			if err != nil || rec.DeadLetter == nil {
				t.Fatalf("DLQ record does not decode with a dead_letter block: %v", err)
			}
			if rec.DeadLetter.OriginalQueue != tc.wantOriginalQueue {
				t.Errorf("dead_letter.original_queue = %q, want %q", rec.DeadLetter.OriginalQueue, tc.wantOriginalQueue)
			}
		})
	}
}

var blankQueueCases = []struct {
	name              string
	metaQueue         string
	wantEventQueue    string // the observer event
	wantOriginalQueue string // the record on the wire
}{
	{"billing", "billing", "billing", "billing"},
	{"empty", "", "orders", "orders"},
	{"spaces", "  ", "orders", "  "},
	{"single space", " ", "orders", " "},
	{"tab and newline", "\t\n", "orders", "\t\n"},
	{"vertical tab and form feed", "\v\f\r", "orders", "\v\f\r"},
	{"no-break space is not blank", "\u00a0", "\u00a0", "\u00a0"},
	{"ideographic space is not blank", "\u3000", "\u3000", "\u3000"},
}

// deadLetterUnknownURN dead-letters an unknown-URN message whose meta.queue is metaQueue, consumed
// from "orders", and returns the observer events and the bodies written to orders.dlq.
func deadLetterUnknownURN(t *testing.T, metaQueue string) (*obsRecorder, []string) {
	t.Helper()
	env, err := babelqueue.Make("urn:bq:orders:unknown", map[string]any{"n": 1}, babelqueue.WithQueue(metaQueue))
	if err != nil {
		t.Fatal(err)
	}
	body, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	rec := &obsRecorder{}
	base := newObsBase(obsMsg(string(body)))
	app := babelqueue.NewApp(base, babelqueue.WithDeadLetterObserver(rec.observe),
		babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDeadLetter))
	drain(t, app)
	return rec, base.published["orders.dlq"]
}
