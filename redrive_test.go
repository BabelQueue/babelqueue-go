package babelqueue

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// deadLettered builds a dead-lettered envelope and puts it on the dlq queue.
func deadLettered(t *testing.T, tr *InMemoryTransport, dlq, urn, originalQueue string, data map[string]any) Envelope {
	t.Helper()
	env, err := Make(urn, data, WithQueue(originalQueue))
	if err != nil {
		t.Fatal(err)
	}
	dl := Annotate(env, "failed", originalQueue, 3, errors.New("boom"))
	body, err := dl.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Publish(context.Background(), dlq, string(body)); err != nil {
		t.Fatal(err)
	}
	return dl
}

// drain reads and acks every message on a queue, returning the decoded envelopes.
func drain(t *testing.T, tr *InMemoryTransport, queue string) []Envelope {
	t.Helper()
	var out []Envelope
	for {
		msg, err := tr.Pop(context.Background(), queue, 0)
		if err != nil {
			t.Fatal(err)
		}
		if msg == nil {
			break
		}
		env, derr := Decode([]byte(msg.Body))
		if derr != nil {
			t.Fatalf("undecodable on %s: %v", queue, derr)
		}
		out = append(out, env)
		_ = tr.Ack(context.Background(), msg)
	}
	return out
}

func TestRedrive_ToSource(t *testing.T) {
	tr := NewInMemoryTransport()
	orig := deadLettered(t, tr, "orders.dlq", "urn:babel:orders:created", "orders", map[string]any{"order_id": 1})

	res, err := Redrive(context.Background(), tr, "orders.dlq", RedriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 || res.Skipped != 0 {
		t.Fatalf("res = %+v", res)
	}

	got := drain(t, tr, "orders")
	if len(got) != 1 {
		t.Fatalf("source queue has %d messages, want 1", len(got))
	}
	if got[0].DeadLetter != nil {
		t.Error("dead_letter block was not stripped")
	}
	if got[0].Attempts != 0 {
		t.Errorf("attempts = %d, want 0 (reset)", got[0].Attempts)
	}
	if got[0].TraceID != orig.TraceID {
		t.Error("trace_id was not preserved")
	}
	if got[0].URN() != "urn:babel:orders:created" {
		t.Errorf("urn = %q", got[0].URN())
	}
	if d := drain(t, tr, "orders.dlq"); len(d) != 0 {
		t.Errorf("DLQ not drained: %d left", len(d))
	}
}

func TestRedrive_ToSandbox(t *testing.T) {
	tr := NewInMemoryTransport()
	deadLettered(t, tr, "orders.dlq", "urn:babel:orders:created", "orders", nil)

	res, err := Redrive(context.Background(), tr, "orders.dlq", RedriveOptions{ToQueue: "sandbox"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 {
		t.Fatalf("res = %+v", res)
	}
	if len(drain(t, tr, "orders")) != 0 {
		t.Error("sandbox replay must not touch the source queue")
	}
	if len(drain(t, tr, "sandbox")) != 1 {
		t.Error("message did not land on the sandbox queue")
	}
}

func TestRedrive_DryRun(t *testing.T) {
	tr := NewInMemoryTransport()
	deadLettered(t, tr, "orders.dlq", "urn:babel:orders:created", "orders", nil)

	res, err := Redrive(context.Background(), tr, "orders.dlq", RedriveOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 0 || res.Skipped != 1 {
		t.Fatalf("res = %+v", res)
	}
	if len(res.Items) != 1 || res.Items[0].To != "orders" || res.Items[0].Redriven {
		t.Errorf("dry-run should report the plan without redriving: %+v", res.Items)
	}
	if len(drain(t, tr, "orders")) != 0 {
		t.Error("dry-run touched the source queue")
	}
	if d := drain(t, tr, "orders.dlq"); len(d) != 1 || d[0].DeadLetter == nil {
		t.Error("dry-run altered the DLQ")
	}
}

func TestRedrive_Select(t *testing.T) {
	tr := NewInMemoryTransport()
	deadLettered(t, tr, "dlq", "urn:babel:orders:created", "orders", nil)
	deadLettered(t, tr, "dlq", "urn:babel:emails:welcome", "emails", nil)

	res, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{
		Select: func(e Envelope) bool { return e.URN() == "urn:babel:orders:created" },
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 || res.Skipped != 1 {
		t.Fatalf("res = %+v", res)
	}
	if len(drain(t, tr, "orders")) != 1 {
		t.Error("selected message was not redriven")
	}
	if len(drain(t, tr, "emails")) != 0 {
		t.Error("unselected message was wrongly redriven")
	}
	if len(drain(t, tr, "dlq")) != 1 {
		t.Error("unselected message was not restored to the DLQ")
	}
}

func TestRedrive_Max(t *testing.T) {
	tr := NewInMemoryTransport()
	for i := 0; i < 3; i++ {
		deadLettered(t, tr, "dlq", "urn:babel:orders:created", "orders", nil)
	}
	res, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{Max: 2})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 2 {
		t.Fatalf("res = %+v", res)
	}
	if len(drain(t, tr, "dlq")) != 1 {
		t.Error("Max was not respected — the DLQ should still hold 1")
	}
}

// failOnTarget refuses to publish to one queue, to exercise the restore-on-failure path.
type failOnTarget struct {
	*InMemoryTransport
	failQueue string
}

func (f *failOnTarget) Publish(ctx context.Context, queue, body string) error {
	if queue == f.failQueue {
		return errors.New("publish refused")
	}
	return f.InMemoryTransport.Publish(ctx, queue, body)
}

func TestRedrive_PublishFailureRestores(t *testing.T) {
	base := NewInMemoryTransport()
	deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
	tr := &failOnTarget{InMemoryTransport: base, failQueue: "orders"}

	if _, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{}); err == nil {
		t.Fatal("expected the publish error to surface")
	}
	if len(drain(t, base, "dlq")) != 1 {
		t.Error("a message must be restored to the DLQ when its re-publish fails")
	}
	if len(drain(t, base, "orders")) != 0 {
		t.Error("nothing should have reached the source queue")
	}
}

func TestRedrive_NoDeadLetterFallsBackToMetaQueue(t *testing.T) {
	tr := NewInMemoryTransport()
	// a plain (never dead-lettered) envelope sitting on the DLQ — redrive falls back to meta.queue
	env, err := Make("urn:babel:orders:created", nil, WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Publish(context.Background(), "dlq", string(body)); err != nil {
		t.Fatal(err)
	}

	res, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 {
		t.Fatalf("res = %+v", res)
	}
	if len(drain(t, tr, "orders")) != 1 {
		t.Error("a message with no dead_letter block should redrive to its meta.queue")
	}
}

func TestRedrive_UndecodableRestored(t *testing.T) {
	tr := NewInMemoryTransport()
	if err := tr.Publish(context.Background(), "dlq", "not-json{{{"); err != nil {
		t.Fatal(err)
	}
	res, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 0 || res.Skipped != 1 {
		t.Fatalf("res = %+v", res)
	}
	msg, _ := tr.Pop(context.Background(), "dlq", 0)
	if msg == nil || msg.Body != "not-json{{{" {
		t.Error("an undecodable body must be restored to the DLQ, not lost")
	}
}

// routedDLQ stamps a RouteURN on every message it pops and records what is published through
// the optional RoutedPublisher capability as (queue, routeURN).
type routedDLQ struct {
	*InMemoryTransport
	route  string
	routed [][2]string
}

func (r *routedDLQ) Pop(ctx context.Context, queue string, timeout time.Duration) (*ReceivedMessage, error) {
	msg, err := r.InMemoryTransport.Pop(ctx, queue, timeout)
	if msg != nil {
		msg.RouteURN = r.route
	}
	return msg, err
}

func (r *routedDLQ) PublishRouted(ctx context.Context, queue, body, routeURN string) error {
	r.routed = append(r.routed, [2]string{queue, routeURN})
	return r.InMemoryTransport.Publish(ctx, queue, body)
}

// Redrive keeps the dead-lettered message's route URN on the message it writes back, and on a
// message it restores to the DLQ (unselected / dry-run); without a route it is a plain Publish.
func TestRedrive_KeepsTheRouteURN(t *testing.T) {
	const route = "urn:babel:orders:refunded"
	tr := &routedDLQ{InMemoryTransport: NewInMemoryTransport(), route: route}
	deadLettered(t, tr.InMemoryTransport, "dlq", "urn:babel:orders:created", "orders", nil)

	if _, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(tr.routed) != 1 || tr.routed[0] != [2]string{"orders", route} {
		t.Errorf("redrive routed = %v, want [[orders %s]]", tr.routed, route)
	}

	tr2 := &routedDLQ{InMemoryTransport: NewInMemoryTransport(), route: route}
	deadLettered(t, tr2.InMemoryTransport, "dlq", "urn:babel:orders:created", "orders", nil)
	if _, err := Redrive(context.Background(), tr2, "dlq", RedriveOptions{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if len(tr2.routed) != 1 || tr2.routed[0] != [2]string{"dlq", route} {
		t.Errorf("dry-run restore routed = %v, want [[dlq %s]]", tr2.routed, route)
	}

	tr3 := &routedDLQ{InMemoryTransport: NewInMemoryTransport()} // no route
	deadLettered(t, tr3.InMemoryTransport, "dlq", "urn:babel:orders:created", "orders", nil)
	if _, err := Redrive(context.Background(), tr3, "dlq", RedriveOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(tr3.routed) != 0 || len(drain(t, tr3.InMemoryTransport, "orders")) != 1 {
		t.Errorf("no-route redrive used PublishRouted: %v", tr3.routed)
	}
}

// failAllPublishAck refuses every publish and records acks, to prove a message is acknowledged
// only after its restore write to the DLQ succeeded.
type failAllPublishAck struct {
	*InMemoryTransport
	acks int
}

func (f *failAllPublishAck) Publish(context.Context, string, string) error {
	return errors.New("broker unavailable")
}

func (f *failAllPublishAck) Ack(context.Context, *ReceivedMessage) error {
	f.acks++
	return nil
}

func TestRedrive_FailedRestoreIsNotAcked(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts RedriveOptions
	}{
		{"dry-run", RedriveOptions{DryRun: true}},
		{"not-selected", RedriveOptions{Select: func(Envelope) bool { return false }}},
		{"redrive-then-restore", RedriveOptions{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := NewInMemoryTransport()
			deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
			tr := &failAllPublishAck{InMemoryTransport: base}

			if _, err := Redrive(context.Background(), tr, "dlq", tc.opts); err == nil {
				t.Fatal("expected the failed write to surface")
			}
			if tr.acks != 0 {
				t.Errorf("a message whose restore failed must not be acknowledged, got %d ack(s)", tr.acks)
			}
		})
	}
}

// rewindingRestoreTransport refuses every publish (so a restore write fails) and records Rewind
// and Ack calls, to prove a failed restore rewinds the record instead of acknowledging it.
type rewindingRestoreTransport struct {
	*InMemoryTransport
	acks      int
	rewound   []*ReceivedMessage
	rewindErr error
}

func (r *rewindingRestoreTransport) Publish(context.Context, string, string) error {
	return errors.New("broker unavailable")
}

func (r *rewindingRestoreTransport) Ack(context.Context, *ReceivedMessage) error {
	r.acks++
	return nil
}

func (r *rewindingRestoreTransport) Rewind(_ context.Context, msg *ReceivedMessage) error {
	r.rewound = append(r.rewound, msg)
	return r.rewindErr
}

func TestRedrive_FailedRestoreRewinds(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts RedriveOptions
	}{
		{"dry-run", RedriveOptions{DryRun: true}},
		{"not-selected", RedriveOptions{Select: func(Envelope) bool { return false }}},
		{"redrive-then-restore", RedriveOptions{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := NewInMemoryTransport()
			deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
			tr := &rewindingRestoreTransport{InMemoryTransport: base}

			if _, err := Redrive(context.Background(), tr, "dlq", tc.opts); err == nil {
				t.Fatal("expected the failed write to surface")
			}
			if len(tr.rewound) != 1 {
				t.Fatalf("Rewind calls = %d, want 1", len(tr.rewound))
			}
			if tr.acks != 0 {
				t.Errorf("a message whose restore failed must not be acknowledged, got %d ack(s)", tr.acks)
			}
		})
	}
}

func TestRedrive_UndecodableFailedRestoreRewinds(t *testing.T) {
	base := NewInMemoryTransport()
	if err := base.Publish(context.Background(), "dlq", "not json"); err != nil {
		t.Fatal(err)
	}
	tr := &rewindingRestoreTransport{InMemoryTransport: base}
	if _, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{}); err == nil {
		t.Fatal("expected the failed write to surface")
	}
	if len(tr.rewound) != 1 || tr.acks != 0 {
		t.Errorf("rewound=%d acks=%d, want 1 and 0", len(tr.rewound), tr.acks)
	}
}

func TestRedrive_RewindFailureIsReportedWithTheRestoreError(t *testing.T) {
	base := NewInMemoryTransport()
	deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
	rewindErr := errors.New("rewind failed")
	tr := &rewindingRestoreTransport{InMemoryTransport: base, rewindErr: rewindErr}

	_, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{DryRun: true})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, rewindErr) {
		t.Errorf("rewind error missing from %v", err)
	}
	if err.Error() == rewindErr.Error() {
		t.Errorf("restore error missing from %v", err)
	}
	if tr.acks != 0 {
		t.Errorf("acks = %d, want 0", tr.acks)
	}
}

// A failed redrive publish whose restore also fails reports both errors and rewinds.
func TestRedrive_PublishAndRestoreFailureReportBoth(t *testing.T) {
	base := NewInMemoryTransport()
	deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
	tr := &rewindingRestoreTransport{InMemoryTransport: base}

	_, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{})
	if err == nil {
		t.Fatal("expected an error")
	}
	if len(tr.rewound) != 1 || tr.acks != 0 {
		t.Errorf("rewound=%d acks=%d, want 1 and 0", len(tr.rewound), tr.acks)
	}
}

// workQueueDownTransport refuses publishes to every queue but the DLQ, so a redrive publish fails
// while the restore write back to the DLQ succeeds. It records Rewind calls.
type workQueueDownTransport struct {
	*InMemoryTransport
	dlq       string
	acks      int
	rewound   []*ReceivedMessage
	rewindErr error
}

func (w *workQueueDownTransport) Publish(ctx context.Context, queue, body string) error {
	if queue != w.dlq {
		return errors.New("work queue unavailable")
	}
	return w.InMemoryTransport.Publish(ctx, queue, body)
}

func (w *workQueueDownTransport) Ack(context.Context, *ReceivedMessage) error {
	w.acks++
	return nil
}

func (w *workQueueDownTransport) Rewind(_ context.Context, msg *ReceivedMessage) error {
	w.rewound = append(w.rewound, msg)
	return w.rewindErr
}

// A failed redrive publish whose restore succeeds still aborts the run, leaving the rest of the
// drained batch popped but unprocessed: the transport is rewound so a later cumulative commit
// cannot skip those records, and the publish error is returned unchanged.
func TestRedrive_PublishFailureRestoredThenRewindsRemainingBatch(t *testing.T) {
	base := NewInMemoryTransport()
	deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
	deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
	tr := &workQueueDownTransport{InMemoryTransport: base, dlq: "dlq"}

	_, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{})
	if err == nil || err.Error() != "work queue unavailable" {
		t.Fatalf("err = %v, want the bare publish error", err)
	}
	if len(tr.rewound) != 1 {
		t.Errorf("Rewind calls = %d, want 1", len(tr.rewound))
	}
	if tr.acks != 1 {
		t.Errorf("acks = %d, want 1 (the restored record only)", tr.acks)
	}
}

func TestRedrive_PublishFailureRewindErrorIsJoined(t *testing.T) {
	base := NewInMemoryTransport()
	deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
	rewindErr := errors.New("rewind failed")
	tr := &workQueueDownTransport{InMemoryTransport: base, dlq: "dlq", rewindErr: rewindErr}

	_, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{})
	if !errors.Is(err, rewindErr) || err.Error() == rewindErr.Error() {
		t.Errorf("err = %v, want publish and rewind errors together", err)
	}
}

// A transport that cannot rewind still returns the restore error and never acks.
func TestRedrive_FailedRestoreWithoutRewinderStillNoAck(t *testing.T) {
	base := NewInMemoryTransport()
	deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
	tr := &failAllPublishAck{InMemoryTransport: base}
	if _, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{DryRun: true}); err == nil {
		t.Fatal("expected an error")
	}
	if tr.acks != 0 {
		t.Errorf("acks = %d, want 0", tr.acks)
	}
}

func TestRedrive_LogsDecodeWarnings(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	tr := NewInMemoryTransport()
	env, err := Make("urn:babel:orders:created", map[string]any{"order_id": 1}, WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	enc, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	// Inject a forbidden top-level key, as a foreign producer might have written it.
	body := strings.TrimSuffix(string(enc), "}") + `,"timestamp":123}`
	if err := tr.Publish(context.Background(), "orders.dlq", body); err != nil {
		t.Fatal(err)
	}

	res, err := Redrive(context.Background(), tr, "orders.dlq", RedriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 {
		t.Fatalf("res = %+v", res)
	}
	out := buf.String()
	for _, want := range []string{"babelqueue: decode warning", "/timestamp", "queue=orders.dlq"} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q:\n%s", want, out)
		}
	}
}
