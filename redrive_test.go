package babelqueue

import (
	"context"
	"errors"
	"testing"
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
