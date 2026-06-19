package babelqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIsReplayAndBypassExternalEffects(t *testing.T) {
	bg := context.Background()
	if IsReplay(bg) {
		t.Error("a plain context must not be a replay")
	}
	rp := withReplay(bg)
	if !IsReplay(rp) {
		t.Error("a withReplay context must be a replay")
	}

	ran := false
	if err := BypassExternalEffects(bg, func() error { ran = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Error("fn must run when not a replay")
	}

	ran = false
	if err := BypassExternalEffects(rp, func() error { ran = true; return errors.New("must not run") }); err != nil {
		t.Fatalf("a replay must skip fn and return nil, got %v", err)
	}
	if ran {
		t.Error("fn must be skipped on a replay")
	}
}

func TestInMemoryTransportHeadersRoundTrip(t *testing.T) {
	tr := NewInMemoryTransport()
	if err := tr.PublishWithHeaders(context.Background(), "q", "body", map[string]string{HeaderReplayBypass: "1"}); err != nil {
		t.Fatal(err)
	}
	msg, _ := tr.Pop(context.Background(), "q", 0)
	if msg == nil || msg.Headers[HeaderReplayBypass] != "1" {
		t.Fatalf("headers not carried through Pop: %+v", msg)
	}
	_ = tr.Publish(context.Background(), "q", "plain")
	m2, _ := tr.Pop(context.Background(), "q", 0)
	if m2.Headers[HeaderReplayBypass] != "" {
		t.Error("a plain Publish must carry no replay header")
	}
}

func TestRedriveBypassStampsHeaderAndConsumeSkipsEffects(t *testing.T) {
	tr := NewInMemoryTransport()
	deadLettered(t, tr, "orders.dlq", "urn:babel:orders:created", "orders", map[string]any{"order_id": 1})

	res, err := Redrive(context.Background(), tr, "orders.dlq", RedriveOptions{Bypass: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 || len(res.Items) != 1 || !res.Items[0].Bypassed {
		t.Fatalf("expected one bypassed redrive: %+v", res)
	}

	msg, _ := tr.Pop(context.Background(), "orders", 0)
	if msg == nil || msg.Headers[HeaderReplayBypass] != "1" {
		t.Fatalf("redriven message is missing the bypass header: %+v", msg)
	}

	emailed := false
	app := NewApp(tr)
	app.Handle("urn:babel:orders:created", func(ctx context.Context, _ Envelope) error {
		if !IsReplay(ctx) {
			t.Error("the handler should see this delivery as a replay")
		}
		return BypassExternalEffects(ctx, func() error { emailed = true; return nil })
	})
	app.dispatch(context.Background(), msg)
	if emailed {
		t.Error("the external side-effect must be skipped on a bypassed replay")
	}
}

func TestRedriveBypassWithoutHeaderSupportFallsBack(t *testing.T) {
	base := NewInMemoryTransport()
	deadLettered(t, base, "dlq", "urn:babel:orders:created", "orders", nil)
	tr := plainTransport{inner: base}

	res, err := Redrive(context.Background(), tr, "dlq", RedriveOptions{Bypass: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 || res.Items[0].Bypassed {
		t.Fatalf("Bypass must be a no-op when the transport is not a HeaderPublisher: %+v", res)
	}
}

// plainTransport wraps InMemoryTransport but exposes only the base Transport methods, so it is
// deliberately NOT a HeaderPublisher.
type plainTransport struct{ inner *InMemoryTransport }

func (p plainTransport) Publish(ctx context.Context, q, b string) error {
	return p.inner.Publish(ctx, q, b)
}

func (p plainTransport) Pop(ctx context.Context, q string, d time.Duration) (*ReceivedMessage, error) {
	return p.inner.Pop(ctx, q, d)
}

func (p plainTransport) Ack(ctx context.Context, m *ReceivedMessage) error {
	return p.inner.Ack(ctx, m)
}
