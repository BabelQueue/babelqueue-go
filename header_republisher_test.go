package babelqueue

import (
	"context"
	"testing"
)

// republisherTransport records which publish path Redrive took.
type republisherTransport struct {
	*InMemoryTransport
	viaFrom    []map[string]string
	viaHeaders []map[string]string
	fromSeen   []*ReceivedMessage
}

func (r *republisherTransport) PublishWithHeaders(ctx context.Context, queue, body string, headers map[string]string) error {
	r.viaHeaders = append(r.viaHeaders, headers)
	return r.InMemoryTransport.PublishWithHeaders(ctx, queue, body, headers)
}

func (r *republisherTransport) PublishFromWithHeaders(ctx context.Context, queue, body string, from *ReceivedMessage, headers map[string]string) error {
	r.viaFrom = append(r.viaFrom, headers)
	r.fromSeen = append(r.fromSeen, from)
	return r.InMemoryTransport.PublishWithHeaders(ctx, queue, body, headers)
}

// Redrive{Bypass: true} prefers HeaderRepublisher (which sees the reserved DLQ message) over a
// bare HeaderPublisher.PublishWithHeaders, and still reports Bypassed.
func TestRedriveBypassPrefersHeaderRepublisher(t *testing.T) {
	rt := &republisherTransport{InMemoryTransport: NewInMemoryTransport()}
	deadLettered(t, rt.InMemoryTransport, "orders.dlq", "urn:babel:orders:created", "orders", map[string]any{"order_id": 1})

	res, err := Redrive(context.Background(), rt, "orders.dlq", RedriveOptions{Bypass: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 || !res.Items[0].Bypassed {
		t.Fatalf("expected one bypassed redrive: %+v", res)
	}
	if len(rt.viaFrom) != 1 || len(rt.viaHeaders) != 0 {
		t.Fatalf("via HeaderRepublisher = %d, via bare PublishWithHeaders = %d; want 1, 0", len(rt.viaFrom), len(rt.viaHeaders))
	}
	if rt.viaFrom[0][HeaderReplayBypass] != "1" || rt.fromSeen[0] == nil || rt.fromSeen[0].Queue != "orders.dlq" {
		t.Errorf("headers = %v, from = %+v", rt.viaFrom[0], rt.fromSeen[0])
	}
	msg, _ := rt.Pop(context.Background(), "orders", 0)
	if msg == nil || msg.Headers[HeaderReplayBypass] != "1" {
		t.Fatalf("redriven message lacks the bypass header: %+v", msg)
	}
}

// Without Bypass the HeaderRepublisher is not used (the plain republish path is unchanged).
func TestRedriveWithoutBypassIgnoresHeaderRepublisher(t *testing.T) {
	rt := &republisherTransport{InMemoryTransport: NewInMemoryTransport()}
	deadLettered(t, rt.InMemoryTransport, "orders.dlq", "urn:babel:orders:created", "orders", map[string]any{"order_id": 1})

	res, err := Redrive(context.Background(), rt, "orders.dlq", RedriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 || res.Items[0].Bypassed || len(rt.viaFrom) != 0 || len(rt.viaHeaders) != 0 {
		t.Fatalf("res = %+v, viaFrom = %d, viaHeaders = %d", res, len(rt.viaFrom), len(rt.viaHeaders))
	}
}
