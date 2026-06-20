package babelqueue

import (
	"context"
	"testing"
)

func TestHeadersFromContext(t *testing.T) {
	if HeadersFromContext(context.Background()) != nil {
		t.Error("a plain context must surface no headers")
	}
	// withHeaders drops empty maps so a header-less delivery stays header-less.
	if HeadersFromContext(withHeaders(context.Background(), nil)) != nil {
		t.Error("nil headers must not be stashed")
	}
	if HeadersFromContext(withHeaders(context.Background(), map[string]string{})) != nil {
		t.Error("an empty header map must not be stashed")
	}

	ctx := withHeaders(context.Background(), map[string]string{"traceparent": "tp"})
	if got := HeadersFromContext(ctx); got["traceparent"] != "tp" {
		t.Fatalf("headers not surfaced: %+v", got)
	}
}

func TestDispatchSurfacesHeadersToHandler(t *testing.T) {
	tr := NewInMemoryTransport()
	body, _ := mustEnv(t, "urn:babel:orders:created").Encode()
	if err := tr.PublishWithHeaders(context.Background(), "default", string(body), map[string]string{"traceparent": "tp-value"}); err != nil {
		t.Fatal(err)
	}
	msg, _ := tr.Pop(context.Background(), "default", 0)

	var seen string
	app := NewApp(tr)
	app.Handle("urn:babel:orders:created", func(ctx context.Context, _ Envelope) error {
		seen = HeadersFromContext(ctx)["traceparent"]
		return nil
	})
	app.dispatch(context.Background(), msg)

	if seen != "tp-value" {
		t.Fatalf("the runtime did not surface the transport header to the handler: %q", seen)
	}
}

func TestPublishWithHeaders_CarriesAndFallsBack(t *testing.T) {
	// A HeaderPublisher transport carries the headers through to the delivery.
	tr := NewInMemoryTransport()
	app := NewApp(tr)
	if _, err := app.PublishWithHeaders(context.Background(), "urn:babel:orders:created",
		map[string]any{"order_id": 1}, map[string]string{"traceparent": "tp"}); err != nil {
		t.Fatal(err)
	}
	msg, _ := tr.Pop(context.Background(), "default", 0)
	if msg == nil || msg.Headers["traceparent"] != "tp" {
		t.Fatalf("headers not carried by PublishWithHeaders: %+v", msg)
	}

	// A non-HeaderPublisher transport degrades to a plain publish: the message still
	// goes out (no error), the headers are simply dropped.
	plain := plainTransport{inner: NewInMemoryTransport()}
	app2 := NewApp(plain)
	id, err := app2.PublishWithHeaders(context.Background(), "urn:babel:orders:created",
		map[string]any{"order_id": 2}, map[string]string{"traceparent": "tp"})
	if err != nil || id == "" {
		t.Fatalf("fallback publish failed: id=%q err=%v", id, err)
	}
	m2, _ := plain.Pop(context.Background(), "default", 0)
	if m2 == nil {
		t.Fatal("fallback publish dropped the message")
	}
	if m2.Headers["traceparent"] != "" {
		t.Error("a non-HeaderPublisher transport must not carry headers")
	}
}

func mustEnv(t *testing.T, urn string) Envelope {
	t.Helper()
	env, err := Make(urn, map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	return env
}
