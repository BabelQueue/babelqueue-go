package schema

import (
	"context"
	"errors"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

func newProvider(t *testing.T) Provider {
	t.Helper()
	p, err := NewMapProvider(map[string][]byte{
		"urn:babel:orders:created": []byte(`{"type":"object","required":["order_id"],"properties":{"order_id":{"type":"integer"}},"additionalProperties":false}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

type errProvider struct{}

func (errProvider) Schema(string) (*Schema, bool, error) { return nil, false, errors.New("boom") }

func TestCheck(t *testing.T) {
	p := newProvider(t)
	if err := Check(p, "urn:babel:orders:created", map[string]any{"order_id": 1.0}); err != nil {
		t.Fatalf("valid data should pass: %v", err)
	}
	err := Check(p, "urn:babel:orders:created", map[string]any{})
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("invalid data should wrap ErrInvalidPayload, got %v", err)
	}
	if err := Check(p, "urn:babel:unknown", map[string]any{"x": 1.0}); err != nil {
		t.Fatalf("an unregistered urn should pass (opt-in), got %v", err)
	}
	if err := Check(errProvider{}, "u", map[string]any{}); err == nil {
		t.Fatal("a provider lookup error should propagate")
	}
}

func TestValidate_Envelope(t *testing.T) {
	p := newProvider(t)
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1.0})
	if err := Validate(p, env); err != nil {
		t.Fatalf("valid envelope should pass: %v", err)
	}
	bad, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": "x"})
	if err := Validate(p, bad); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("invalid envelope should wrap ErrInvalidPayload, got %v", err)
	}
}

func TestWrap(t *testing.T) {
	p := newProvider(t)
	called := 0
	h := func(_ context.Context, _ babelqueue.Envelope) error { called++; return nil }
	wrapped := Wrap(p, h)
	ctx := context.Background()

	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1.0})
	if err := wrapped(ctx, env); err != nil || called != 1 {
		t.Fatalf("valid data → handler runs: err=%v called=%d", err, called)
	}

	bad, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{})
	if err := wrapped(ctx, bad); !errors.Is(err, ErrInvalidPayload) || called != 1 {
		t.Fatalf("invalid data → ErrInvalidPayload, handler skipped: err=%v called=%d", err, called)
	}

	un, _ := babelqueue.Make("urn:babel:unknown", map[string]any{"x": 1.0})
	if err := wrapped(ctx, un); err != nil || called != 2 {
		t.Fatalf("unregistered urn → handler runs: err=%v called=%d", err, called)
	}

	wrappedErr := Wrap(errProvider{}, h)
	if err := wrappedErr(ctx, env); err == nil || called != 2 {
		t.Fatalf("provider error → propagate, handler skipped: err=%v called=%d", err, called)
	}
}
