package otel

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestTraceparentConformance runs the `traceparent` section of the vendored cross-SDK
// conformance suite (ADR-0028, ADR-0032 section 2). Scope `otel`: this module carries the
// W3C traceparent extract code, so it is the one that runs the section.
func TestTraceparentConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "testdata", "conformance", "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m struct {
		Traceparent struct {
			Cases []struct {
				Name        string  `json:"name"`
				Mode        string  `json:"mode"`
				Traceparent *string `json:"traceparent"`
				TraceID     string  `json:"trace_id"`
				Expect      struct {
					Source       string `json:"source"`
					TraceIDHex   string `json:"trace_id_hex"`
					ParentSpanID string `json:"parent_span_id"`
					Flags        string `json:"flags"`
				} `json:"expect"`
			} `json:"cases"`
		} `json:"traceparent"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(m.Traceparent.Cases) == 0 {
		t.Fatal("manifest has no traceparent cases")
	}
	for _, c := range m.Traceparent.Cases {
		t.Run(c.Name, func(t *testing.T) {
			if c.Mode != "extract" {
				t.Fatalf("unsupported mode %q", c.Mode)
			}
			header := ""
			if c.Traceparent != nil {
				header = *c.Traceparent
			}
			want := Extracted{
				Source: c.Expect.Source, TraceIDHex: c.Expect.TraceIDHex,
				ParentSpanID: c.Expect.ParentSpanID, Flags: c.Expect.Flags,
			}
			if got := ExtractTraceparent(header, c.TraceID); got != want {
				t.Fatalf("extract(%q, %q) = %+v, want %+v", header, c.TraceID, got, want)
			}
			// Runtime path: deliver the header through the real transport -> App.dispatch
			// seam and observe the parent WrapHandler actually gives the consumer span.
			if got := runtimeExtract(t, c.Traceparent, c.TraceID); got != want {
				t.Fatalf("runtime extract(%q, %q) = %+v, want %+v", header, c.TraceID, got, want)
			}
		})
	}
}

// runtimeExtract drives the production consume path: the header rides the transport's
// out-of-band channel, App dispatch surfaces it on the handler context, and
// [WrapHandler] starts the consumer span. The recorded span's parent is what a real
// consumer would link to.
func runtimeExtract(t *testing.T, traceparent *string, envelopeTraceID string) Extracted {
	t.Helper()
	var headers map[string]string
	if traceparent != nil {
		headers = map[string]string{HeaderTraceparent: *traceparent}
	}
	// AlwaysSample so even a not-sampled remote parent still records the consumer span.
	sr := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr)).Tracer("conformance")

	env, err := babelqueue.Make("urn:babel:conformance:traceparent", map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	env.TraceID = envelopeTraceID
	body, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	tr := babelqueue.NewInMemoryTransport()
	if err := tr.PublishWithHeaders(context.Background(), "default", string(body), headers); err != nil {
		t.Fatal(err)
	}
	app := babelqueue.NewApp(tr)
	app.Handle("urn:babel:conformance:traceparent", WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil }))
	if _, err := app.Drain(context.Background(), "default", 1); err != nil {
		t.Fatal(err)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("want 1 consumer span, got %d", len(spans))
	}
	parent := spans[0].Parent()
	if !parent.IsValid() || !parent.IsRemote() {
		t.Fatalf("consumer span has no valid remote parent: %+v", parent)
	}
	// The trace_id fallback derives its parent from the envelope, with no header-level
	// span/flags to report; the traceparent source reports the header's parts.
	if traceparent == nil || !headerUsed(parent, *traceparent) {
		return Extracted{Source: "trace_id", TraceIDHex: parent.TraceID().String()}
	}
	return Extracted{
		Source: "traceparent", TraceIDHex: parent.TraceID().String(),
		ParentSpanID: parent.SpanID().String(), Flags: flagsHex(parent),
	}
}

// headerUsed reports whether the span parent came from the delivered traceparent (its
// trace id and span id both appear in the header) rather than the trace_id fallback.
func headerUsed(parent trace.SpanContext, header string) bool {
	return strings.Contains(header, parent.TraceID().String()) && strings.Contains(header, parent.SpanID().String())
}

func flagsHex(sc trace.SpanContext) string {
	const hex = "0123456789abcdef"
	b := byte(sc.TraceFlags())
	return string([]byte{hex[b>>4], hex[b&0x0f]})
}

// TestExtractTraceparentAgreesWithRuntime pins the inputs the shared fixtures do not
// list (all-zero ids, uppercase hex, a future version): ExtractTraceparent and the
// WrapHandler runtime path must reach the same decision, because both go through the
// W3C propagator. A hand-written structural check used to accept some of these.
func TestExtractTraceparentAgreesWithRuntime(t *testing.T) {
	const traceID = "7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b"
	for name, header := range map[string]string{
		"zero-trace-id":     "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"zero-parent-id":    "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"uppercase-hex":     "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01",
		"version-ff":        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"future-version-01": "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	} {
		t.Run(name, func(t *testing.T) {
			h := header
			got := ExtractTraceparent(h, traceID)
			if rt := runtimeExtract(t, &h, traceID); got != rt {
				t.Fatalf("ExtractTraceparent = %+v, runtime = %+v", got, rt)
			}
		})
	}
}
