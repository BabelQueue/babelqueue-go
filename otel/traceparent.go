package otel

import (
	"context"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HeaderTraceparent / HeaderTracestate are the out-of-band transport headers that
// carry W3C Trace Context across a hop (ADR-0028, implementing ADR-0025 Option 2).
// They ride beside the frozen envelope on the transport's per-message metadata
// channel — the same out-of-band seam as the replay-bypass marker (ADR-0027) — so
// a consumer can start its span as a true child of the producer's span, not merely
// share the trace_id-derived trace. The envelope is never touched (GR-1).
const (
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

// propagator is the W3C Trace Context propagator. It reads/writes the traceparent
// (and tracestate) headers, which is exactly the wire format ADR-0025 Option 2
// names — so a babelqueue traceparent header interoperates with any OTel SDK or
// W3C-compliant peer.
var propagator = propagation.TraceContext{}

// mapCarrier adapts a map[string]string to OTel's [propagation.TextMapCarrier], so
// the W3C propagator can inject/extract traceparent into/out of the SDK-owned
// transport-header map carried by [babelqueue.HeaderPublisher] /
// [babelqueue.ReceivedMessage.Headers].
type mapCarrier map[string]string

func (c mapCarrier) Get(key string) string { return c[key] }
func (c mapCarrier) Set(key, value string) { c[key] = value }

func (c mapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

var _ propagation.TextMapCarrier = mapCarrier(nil)

// injectTraceparent writes the active span context in ctx as W3C traceparent (and
// tracestate, when present) into headers, returning the populated map. It is the
// producer half: the resulting headers are then handed to a
// [babelqueue.HeaderPublisher] so the consumer can reconstruct the remote parent.
// When ctx carries no valid span context, the propagator writes nothing and the
// map is returned unchanged (so a no-trace publish stays header-free).
func injectTraceparent(ctx context.Context, headers map[string]string) map[string]string {
	if headers == nil {
		headers = make(map[string]string, 2)
	}
	propagator.Inject(ctx, mapCarrier(headers))
	return headers
}

// remoteParentFromHeaders extracts a W3C traceparent (and tracestate) from the
// out-of-band transport headers on ctx (surfaced by the runtime via
// [babelqueue.HeadersFromContext]) and returns ctx carrying the resulting remote
// parent span context, plus true when a valid traceparent was found.
//
// This is the consumer half of true cross-hop parent-child linkage: a span started
// from the returned context is a child of the producer's span (remote parent),
// preserving per-hop span timing and real parent→child links — not just shared-trace
// correlation. When no traceparent header is present it returns (ctx, false) so the
// caller can fall back to the v0.1 trace_id-derived parent (ADR-0025 Option 1).
func remoteParentFromHeaders(ctx context.Context) (context.Context, bool) {
	headers := babelqueue.HeadersFromContext(ctx)
	if headers[HeaderTraceparent] == "" {
		return ctx, false
	}
	extracted := propagator.Extract(ctx, mapCarrier(headers))
	if !trace.SpanContextFromContext(extracted).IsValid() {
		return ctx, false
	}
	return extracted, true
}
