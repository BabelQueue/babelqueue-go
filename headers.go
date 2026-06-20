package babelqueue

import "context"

// headersKey is the context key under which the runtime stashes a delivered
// message's out-of-band transport headers (see [ReceivedMessage.Headers]).
type headersKey struct{}

// withHeaders returns ctx carrying the delivered message's out-of-band transport
// headers, so a handler (or a wrapper such as the optional otel module) can read
// metadata that rides beside the frozen envelope — e.g. a W3C traceparent for
// cross-hop span parent-child linkage (ADR-0028). The envelope is untouched
// (GR-1); this is the same out-of-band header seam as the replay-bypass marker
// (ADR-0027). A nil or empty map is fine; reads are nil-safe.
func withHeaders(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return context.WithValue(ctx, headersKey{}, headers)
}

// HeadersFromContext returns the out-of-band transport headers that arrived with
// the message currently being handled, or nil when none were carried (or the
// transport does not surface headers). The returned map is read-only — do not
// mutate it. It is the consume-side counterpart of [HeaderPublisher]: a handler
// or an optional wrapper (e.g. the otel module's WrapHandler) reads per-message
// metadata that travels beside the frozen envelope, never in it (GR-1).
func HeadersFromContext(ctx context.Context) map[string]string {
	h, _ := ctx.Value(headersKey{}).(map[string]string)
	return h
}
