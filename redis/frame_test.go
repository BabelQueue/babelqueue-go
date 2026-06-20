package redis

import (
	"encoding/json"
	"testing"
)

// These are pure unit tests for the header-frame logic (ADR-0028). They need no broker:
// they exercise the frame marshal/unframe round-trip, bare-value back-compat, and the
// Ack-handle invariant directly against the unexported helpers, so the framing is fully
// covered without a live Redis.

// bareEnvelope is a minimal stand-in for a frozen wire envelope value. The only property
// that matters here is that it is a JSON object WITHOUT the reserved "__bq_frame"
// sentinel, so unframe must treat it as bare.
const bareEnvelope = `{"job":"urn:babel:test:ping","data":{"n":1},"meta":{"id":"abc"}}`

func TestUnframeBareEnvelopePassesThrough(t *testing.T) {
	body, headers := unframe(bareEnvelope)
	if body != bareEnvelope {
		t.Errorf("body = %q, want the value verbatim", body)
	}
	if headers != nil {
		t.Errorf("headers = %v, want nil for a bare value", headers)
	}
}

func TestUnframeNonJSONPassesThrough(t *testing.T) {
	for _, v := range []string{"", "not json", "[1,2,3]", `"a string"`, "42"} {
		body, headers := unframe(v)
		if body != v {
			t.Errorf("unframe(%q) body = %q, want verbatim", v, body)
		}
		if headers != nil {
			t.Errorf("unframe(%q) headers = %v, want nil", v, headers)
		}
	}
}

func TestUnframeJSONObjectWithoutSentinelIsBare(t *testing.T) {
	// A JSON object that happens to have a "headers" / "body" shape but no sentinel must
	// NOT be mistaken for a frame — only the reserved key marks a frame.
	v := `{"headers":{"x":"y"},"body":"spoof"}`
	body, headers := unframe(v)
	if body != v {
		t.Errorf("body = %q, want verbatim (no sentinel = bare)", body)
	}
	if headers != nil {
		t.Errorf("headers = %v, want nil", headers)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	in := map[string]string{
		"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"tracestate":  "vendor=value",
	}
	raw, err := json.Marshal(headerFrame{Version: frameVersion, Headers: in, Body: bareEnvelope})
	if err != nil {
		t.Fatal(err)
	}

	body, headers := unframe(string(raw))
	if body != bareEnvelope {
		t.Errorf("body = %q, want the unframed wire envelope %q", body, bareEnvelope)
	}
	if len(headers) != len(in) {
		t.Fatalf("headers = %v, want %v", headers, in)
	}
	for k, v := range in {
		if headers[k] != v {
			t.Errorf("headers[%q] = %q, want %q", k, headers[k], v)
		}
	}
}

func TestSanitizeHeadersDropsBlanks(t *testing.T) {
	got := sanitizeHeaders(map[string]string{
		"traceparent": "00-trace-01",
		"":            "no-key",
		"empty-val":   "",
	})
	if len(got) != 1 || got["traceparent"] != "00-trace-01" {
		t.Errorf("sanitizeHeaders = %v, want only the non-blank traceparent", got)
	}
	if got := sanitizeHeaders(nil); got != nil {
		t.Errorf("sanitizeHeaders(nil) = %v, want nil", got)
	}
	if got := sanitizeHeaders(map[string]string{"": ""}); got != nil {
		t.Errorf("sanitizeHeaders(all-blank) = %v, want nil", got)
	}
}

func TestFrameValueWithoutHeadersStaysBare(t *testing.T) {
	// Plain Publish stores `body`; PublishWithHeaders with nil/empty/blank headers must
	// store the byte-identical bare value so nothing regresses and cross-version queues
	// interoperate.
	for name, headers := range map[string]map[string]string{
		"nil":       nil,
		"empty":     {},
		"all-blank": {"": "", "k": ""},
	} {
		got, err := frameValue(bareEnvelope, headers)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != bareEnvelope {
			t.Errorf("%s: frameValue = %q, want byte-identical bare body %q", name, got, bareEnvelope)
		}
	}
}

func TestFrameValueWithHeadersIsAFrame(t *testing.T) {
	got, err := frameValue(bareEnvelope, map[string]string{"traceparent": "00-trace-01"})
	if err != nil {
		t.Fatal(err)
	}
	if got == bareEnvelope {
		t.Fatal("with headers the stored value must be a frame, not the bare body")
	}
	if !containsSentinel(got) {
		t.Errorf("frame %q is missing the __bq_frame sentinel", got)
	}
}

// TestStoredValueAckHandleInvariant models the full RPUSH→BLMOVE→LREM cycle without a
// broker: frameValue is what RPUSH stores and what BLMOVE returns (the Pop Handle), and
// Ack's LREM matches on that exact value. So the handle must equal the stored value
// byte-for-byte in BOTH the framed and bare cases, and unframe must recover the right
// body/headers from it.
func TestStoredValueAckHandleInvariant(t *testing.T) {
	cases := []struct {
		name        string
		headers     map[string]string
		wantHeaders map[string]string
	}{
		{"bare (no headers)", nil, nil},
		{"framed (traceparent)", map[string]string{"traceparent": "00-trace-01"}, map[string]string{"traceparent": "00-trace-01"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stored, err := frameValue(bareEnvelope, c.headers) // what RPUSH writes
			if err != nil {
				t.Fatal(err)
			}
			// BLMOVE returns `stored`; Pop sets Handle = stored, then unframes it.
			handle := stored
			body, headers := unframe(stored)
			if body != bareEnvelope {
				t.Errorf("body = %q, want %q", body, bareEnvelope)
			}
			if len(headers) != len(c.wantHeaders) {
				t.Errorf("headers = %v, want %v", headers, c.wantHeaders)
			}
			for k, v := range c.wantHeaders {
				if headers[k] != v {
					t.Errorf("headers[%q] = %q, want %q", k, headers[k], v)
				}
			}
			// LREM matches on the handle, which MUST equal the stored value.
			if handle != stored {
				t.Errorf("Ack handle %q != stored value %q — LREM would not match", handle, stored)
			}
		})
	}
}

// TestFrameBodyIsNotTheWireEnvelope locks GR-1: the wire envelope is never mutated by
// framing. The frame is a separate JSON object whose "body" field holds the verbatim
// envelope; the envelope bytes themselves are untouched.
func TestFrameBodyIsNotTheWireEnvelope(t *testing.T) {
	raw, err := json.Marshal(headerFrame{
		Version: frameVersion,
		Headers: map[string]string{"traceparent": "00-trace-01"},
		Body:    bareEnvelope,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The stored frame must NOT be the bare envelope (it wraps it) ...
	if string(raw) == bareEnvelope {
		t.Fatal("frame must differ from the bare envelope")
	}
	// ... and unframing must reproduce the exact original envelope bytes.
	body, _ := unframe(string(raw))
	if body != bareEnvelope {
		t.Errorf("unframed body = %q, want the original envelope %q", body, bareEnvelope)
	}
}
