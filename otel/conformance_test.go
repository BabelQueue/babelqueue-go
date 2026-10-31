package otel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

// ---------------------------------------------------------------------------------------------
// Conformance v1.9.0 `otel` sections: span-shape (ADR-0038), trace-id (ADR-0038 section 7) and
// metrics (ADR-0039). A missing section is a hard failure, never a skip: the suite and the runner
// move together. Every subtest is named after its manifest case.
// ---------------------------------------------------------------------------------------------

func loadSection(t *testing.T, name string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "testdata", "conformance", "manifest.json"))
	if err != nil {
		t.Fatalf("vendored conformance manifest: %v", err)
	}
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(raw, &sections); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	sec, ok := sections[name]
	if !ok {
		t.Fatalf("manifest has no %q section", name)
	}
	if err := json.Unmarshal(sec, into); err != nil {
		t.Fatalf("parse %q section: %v", name, err)
	}
}

// ---- span-shape ----

type spanCase struct {
	Name          string  `json:"name"`
	Mode          string  `json:"mode"`
	Operation     string  `json:"operation"`
	System        *string `json:"system"`
	Queue         *string `json:"queue"`
	URN           string  `json:"urn"`
	ConsumerGroup *string `json:"consumer_group"`
	TraceID       *string `json:"trace_id"`
	MessageID     *string `json:"message_id"`
	Attempts      *int    `json:"attempts"`
	SchemaVersion *int    `json:"schema_version"`
	EnvelopeBuilt *bool   `json:"envelope_built"`
	Outcome       string  `json:"outcome"`
	Expect        struct {
		SpanName           string         `json:"span_name"`
		SpanNameOneOf      []string       `json:"span_name_one_of"`
		SpanKind           string         `json:"span_kind"`
		Status             string         `json:"status"`
		ErrorType          string         `json:"error_type"`
		Attributes         map[string]any `json:"attributes"`
		OptionalAttributes []string       `json:"optional_attributes"`
	} `json:"expect"`
}

// captureTransport records the bodies handed to Publish and can fail them.
type captureTransport struct {
	fail   error
	bodies []string
}

func (c *captureTransport) Publish(_ context.Context, _ string, body string) error {
	c.bodies = append(c.bodies, body)
	return c.fail
}

func (c *captureTransport) PublishWithHeaders(ctx context.Context, q, body string, _ map[string]string) error {
	return c.Publish(ctx, q, body)
}

func (c *captureTransport) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	return nil, nil
}

func (c *captureTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error { return nil }

// spanTable is the ADR-0038 section 4 attribute key set the suite asserts.
var spanTable = map[string]bool{
	"messaging.system": true, "messaging.operation.type": true, "messaging.operation.name": true,
	"messaging.destination.name": true, "messaging.message.id": true, "messaging.message.conversation_id": true,
	"messaging.consumer.group.name": true, "babelqueue.urn": true, "babelqueue.schema_version": true,
	"babelqueue.attempts": true, "error.type": true, "messaging.operation": true, "messaging.babelqueue.attempts": true,
}

func TestSpanShapeConformance(t *testing.T) {
	var sec struct {
		Cases []spanCase `json:"cases"`
	}
	loadSection(t, "span-shape", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("span-shape section has no cases")
	}
	for _, c := range sec.Cases {
		t.Run(c.Name, func(t *testing.T) {
			if c.Mode != "span" {
				t.Fatalf("unsupported mode %q", c.Mode)
			}
			tracer, sr := recorder(t)
			var opts []Option
			if c.System != nil {
				opts = append(opts, WithSystem(*c.System))
			}
			var produced *babelqueue.Envelope
			var runErr error

			switch c.Operation {
			case "send":
				tr := &captureTransport{}
				built := c.EnvelopeBuilt == nil || *c.EnvelopeBuilt
				if c.Outcome == "error" && built {
					tr.fail = errors.New("transport rejected the publish")
				}
				data := map[string]any{"k": "v"}
				if !built {
					data = map[string]any{"unserialisable": make(chan int)} // Make accepts it, Encode rejects it
				}
				var callOpts []babelqueue.Option
				if c.Queue != nil {
					callOpts = append(callOpts, babelqueue.WithQueue(*c.Queue))
				}
				_, runErr = NewPublisher(tracer, babelqueue.NewApp(tr), opts...).Publish(context.Background(), c.URN, data, callOpts...)
				if len(tr.bodies) > 0 {
					env, err := babelqueue.Decode([]byte(tr.bodies[len(tr.bodies)-1]))
					if err != nil {
						t.Fatalf("the envelope handed to the transport does not decode: %v", err)
					}
					produced = &env
				}
				if !built && produced != nil {
					t.Fatal("an envelope reached the transport although the build was meant to fail")
				}
			case "process":
				if c.ConsumerGroup != nil {
					opts = append(opts, WithConsumerGroup(*c.ConsumerGroup))
				}
				// Build the delivered envelope directly: some cases omit fields the schema requires.
				env := babelqueue.Envelope{Job: c.URN, Data: map[string]any{}}
				if c.Queue != nil {
					env.Meta.Queue = *c.Queue
				}
				if c.TraceID != nil {
					env.TraceID = *c.TraceID
				}
				if c.MessageID != nil {
					env.Meta.ID = *c.MessageID
				}
				if c.Attempts != nil {
					env.Attempts = *c.Attempts
				}
				if c.SchemaVersion != nil {
					env.Meta.SchemaVersion = *c.SchemaVersion
				}
				handler := func(context.Context, babelqueue.Envelope) error {
					if c.Outcome == "error" {
						return errors.New("handler failed")
					}
					return nil
				}
				runErr = WrapHandler(tracer, handler, opts...)(context.Background(), env)
			default:
				t.Fatalf("unknown operation %q", c.Operation)
			}

			if (c.Outcome == "error") != (runErr != nil) {
				t.Fatalf("outcome %q but err = %v", c.Outcome, runErr)
			}
			spans := sr.Ended()
			if len(spans) != 1 {
				t.Fatalf("want 1 span, got %d", len(spans))
			}
			assertSpanShape(t, c, spans[0], produced)
		})
	}
}

func assertSpanShape(t *testing.T, c spanCase, s sdktrace.ReadOnlySpan, produced *babelqueue.Envelope) {
	t.Helper()
	// Placeholders: the runner reads the produced envelope (the one handed to the transport).
	placeholder := map[string]string{}
	producedQueue := ""
	if produced != nil {
		placeholder["$produced.meta.id"] = produced.Meta.ID
		placeholder["$produced.trace_id"] = produced.TraceID
		placeholder["$produced.meta.queue"] = produced.Meta.Queue
		producedQueue = produced.Meta.Queue
	}
	blankProducedQueue := c.Operation == "send" && isBlank(producedQueue)

	wantName := c.Expect.SpanName
	if strings.Contains(wantName, "$produced.meta.queue") {
		if blankProducedQueue {
			wantName = "send"
		} else {
			wantName = strings.ReplaceAll(wantName, "$produced.meta.queue", producedQueue)
		}
	}

	attrs := attrMap(s.Attributes())
	got := func(k string) (string, bool) {
		v, ok := attrs[k]
		return v.Emit(), ok
	}

	if len(c.Expect.SpanNameOneOf) > 0 {
		ok := false
		for _, n := range c.Expect.SpanNameOneOf {
			ok = ok || s.Name() == n
		}
		if !ok {
			t.Errorf("span name %q not in %v", s.Name(), c.Expect.SpanNameOneOf)
		}
	} else if s.Name() != wantName {
		t.Errorf("span name = %q, want %q", s.Name(), wantName)
	}
	if strings.Contains(s.Name(), c.URN) {
		t.Errorf("the URN is in the span name %q", s.Name())
	}
	wantKind := map[string]trace.SpanKind{"PRODUCER": trace.SpanKindProducer, "CONSUMER": trace.SpanKindConsumer}[c.Expect.SpanKind]
	if s.SpanKind() != wantKind {
		t.Errorf("span kind = %v, want %s", s.SpanKind(), c.Expect.SpanKind)
	}
	wantStatus := codes.Unset
	if c.Expect.Status == "error" {
		wantStatus = codes.Error
	}
	if s.Status().Code != wantStatus {
		t.Errorf("status = %v, want %s", s.Status().Code, c.Expect.Status)
	}

	expected := map[string]any{}
	for k, v := range c.Expect.Attributes {
		if str, ok := v.(string); ok {
			if rep, isPlaceholder := placeholder[str]; isPlaceholder {
				if str == "$produced.meta.queue" && blankProducedQueue {
					continue // a blank produced queue writes no destination
				}
				v = rep
			} else if strings.HasPrefix(str, "$produced.") {
				t.Fatalf("no produced envelope for placeholder %s", str)
			}
		}
		expected[k] = v
	}
	optional := map[string]bool{}
	for _, k := range c.Expect.OptionalAttributes {
		optional[k] = true
	}

	for k, want := range expected {
		v, ok := attrs[k]
		if !ok {
			t.Errorf("attribute %q missing", k)
			continue
		}
		switch w := want.(type) {
		case string:
			if v.Type() != attribute.STRING || v.AsString() != w {
				t.Errorf("attribute %q = %q, want %q", k, v.Emit(), w)
			}
		case float64: // JSON integers
			if v.Type() != attribute.INT64 || v.AsInt64() != int64(w) {
				t.Errorf("attribute %q = %s, want %v", k, v.Emit(), w)
			}
		default:
			t.Fatalf("unsupported expectation type %T for %q", want, k)
		}
	}
	for k := range attrs {
		_, isExpected := expected[k]
		switch {
		case k == "error.type":
			// asserted below
		case strings.HasPrefix(k, "babelqueue.") && !spanTable[k]:
			t.Errorf("forbidden babelqueue.* attribute %q", k)
		case spanTable[k] && !isExpected && !optional[k]:
			t.Errorf("table attribute %q must be absent (got %q)", k, attrs[k].Emit())
		}
	}
	// optional attributes: when present they equal what the input implies and the name agrees.
	if v, ok := got("messaging.destination.name"); ok && optional["messaging.destination.name"] {
		if c.Queue == nil || v != *c.Queue {
			t.Errorf("optional destination %q != the queue given to the producer", v)
		}
		if s.Name() != "send "+v {
			t.Errorf("span name %q disagrees with destination %q", s.Name(), v)
		}
	}
	if optional["messaging.destination.name"] {
		if _, ok := got("messaging.destination.name"); !ok && s.Name() != "send" {
			t.Errorf("no destination but span name %q, want the bare send", s.Name())
		}
	}
	if c.Expect.ErrorType == "present" {
		if v, ok := attrs["error.type"]; !ok || v.Type() != attribute.STRING || v.AsString() == "" {
			t.Errorf("error.type = %v (present %v), want a non-empty string", v.Emit(), ok)
		}
	} else if _, ok := attrs["error.type"]; ok {
		t.Error("error.type must be absent on success")
	}
	if c.Outcome == "error" && len(s.Events()) == 0 {
		t.Error("a failed span must record the error event")
	}
}

// ---- trace-id ----

func TestTraceIDConformance(t *testing.T) {
	var sec struct {
		Cases []struct {
			Name    string `json:"name"`
			Mode    string `json:"mode"`
			URN     string `json:"urn"`
			TraceID string `json:"trace_id"`
			Expect  struct {
				TraceID    string `json:"trace_id"`
				TraceIDHex string `json:"trace_id_hex"`
				Mapping    string `json:"mapping"`
				Decoded    bool   `json:"decoded"`
			} `json:"expect"`
		} `json:"cases"`
	}
	loadSection(t, "trace-id", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("trace-id section has no cases")
	}
	for _, c := range sec.Cases {
		t.Run(c.Name, func(t *testing.T) {
			switch c.Mode {
			case "map":
				got, hashed := mapTraceID(c.TraceID)
				if got.String() != c.Expect.TraceIDHex {
					t.Errorf("TraceIDOf(%q) = %s, want %s", c.TraceID, got, c.Expect.TraceIDHex)
				}
				if TraceIDOf(c.TraceID) != got {
					t.Error("TraceIDOf disagrees with the mapping")
				}
				if want := c.Expect.Mapping == "hash"; hashed != want {
					t.Errorf("hashed = %v, want mapping %q", hashed, c.Expect.Mapping)
				}
				if c.Expect.Mapping == "hash" {
					sum := sha256.Sum256([]byte(c.TraceID))
					if hex.EncodeToString(sum[:16]) != c.Expect.TraceIDHex {
						t.Errorf("hash mapping is not SHA-256 first 16 bytes of the UTF-8 value")
					}
				}
			case "decode", "reemit":
				env := encodeWithTraceID(t, c.TraceID)
				dec, err := babelqueue.Decode(env)
				if err != nil {
					t.Fatalf("decode rejected a non-UUID trace_id: %v", err)
				}
				if dec.TraceID != c.TraceID {
					t.Fatalf("decoded trace_id = %q, want %q byte for byte", dec.TraceID, c.TraceID)
				}
				if c.Mode == "reemit" {
					dec.Attempts++
					again, err := dec.Encode()
					if err != nil {
						t.Fatal(err)
					}
					back, err := babelqueue.Decode(again)
					if err != nil || back.TraceID != c.Expect.TraceID {
						t.Fatalf("re-emitted trace_id = %q (err %v), want %q", back.TraceID, err, c.Expect.TraceID)
					}
				}
			case "downstream", "make":
				urn := c.URN
				if urn == "" {
					urn = "urn:acme:downstream"
				}
				env, err := babelqueue.Make(urn, nil, babelqueue.WithTraceID(c.TraceID))
				if err != nil {
					t.Fatalf("make rejected the trace_id: %v", err)
				}
				if env.TraceID != c.Expect.TraceID {
					t.Errorf("trace_id = %q, want %q", env.TraceID, c.Expect.TraceID)
				}
				if c.Expect.TraceIDHex != "" && TraceIDOf(env.TraceID).String() != c.Expect.TraceIDHex {
					t.Errorf("TraceIDOf(stored) = %s, want %s", TraceIDOf(env.TraceID), c.Expect.TraceIDHex)
				}
			default:
				t.Fatalf("unsupported mode %q", c.Mode)
			}
		})
	}
}

// encodeWithTraceID encodes a valid envelope and sets its trace_id verbatim (Make would trim).
func encodeWithTraceID(t *testing.T, traceID string) []byte {
	t.Helper()
	env, err := babelqueue.Make("urn:acme:trace", map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	env.TraceID = traceID
	body, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// ---- metrics ----

type metricsCase struct {
	Name          string  `json:"name"`
	Mode          string  `json:"mode"`
	System        *string `json:"system"`
	Queue         *string `json:"queue"`
	Attempts      int     `json:"attempts"`
	Outcome       string  `json:"outcome"`
	Meter         *bool   `json:"meter"`
	Trigger       string  `json:"trigger"`
	ObserverCalls bool    `json:"observer_called"`
	SDKTest       bool    `json:"sdk_test_required"`
	Event         *struct {
		Queue           string `json:"queue"`
		DeadLetterQueue string `json:"dead_letter_queue"`
		Reason          string `json:"reason"`
	} `json:"event"`
	Expect metricsExpect `json:"expect"`
}

type metricsExpect struct {
	Metric     string `json:"metric"`
	Instrument string `json:"instrument"`
	Unit       string `json:"unit"`
	Operation  string `json:"operation_name"`
	Attributes struct {
		Always               []string `json:"always"`
		WhenDestinationKnown []string `json:"when_destination_known"`
		OnFailure            []string `json:"on_failure"`
	} `json:"attributes"`
	Forbidden []string              `json:"forbidden_attributes"`
	Advisory  []float64             `json:"advisory_bucket_boundaries"`
	Records   []metricsRecordExpect `json:"records"`
}

type metricsRecordExpect struct {
	Metric     string            `json:"metric"`
	Kind       string            `json:"kind"`
	Value      any               `json:"value"`
	Attributes map[string]string `json:"attributes"`
	ErrorType  string            `json:"error_type"`
}

func TestMetricsConformance(t *testing.T) {
	var sec struct {
		Cases []metricsCase `json:"cases"`
	}
	loadSection(t, "metrics", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("metrics section has no cases")
	}
	for _, c := range sec.Cases {
		t.Run(c.Name, func(t *testing.T) {
			switch c.Mode {
			case "instrument":
				runInstrumentCase(t, c)
			case "publish", "consume":
				runScenarioCase(t, c)
			case "dead_letter":
				runDeadLetterCase(t, c)
			default:
				t.Fatalf("unsupported mode %q", c.Mode)
			}
		})
	}
}

// scenarioRecords drives one publish or consume scenario with a fresh in-memory reader.
func scenarioRecords(t *testing.T, mode string, c metricsCase, queue *string, outcome string) []metricRecord {
	recs, _ := scenarioRun(t, mode, c, queue, outcome)
	return recs
}

// scenarioRun is scenarioRecords that also returns the meta.queue of the envelope handed to the
// transport (publish only; "" when none was), read from the body the transport received.
func scenarioRun(t *testing.T, mode string, c metricsCase, queue *string, outcome string) ([]metricRecord, string) {
	t.Helper()
	meter, reader := newMeter(t)
	opts := []Option{}
	if c.Meter == nil || *c.Meter {
		opts = append(opts, WithMeter(meter))
	}
	if c.System != nil {
		opts = append(opts, WithSystem(*c.System))
	}
	tracer, _ := recorder(t)
	producedQueue := ""
	switch mode {
	case "publish":
		tr := &captureTransport{}
		if outcome == "error" {
			tr.fail = errors.New("transport rejected the publish")
		}
		var callOpts []babelqueue.Option
		if queue != nil {
			callOpts = append(callOpts, babelqueue.WithQueue(*queue))
		}
		_, err := NewPublisher(tracer, babelqueue.NewApp(tr), opts...).Publish(context.Background(), "urn:acme:order.created", map[string]any{"k": "v"}, callOpts...)
		if (outcome == "error") != (err != nil) {
			t.Fatalf("outcome %q but err = %v", outcome, err)
		}
		if len(tr.bodies) > 0 {
			env, derr := babelqueue.Decode([]byte(tr.bodies[len(tr.bodies)-1]))
			if derr != nil {
				t.Fatalf("the envelope handed to the transport does not decode: %v", derr)
			}
			producedQueue = env.Meta.Queue
		}
	case "consume":
		env := babelqueue.Envelope{Job: "urn:acme:order.created", TraceID: "t", Data: map[string]any{}, Attempts: c.Attempts}
		env.Meta.ID, env.Meta.SchemaVersion = "m-1", 1
		if queue != nil {
			env.Meta.Queue = *queue
		}
		handler := func(context.Context, babelqueue.Envelope) error {
			if outcome == "error" {
				return errors.New("handler failed")
			}
			return nil
		}
		err := WrapHandler(tracer, handler, opts...)(context.Background(), env)
		if (outcome == "error") != (err != nil) {
			t.Fatalf("outcome %q but err = %v", outcome, err)
		}
	}
	return flatten(t, collect(t, reader)), producedQueue
}

func runScenarioCase(t *testing.T, c metricsCase) {
	// $produced.meta.queue is read from the envelope handed to the transport (Faz 02 rule).
	recs, producedQueue := scenarioRun(t, c.Mode, c, c.Queue, c.Outcome)
	assertRecords(t, c.Expect.Records, recs, producedQueue)
}

// assertRecords compares the recorded data points with the expected ones as a multiset; nothing
// else may be recorded. error_type "present" demands a non-empty error.type on the data point
// (its value is language-specific and the expected attributes omit it).
func assertRecords(t *testing.T, expected []metricsRecordExpect, recs []metricRecord, producedQueue string) {
	t.Helper()
	var want []string
	wantErr := map[string]bool{}
	for _, r := range expected {
		attrs := map[string]string{}
		for k, v := range r.Attributes {
			if v == "$produced.meta.queue" {
				if isBlank(producedQueue) {
					continue
				}
				v = producedQueue
			}
			attrs[k] = v
		}
		k := metricRecord{r.Metric, r.Kind, 0, attrs}.key()
		want = append(want, k)
		if r.ErrorType == "present" {
			wantErr[k] = true
		}
		if r.Kind == "add" {
			if v, _ := r.Value.(float64); v != 1 {
				t.Fatalf("unsupported add value %v", r.Value)
			}
		}
	}

	var got []string
	for _, r := range recs {
		attrs := map[string]string{}
		hasErr := false
		for k, v := range r.attrs {
			if k == "error.type" {
				hasErr = v != ""
				continue
			}
			attrs[k] = v
		}
		k := metricRecord{r.metric, r.kind, 0, attrs}.key()
		if hasErr != wantErr[k] {
			t.Errorf("record %s: error.type present = %v, want %v", k, hasErr, wantErr[k])
		}
		if r.kind == "record" && !(r.value >= 0) {
			t.Errorf("record %s: value %v is not a non-negative number of seconds", k, r.value)
		}
		got = append(got, k)
	}
	sort.Strings(want)
	sort.Strings(got)
	if strings.Join(want, "\n") != strings.Join(got, "\n") {
		t.Errorf("recorded data points differ.\nwant:\n%s\ngot:\n%s", strings.Join(want, "\n"), strings.Join(got, "\n"))
	}
}

// dlScript hands the App scripted messages and records plain publishes.
type dlScript struct {
	msgs      []*babelqueue.ReceivedMessage
	published []string
	failTo    string
}

func (d *dlScript) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	if len(d.msgs) == 0 {
		return nil, nil
	}
	m := d.msgs[0]
	d.msgs = d.msgs[1:]
	return m, nil
}

func (d *dlScript) Publish(_ context.Context, queue, _ string) error {
	if d.failTo != "" && queue == d.failTo {
		return errors.New("dlq publish failed")
	}
	d.published = append(d.published, queue)
	return nil
}

func (d *dlScript) Ack(context.Context, *babelqueue.ReceivedMessage) error { return nil }

// dlNoop has only NativeSettler: the SQS shape, a no-op left to RedrivePolicy.
type dlNoop struct{ *dlScript }

func (dlNoop) SettleNative(context.Context, *babelqueue.ReceivedMessage, string, string) error {
	return nil
}

// dlNativeFails has both native settles; the dead-letter call fails.
type dlNativeFails struct{ *dlScript }

func (dlNativeFails) SettleNative(context.Context, *babelqueue.ReceivedMessage, string, string) error {
	return errors.New("native dead-letter failed")
}

func (dlNativeFails) SettleNativeDeadLetter(context.Context, *babelqueue.ReceivedMessage, string, string) (bool, error) {
	return false, errors.New("native dead-letter failed")
}

// driveNoCountTrigger drives a real App, with the otel observer wired in, to a situation in which
// the SDK itself dead-lettered nothing. The conformance case is observer level, so a runner can only
// pass these trivially; this makes the rule an assertion on the App (ADR-0039 section 5.1).
func driveNoCountTrigger(t *testing.T, trigger string, observer babelqueue.DeadLetterObserver) {
	t.Helper()
	body := func() string {
		env, err := babelqueue.Make("urn:acme:order.created", map[string]any{"k": "v"}, babelqueue.WithQueue("orders"))
		if err != nil {
			t.Fatal(err)
		}
		b, err := env.Encode()
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	failing := func(context.Context, babelqueue.Envelope) error { return errors.New("boom") }
	gated := func() *babelqueue.ReceivedMessage {
		return &babelqueue.ReceivedMessage{Body: "{}", Queue: "orders", RouteURN: "babelqueue:unsupported-schema-version:2"}
	}
	run := func(tr babelqueue.Transport, handle bool, opts ...babelqueue.AppOption) {
		opts = append([]babelqueue.AppOption{babelqueue.WithDeadLetterObserver(observer), babelqueue.WithRewindPause(0)}, opts...)
		app := babelqueue.NewApp(tr, opts...)
		if handle {
			app.Handle("urn:acme:order.created", failing)
		}
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
	}
	msg := func() *babelqueue.ReceivedMessage { return &babelqueue.ReceivedMessage{Body: body(), Queue: "orders"} }

	switch trigger {
	case "bq_dlq_write_failed": // the write to the BabelQueue DLQ fails
		run(&dlScript{msgs: []*babelqueue.ReceivedMessage{msg()}, failTo: "orders.dlq"}, true,
			babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true))
	case "native_dead_letter_failed": // the explicit native dead-letter call fails
		run(dlNativeFails{&dlScript{msgs: []*babelqueue.ReceivedMessage{gated()}}}, false)
	case "native_noop": // the SQS no-op native settle
		run(dlNoop{&dlScript{msgs: []*babelqueue.ReceivedMessage{gated()}}}, false)
	case "broker_autonomous": // a failing delivery below the retry limit: the broker may dead-letter it later, the SDK does not
		run(&dlScript{msgs: []*babelqueue.ReceivedMessage{msg()}}, true, babelqueue.WithMaxAttempts(3), babelqueue.WithDeadLetter(true))
	case "dlq_disabled_dropped": // dead-lettering off: the exhausted message is dropped
		run(&dlScript{msgs: []*babelqueue.ReceivedMessage{msg()}}, true, babelqueue.WithMaxAttempts(1))
	default:
		t.Fatalf("unknown no-count trigger %q", trigger)
	}
}

func runDeadLetterCase(t *testing.T, c metricsCase) {
	meter, reader := newMeter(t)
	opts := []Option{WithMeter(meter)}
	if c.System != nil {
		opts = append(opts, WithSystem(*c.System))
	}
	observer := NewDeadLetterObserver(opts...)

	if c.ObserverCalls {
		if c.Event == nil {
			t.Fatal("observer_called case without an event")
		}
		observer(context.Background(), babelqueue.DeadLetterEvent{
			Queue: c.Event.Queue, DeadLetterQueue: c.Event.DeadLetterQueue, Reason: c.Event.Reason,
		})
	} else {
		if !c.SDKTest {
			t.Fatal("an observer_called: false case must carry sdk_test_required")
		}
		driveNoCountTrigger(t, c.Trigger, observer)
	}
	assertRecords(t, c.Expect.Records, flatten(t, collect(t, reader)), "")
}

func runInstrumentCase(t *testing.T, c metricsCase) {
	e := c.Expect
	for _, scenario := range []struct {
		name    string
		queue   *string
		outcome string
		keys    []string
	}{
		{"destination known, success", strPtr("orders"), "ok", append(append([]string{}, e.Attributes.Always...), e.Attributes.WhenDestinationKnown...)},
		{"no destination, success", strPtr(""), "ok", e.Attributes.Always},
		{"destination known, failure", strPtr("orders"), "error", append(append(append([]string{}, e.Attributes.Always...), e.Attributes.WhenDestinationKnown...), e.Attributes.OnFailure...)},
	} {
		// "when failed" keys only exist on a metric that records failures; a metric without
		// on_failure never carries error.type, whatever the outcome.
		switch e.Metric {
		case MetricSentMessages:
			sc := metricsCase{Mode: "publish", System: strPtr("kafka")}
			checkInstrument(t, e, scenarioRecords(t, "publish", sc, scenario.queue, scenario.outcome), scenario.keys, scenario.name)
		case MetricProcessDuration, MetricConsumedMessages, MetricRetries:
			sc := metricsCase{Mode: "consume", System: strPtr("kafka"), Attempts: 1}
			checkInstrument(t, e, scenarioRecords(t, "consume", sc, scenario.queue, scenario.outcome), scenario.keys, scenario.name)
		case MetricDeadLetters:
			meter, reader := newMeter(t)
			NewDeadLetterObserver(WithSystem("kafka"), WithMeter(meter))(context.Background(),
				babelqueue.DeadLetterEvent{Queue: derefOr(scenario.queue, ""), DeadLetterQueue: "orders.dlq", Reason: "failed"})
			checkInstrument(t, e, flatten(t, collect(t, reader)), scenario.keys, scenario.name)
		default:
			t.Fatalf("unknown metric %q", e.Metric)
		}
	}
	// Name, kind and unit, from one collection where the metric is recorded.
	meter, reader := newMeter(t)
	tracer, _ := recorder(t)
	switch e.Metric {
	case MetricSentMessages:
		_, _ = NewPublisher(tracer, babelqueue.NewApp(&captureTransport{}), WithMeter(meter)).Publish(context.Background(), "urn:acme:a", nil)
	case MetricDeadLetters:
		NewDeadLetterObserver(WithMeter(meter))(context.Background(), babelqueue.DeadLetterEvent{Queue: "orders"})
	default:
		env := babelqueue.Envelope{Job: "urn:acme:a", Attempts: 1, Meta: babelqueue.Meta{Queue: "orders"}}
		_ = WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil }, WithMeter(meter))(context.Background(), env)
	}
	m, ok := findMetric(collect(t, reader), e.Metric)
	if !ok {
		t.Fatalf("metric %s was not recorded", e.Metric)
	}
	if m.Unit != e.Unit {
		t.Errorf("unit = %q, want %q", m.Unit, e.Unit)
	}
	switch d := m.Data.(type) {
	case metricdata.Histogram[float64]:
		if e.Instrument != "histogram" {
			t.Errorf("kind = histogram, want %s", e.Instrument)
		}
		if len(e.Advisory) > 0 {
			if got := d.DataPoints[0].Bounds; fmt.Sprint(got) != fmt.Sprint(e.Advisory) {
				t.Errorf("bucket boundaries = %v, want the advisory %v", got, e.Advisory)
			}
		}
	case metricdata.Sum[int64]:
		if e.Instrument != "counter" || !d.IsMonotonic {
			t.Errorf("kind = counter (monotonic %v), want %s", d.IsMonotonic, e.Instrument)
		}
	default:
		t.Errorf("unexpected data %T", m.Data)
	}
}

func checkInstrument(t *testing.T, e metricsExpect, recs []metricRecord, wantKeys []string, scenario string) {
	t.Helper()
	found := false
	for _, r := range recs {
		if r.metric != e.Metric {
			continue
		}
		found = true
		got := make([]string, 0, len(r.attrs))
		for k := range r.attrs {
			got = append(got, k)
		}
		want := append([]string{}, wantKeys...)
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: attribute keys = %v, want exactly %v", scenario, got, want)
		}
		if op := r.attrs["messaging.operation.name"]; op != e.Operation {
			t.Errorf("%s: messaging.operation.name = %q, want %q", scenario, op, e.Operation)
		}
		for _, f := range e.Forbidden {
			if _, bad := r.attrs[f]; bad {
				t.Errorf("%s: forbidden attribute %q present", scenario, f)
			}
		}
	}
	if !found {
		t.Errorf("%s: metric %s not recorded", scenario, e.Metric)
	}
}

func strPtr(s string) *string { return &s }

func derefOr(p *string, def string) string {
	if p == nil {
		return def
	}
	return *p
}
