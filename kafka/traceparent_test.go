package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	testTraceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	testTracestate  = "vendor=value"
)

// PublishWithHeaders writes the unchanged envelope as the record value and carries traceparent /
// tracestate as record headers beside the bq- projection (ADR-0028, broker-bindings.md §6.3).
func TestPublishWithHeadersCarriesTraceContext(t *testing.T) {
	fw := &fakeWriter{}
	tr := newTransport(t, fw, &fakeReader{})
	body := sampleBody(t, 0)

	err := tr.PublishWithHeaders(context.Background(), "orders", body, map[string]string{
		"traceparent": testTraceparent,
		"tracestate":  testTracestate,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fw.sent) != 1 {
		t.Fatalf("sent %d records, want 1", len(fw.sent))
	}
	m := fw.sent[0]
	if string(m.Value) != body {
		t.Error("record value must be the unchanged envelope")
	}
	h := headerMap(m.Headers)
	if h["traceparent"] != testTraceparent || h["tracestate"] != testTracestate {
		t.Errorf("trace headers = %v", h)
	}
	if h["bq-job"] != urn || h["bq-attempts"] != "0" || h["bq-schema-version"] != "1" {
		t.Errorf("bq- projection lost: %v", h)
	}
	if strings.Contains(body, "traceparent") || strings.Contains(string(m.Value), "traceparent") {
		t.Error("traceparent must not appear in the envelope body")
	}

	// The same body through plain Publish has a byte-identical value.
	fw2 := &fakeWriter{}
	if err := newTransport(t, fw2, &fakeReader{}).Publish(context.Background(), "orders", body); err != nil {
		t.Fatal(err)
	}
	if string(fw2.sent[0].Value) != string(m.Value) {
		t.Error("PublishWithHeaders and Publish must write byte-identical values")
	}
}

// A bq- contract header (and bq-delay / bq-original-topic) wins a key collision; blank keys and
// values are skipped; nil/empty headers behave exactly like Publish.
func TestPublishWithHeadersContractHeadersWinAndBlanksAreSkipped(t *testing.T) {
	fw := &fakeWriter{}
	tr := newTransport(t, fw, &fakeReader{})
	body := sampleBody(t, 0)

	err := tr.PublishWithHeaders(context.Background(), "orders", body, map[string]string{
		"bq-job":            "urn:evil",
		"bq-attempts":       "99",
		"bq-schema-version": "2",
		"bq-delay":          "1",
		"bq-original-topic": "x",
		"":                  "no-key",
		"blank":             "",
		"traceparent":       testTraceparent,
		"bq-replay-bypass":  "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := fw.sent[0]
	h := headerMap(rec.Headers)
	if h["bq-job"] != urn || h["bq-attempts"] != "0" || h["bq-schema-version"] != "1" {
		t.Errorf("a contract header was clobbered: %v", h)
	}
	for _, k := range []string{"bq-delay", "bq-original-topic", "", "blank"} {
		if _, ok := h[k]; ok {
			t.Errorf("header %q must not be written: %v", k, h)
		}
	}
	if h["traceparent"] != testTraceparent || h["bq-replay-bypass"] != "1" {
		t.Errorf("out-of-band headers missing: %v", h)
	}
	for _, name := range []string{"bq-job", "bq-attempts", "bq-schema-version"} {
		n := 0
		for _, hh := range rec.Headers {
			if hh.Key == name {
				n++
			}
		}
		if n != 1 {
			t.Errorf("header %q written %d times", name, n)
		}
	}

	for _, hs := range []map[string]string{nil, {}} {
		fwa, fwb := &fakeWriter{}, &fakeWriter{}
		if err := newTransport(t, fwa, &fakeReader{}).PublishWithHeaders(context.Background(), "orders", body, hs); err != nil {
			t.Fatal(err)
		}
		if err := newTransport(t, fwb, &fakeReader{}).Publish(context.Background(), "orders", body); err != nil {
			t.Fatal(err)
		}
		if len(fwa.sent[0].Headers) != len(fwb.sent[0].Headers) {
			t.Errorf("empty headers must behave like Publish: %v vs %v", fwa.sent[0].Headers, fwb.sent[0].Headers)
		}
	}
}

// A body that does not decode carries no bq- projection but still takes the out-of-band headers.
func TestPublishWithHeadersOnUndecodableBody(t *testing.T) {
	fw := &fakeWriter{}
	tr := newTransport(t, fw, &fakeReader{})
	if err := tr.PublishWithHeaders(context.Background(), "orders", "not json", map[string]string{"traceparent": testTraceparent}); err != nil {
		t.Fatal(err)
	}
	h := headerMap(fw.sent[0].Headers)
	if len(h) != 1 || h["traceparent"] != testTraceparent {
		t.Errorf("headers = %v", h)
	}
}

// inject -> publish -> receive: the handler reads the same traceparent through HeadersFromContext,
// the received Headers map carries no bq- contract header, and the delivered body is unchanged.
func TestTraceparentRoundTripReachesHandler(t *testing.T) {
	fw := &fakeWriter{}
	fr := &fakeReader{}
	tr := newTransport(t, fw, fr)
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))

	var gotHeaders map[string]string
	var gotAttempts int
	app.Handle(urn, func(ctx context.Context, env babelqueue.Envelope) error {
		gotHeaders = babelqueue.HeadersFromContext(ctx)
		gotAttempts = env.Attempts
		return nil
	})

	if _, err := app.PublishWithHeaders(context.Background(), urn, map[string]any{"order_id": 1042},
		map[string]string{"traceparent": testTraceparent, "tracestate": testTracestate}); err != nil {
		t.Fatal(err)
	}
	if len(fw.sent) != 1 {
		t.Fatalf("sent %d records, want 1", len(fw.sent))
	}
	if strings.Contains(string(fw.sent[0].Value), "traceparent") {
		t.Error("traceparent leaked into the envelope body")
	}

	fr.messages = []kafkago.Message{fw.sent[0]}
	n, err := app.Drain(context.Background(), "orders", 1)
	if err != nil || n != 1 {
		t.Fatalf("drain = %d, %v", n, err)
	}
	if gotHeaders["traceparent"] != testTraceparent || gotHeaders["tracestate"] != testTracestate {
		t.Errorf("handler headers = %v", gotHeaders)
	}
	for k := range gotHeaders {
		switch k {
		case "bq-job", "bq-trace-id", "bq-message-id", "bq-schema-version", "bq-source-lang", "bq-attempts", "bq-delay", "bq-original-topic":
			t.Errorf("contract header %q leaked into the headers map", k)
		}
	}
	if gotAttempts != 0 {
		t.Errorf("attempts = %d, want 0", gotAttempts)
	}
	if len(fr.committed) != 1 {
		t.Errorf("record not committed: %d", len(fr.committed))
	}
}

// Pop surfaces the out-of-band headers (first duplicate wins), stays header-less when there are
// none, and also surfaces them on a version-gated delivery.
func TestPopSurfacesOutOfBandHeaders(t *testing.T) {
	body := sampleBody(t, 0)
	rec := kafkago.Message{Topic: "orders", Value: []byte(body), Headers: []kafkago.Header{
		{Key: "bq-job", Value: []byte(urn)},
		{Key: "bq-attempts", Value: []byte("0")},
		{Key: "traceparent", Value: []byte(testTraceparent)},
		{Key: "traceparent", Value: []byte("second")},
		{Key: "bq-replay-bypass", Value: []byte("1")},
		{Key: "empty", Value: nil},
	}}
	tr := newTransport(t, &fakeWriter{}, &fakeReader{messages: []kafkago.Message{rec}})
	msg, err := tr.Pop(context.Background(), "orders", time.Second)
	if err != nil || msg == nil {
		t.Fatalf("pop = %v, %v", msg, err)
	}
	if len(msg.Headers) != 2 || msg.Headers["traceparent"] != testTraceparent || msg.Headers["bq-replay-bypass"] != "1" {
		t.Errorf("headers = %v", msg.Headers)
	}

	plain := kafkago.Message{Topic: "orders", Value: []byte(body), Headers: []kafkago.Header{{Key: "bq-job", Value: []byte(urn)}}}
	tr2 := newTransport(t, &fakeWriter{}, &fakeReader{messages: []kafkago.Message{plain}})
	msg2, _ := tr2.Pop(context.Background(), "orders", time.Second)
	if msg2 == nil || msg2.Headers != nil {
		t.Errorf("a header-less record must surface nil Headers, got %v", msg2)
	}

	gated := kafkago.Message{Topic: "orders", Value: []byte(body), Headers: []kafkago.Header{
		{Key: "bq-schema-version", Value: []byte("2")},
		{Key: "traceparent", Value: []byte(testTraceparent)},
	}}
	tr3 := newTransport(t, &fakeWriter{}, &fakeReader{messages: []kafkago.Message{gated}})
	msg3, _ := tr3.Pop(context.Background(), "orders", time.Second)
	if msg3 == nil || msg3.Headers["traceparent"] != testTraceparent || len(msg3.Headers) != 1 {
		t.Errorf("gated delivery headers = %v", msg3)
	}
}

// The retry chain keeps the trace: PublishRetry, PublishRetryForAttempt and the App's retry
// re-publish (PublishFrom onto the record's own topic) forward traceparent / tracestate; a
// redrive-style write to another topic copies no header.
func TestRetryChainForwardsTraceHeaders(t *testing.T) {
	hdrs := []kafkago.Header{
		{Key: "traceparent", Value: []byte(testTraceparent)},
		{Key: "tracestate", Value: []byte(testTracestate)},
		{Key: "bq-replay-bypass", Value: []byte("1")},
	}
	handle := kafkago.Message{Topic: "orders", Key: []byte("k"), Headers: hdrs}
	from := func() *babelqueue.ReceivedMessage {
		return &babelqueue.ReceivedMessage{Body: sampleBody(t, 0), Queue: "orders", Handle: handle}
	}
	newRetryTransport := func(w *fakeWriter) *Transport {
		tr, err := New(WithWriter(w), WithReaderFactory(func(string) Reader { return &fakeReader{} }), WithRetryTiers(DefaultRetryTiers...))
		if err != nil {
			t.Fatal(err)
		}
		return tr
	}
	check := func(name string, rec kafkago.Message) {
		t.Helper()
		h := headerMap(rec.Headers)
		if h["traceparent"] != testTraceparent || h["tracestate"] != testTracestate {
			t.Errorf("%s: trace headers = %v", name, h)
		}
	}

	w := &fakeWriter{}
	tr := newRetryTransport(w)
	if err := tr.PublishRetry(context.Background(), from(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	check("PublishRetry", w.sent[0])

	w = &fakeWriter{}
	tr = newRetryTransport(w)
	if err := tr.PublishRetryForAttempt(context.Background(), from(), 0); err != nil {
		t.Fatal(err)
	}
	check("PublishRetryForAttempt", w.sent[0])

	// The App's retry re-publish: the body differs from the reserved one (attempts + 1), so it is not a restore.
	w = &fakeWriter{}
	tr = newRetryTransport(w)
	if err := tr.PublishFrom(context.Background(), "orders", sampleBody(t, 1), from()); err != nil {
		t.Fatal(err)
	}
	check("PublishFrom retry", w.sent[0])
	if string(w.sent[0].Key) != "k" {
		t.Errorf("key lost: %q", w.sent[0].Key)
	}
	if _, ok := headerMap(w.sent[0].Headers)["bq-replay-bypass"]; ok {
		t.Error("only the trace headers are forwarded on a retry")
	}

	w = &fakeWriter{}
	tr = newRetryTransport(w)
	dlqFrom := &babelqueue.ReceivedMessage{Body: sampleBody(t, 0), Queue: "orders.dlq", Handle: kafkago.Message{Topic: "orders.dlq", Headers: hdrs}}
	if err := tr.PublishFrom(context.Background(), "orders", dlqFrom.Body, dlqFrom); err != nil {
		t.Fatal(err)
	}
	if _, ok := headerMap(w.sent[0].Headers)["traceparent"]; ok {
		t.Error("a redrive to another topic must not copy the stale traceparent")
	}
}

// The re-inject consumer drops bq-delay / bq-original-topic and keeps every other header: a
// record published with PublishWithHeaders keeps its traceparent through the retry topic.
func TestPublishWithHeadersSurvivesReinject(t *testing.T) {
	rec := kafkago.Message{Topic: "orders.retry.1", Value: []byte(sampleBody(t, 0)), Headers: []kafkago.Header{
		{Key: "bq-job", Value: []byte(urn)},
		{Key: "bq-delay", Value: []byte("5000")},
		{Key: "bq-original-topic", Value: []byte("orders")},
		{Key: "traceparent", Value: []byte(testTraceparent)},
	}}
	out := reinjectRecord(rec)
	h := headerMap(out.Headers)
	if out.Topic != "orders" || h["traceparent"] != testTraceparent {
		t.Errorf("topic=%q headers=%v", out.Topic, h)
	}
	if _, ok := h["bq-delay"]; ok {
		t.Error("bq-delay must be dropped")
	}
}

// Redrive{Bypass: true} stamps bq-replay-bypass AND still carries the DLQ record's key
// byte-for-byte (§6.5): the header-stamping republish goes through PublishFromWithHeaders, not a
// keyless PublishWithHeaders. The §6 projection is re-derived from the reset body and the stale
// trace headers of the DLQ record are not copied.
func TestRedriveBypassKeepsRecordKeyAndStampsHeader(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{"keyed", []byte("customer-42")},
		{"null", nil},
		{"empty", []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := dlqRecord(t, tc.key)
			rec.Headers = []kafkago.Header{{Key: "traceparent", Value: []byte("stale")}}
			fw := &fakeWriter{}
			fr := &fakeReader{messages: []kafkago.Message{rec}}
			tr, err := New(WithWriter(fw), WithReaderFactory(func(string) Reader { return fr }))
			if err != nil {
				t.Fatal(err)
			}
			res, err := babelqueue.Redrive(context.Background(), tr, "orders.dlq",
				babelqueue.RedriveOptions{Bypass: true, Timeout: 20 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			if res.Redriven != 1 || len(res.Items) != 1 || !res.Items[0].Bypassed {
				t.Fatalf("want one bypassed redrive, got %+v", res)
			}
			if len(fw.sent) != 1 || fw.sent[0].Topic != "orders" {
				t.Fatalf("writes = %+v; want exactly one record on orders", fw.sent)
			}
			got := fw.sent[0]
			if (got.Key == nil) != (tc.key == nil) || string(got.Key) != string(tc.key) {
				t.Fatalf("key = %#v (nil=%v), want %#v (nil=%v)", got.Key, got.Key == nil, tc.key, tc.key == nil)
			}
			h := headerMap(got.Headers)
			if h["bq-replay-bypass"] != "1" {
				t.Errorf("bq-replay-bypass missing: %v", h)
			}
			if h["bq-job"] != urn || h["bq-attempts"] != "0" {
				t.Errorf("§6 projection not re-derived from the reset body: %v", h)
			}
			if _, ok := h["traceparent"]; ok {
				t.Errorf("the stale DLQ traceparent must not be copied: %v", h)
			}
			if len(fr.committed) != 1 {
				t.Errorf("DLQ record not committed after a successful redrive: %d", len(fr.committed))
			}
		})
	}
}

// PublishFromWithHeaders keeps the key and the retry trace forwarding of PublishFrom; a caller's
// own traceparent wins over the forwarded one; a from without a Kafka record yields a keyless record.
func TestPublishFromWithHeadersKeyAndPrecedence(t *testing.T) {
	fw := &fakeWriter{}
	tr := newTransport(t, fw, &fakeReader{})
	from := &babelqueue.ReceivedMessage{Body: sampleBody(t, 0), Queue: "orders", Handle: kafkago.Message{
		Topic: "orders", Key: []byte("k"),
		Headers: []kafkago.Header{{Key: "traceparent", Value: []byte("stale")}, {Key: "tracestate", Value: []byte(testTracestate)}},
	}}
	body := sampleBody(t, 1)

	if err := tr.PublishFromWithHeaders(context.Background(), "orders", body, from,
		map[string]string{"traceparent": testTraceparent, "bq-job": "urn:evil"}); err != nil {
		t.Fatal(err)
	}
	m := fw.sent[0]
	h := headerMap(m.Headers)
	if string(m.Key) != "k" || h["traceparent"] != testTraceparent || h["tracestate"] != testTracestate || h["bq-job"] != urn {
		t.Errorf("key=%q headers=%v", m.Key, h)
	}
	n := 0
	for _, hh := range m.Headers {
		if hh.Key == "traceparent" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("traceparent written %d times", n)
	}

	fw2 := &fakeWriter{}
	tr2 := newTransport(t, fw2, &fakeReader{})
	if err := tr2.PublishFromWithHeaders(context.Background(), "orders", body, &babelqueue.ReceivedMessage{Queue: "orders"},
		map[string]string{"traceparent": testTraceparent}); err != nil {
		t.Fatal(err)
	}
	if fw2.sent[0].Key != nil || headerMap(fw2.sent[0].Headers)["traceparent"] != testTraceparent {
		t.Errorf("foreign handle: %+v", fw2.sent[0])
	}
}
