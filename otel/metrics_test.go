package otel

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// metricRecord is one recorded metric data point, flattened: a counter data point of value n
// becomes n records of 1; a histogram data point of count n becomes n records.
type metricRecord struct {
	metric string
	kind   string // "add" | "record"
	value  float64
	attrs  map[string]string
}

func (r metricRecord) key() string {
	keys := make([]string, 0, len(r.attrs))
	for k := range r.attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(r.metric + "|" + r.kind + "|")
	for _, k := range keys {
		b.WriteString(k + "=" + r.attrs[k] + ";")
	}
	return b.String()
}

func newMeter(t *testing.T) (metric.Meter, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return mp.Meter("test", metric.WithSchemaURL(SchemaURL)), reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

func attrsOf(set attribute.Set) map[string]string {
	m := map[string]string{}
	for _, kv := range set.ToSlice() {
		m[string(kv.Key)] = kv.Value.Emit()
	}
	return m
}

// flatten turns the collected data into records. Histogram records carry the sum as value when the
// data point holds one observation, and are otherwise expanded without a value.
func flatten(t *testing.T, rm metricdata.ResourceMetrics) []metricRecord {
	t.Helper()
	var out []metricRecord
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					for i := int64(0); i < dp.Value; i++ {
						out = append(out, metricRecord{m.Name, "add", 1, attrsOf(dp.Attributes)})
					}
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					for i := uint64(0); i < dp.Count; i++ {
						out = append(out, metricRecord{m.Name, "record", dp.Sum / float64(dp.Count), attrsOf(dp.Attributes)})
					}
				}
			default:
				t.Fatalf("unexpected metric data %T for %s", m.Data, m.Name)
			}
		}
	}
	return out
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func countOf(recs []metricRecord, name string) int {
	n := 0
	for _, r := range recs {
		if r.metric == name {
			n++
		}
	}
	return n
}

func consume(t *testing.T, h babelqueue.Handler, queue string, attempts int) error {
	t.Helper()
	env, err := babelqueue.Make("urn:acme:order.created", nil, babelqueue.WithQueue(queue))
	if err != nil {
		t.Fatal(err)
	}
	env.Attempts = attempts
	return h(context.Background(), env)
}

func TestMetrics_ConsumeSuccess(t *testing.T) {
	meter, reader := newMeter(t)
	tracer, _ := recorder(t)
	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error {
		time.Sleep(2 * time.Millisecond)
		return nil
	}, WithSystem("kafka"), WithMeter(meter))
	if err := consume(t, h, "orders.events", 0); err != nil {
		t.Fatal(err)
	}

	recs := flatten(t, collect(t, reader))
	if len(recs) != 2 {
		t.Fatalf("records = %+v, want consumed +1 and one duration (no retries at attempts 0)", recs)
	}
	for _, r := range recs {
		want := map[string]string{
			"messaging.system": "kafka", "messaging.operation.name": "process", "messaging.destination.name": "orders.events",
		}
		if r.key() != (metricRecord{r.metric, r.kind, 0, want}).key() {
			t.Errorf("record %s has attributes %v, want exactly %v", r.metric, r.attrs, want)
		}
	}
	if countOf(recs, MetricConsumedMessages) != 1 || countOf(recs, MetricProcessDuration) != 1 {
		t.Errorf("records = %+v", recs)
	}
	for _, r := range recs {
		if r.metric == MetricProcessDuration && !(r.value >= 0.002 && r.value < 5) {
			t.Errorf("duration = %v s, want the handler's wall time in seconds", r.value)
		}
	}
}

func TestMetrics_ConsumeFailureWithRetries(t *testing.T) {
	meter, reader := newMeter(t)
	tracer, _ := recorder(t)
	boom := errors.New("boom")
	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return boom }, WithSystem("kafka"), WithMeter(meter))
	if err := consume(t, h, "orders.events", 2); !errors.Is(err, boom) {
		t.Fatalf("the wrapper must return the handler's error unchanged, got %v", err)
	}

	recs := flatten(t, collect(t, reader))
	if countOf(recs, MetricConsumedMessages) != 1 || countOf(recs, MetricRetries) != 1 || countOf(recs, MetricProcessDuration) != 1 {
		t.Fatalf("records = %+v", recs)
	}
	for _, r := range recs {
		_, hasErr := r.attrs["error.type"]
		if want := r.metric == MetricProcessDuration; hasErr != want {
			t.Errorf("%s error.type present = %v, want %v (only the duration carries it: the delivery was received)", r.metric, hasErr, want)
		}
		if hasErr && r.attrs["error.type"] == "" {
			t.Errorf("%s error.type is empty", r.metric)
		}
	}
}

func TestMetrics_RetriesThresholdIsAttemptsAboveZero(t *testing.T) {
	meter, reader := newMeter(t)
	tracer, _ := recorder(t)
	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil }, WithMeter(meter))
	_ = consume(t, h, "orders", 0)
	if n := countOf(flatten(t, collect(t, reader)), MetricRetries); n != 0 {
		t.Fatalf("retries after attempts 0 = %d, want 0", n)
	}
	_ = consume(t, h, "orders", 1)
	if n := countOf(flatten(t, collect(t, reader)), MetricRetries); n != 1 {
		t.Fatalf("retries after attempts 1 = %d, want 1", n)
	}
}

func TestMetrics_NoDestinationWhenQueueBlank(t *testing.T) {
	meter, reader := newMeter(t)
	tracer, _ := recorder(t)
	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil }, WithMeter(meter))
	_ = consume(t, h, "  ", 0)
	for _, r := range flatten(t, collect(t, reader)) {
		if _, ok := r.attrs["messaging.destination.name"]; ok {
			t.Errorf("%s carries a destination for a blank queue: %v", r.metric, r.attrs)
		}
		if r.attrs["messaging.system"] != "babelqueue" {
			t.Errorf("%s system = %q", r.metric, r.attrs["messaging.system"])
		}
	}
}

func TestMetrics_NoMeterRecordsNothingAndStillWorks(t *testing.T) {
	tracer, sr := recorder(t)
	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil }, WithSystem("kafka"))
	if err := consume(t, h, "orders", 3); err != nil {
		t.Fatal(err)
	}
	if len(sr.Ended()) != 1 {
		t.Fatal("tracing must work without a meter")
	}
	pub := NewPublisher(tracer, babelqueue.NewApp(babelqueue.NewInMemoryTransport()))
	if _, err := pub.Publish(context.Background(), "urn:acme:a", nil); err != nil {
		t.Fatal(err)
	}
	// The observer with no meter is inert.
	NewDeadLetterObserver(WithSystem("kafka"))(context.Background(), babelqueue.DeadLetterEvent{Queue: "orders"})
}

func TestMetrics_PublishSuccessAndFailure(t *testing.T) {
	meter, reader := newMeter(t)
	tracer, _ := recorder(t)

	ok := NewPublisher(tracer, babelqueue.NewApp(babelqueue.NewInMemoryTransport()), WithSystem("kafka"), WithMeter(meter))
	if _, err := ok.Publish(context.Background(), "urn:acme:a", nil, babelqueue.WithQueue("orders.events")); err != nil {
		t.Fatal(err)
	}
	recs := flatten(t, collect(t, reader))
	if len(recs) != 1 {
		t.Fatalf("records = %+v", recs)
	}
	want := map[string]string{"messaging.system": "kafka", "messaging.operation.name": "send", "messaging.destination.name": "orders.events"}
	if recs[0].metric != MetricSentMessages || recs[0].key() != (metricRecord{MetricSentMessages, "add", 0, want}).key() {
		t.Errorf("record = %+v, want sent +1 with %v", recs[0], want)
	}

	bad := NewPublisher(tracer, babelqueue.NewApp(failTransport{}), WithSystem("rabbitmq"), WithMeter(meter))
	if _, err := bad.Publish(context.Background(), "urn:acme:a", nil, babelqueue.WithQueue("orders")); err == nil {
		t.Fatal("expected a publish error")
	}
	var failed *metricRecord
	for _, r := range flatten(t, collect(t, reader)) {
		if r.attrs["messaging.system"] == "rabbitmq" {
			r := r
			failed = &r
		}
	}
	if failed == nil || failed.attrs["error.type"] == "" || failed.attrs["messaging.destination.name"] != "orders" {
		t.Fatalf("failed attempt record = %+v, want sent +1 with destination orders and error.type", failed)
	}
}

func TestMetrics_PublishBeforeEnvelopeBuiltHasNoDestination(t *testing.T) {
	meter, reader := newMeter(t)
	tracer, _ := recorder(t)
	pub := NewPublisher(tracer, babelqueue.NewApp(babelqueue.NewInMemoryTransport()), WithMeter(meter))
	if _, err := pub.Publish(context.Background(), "urn:acme:a", map[string]any{"bad": make(chan int)},
		babelqueue.WithQueue("orders")); err == nil {
		t.Fatal("expected an encode error")
	}
	recs := flatten(t, collect(t, reader))
	if len(recs) != 1 || recs[0].attrs["error.type"] == "" {
		t.Fatalf("records = %+v, want one failed attempt", recs)
	}
	if _, ok := recs[0].attrs["messaging.destination.name"]; ok {
		t.Error("no envelope reached the transport, so no destination may be recorded")
	}
}

func TestMetrics_InstrumentDefinitions(t *testing.T) {
	meter, reader := newMeter(t)
	tracer, _ := recorder(t)
	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return nil }, WithMeter(meter))
	_ = consume(t, h, "orders", 1)
	pub := NewPublisher(tracer, babelqueue.NewApp(babelqueue.NewInMemoryTransport()), WithMeter(meter))
	_, _ = pub.Publish(context.Background(), "urn:acme:a", nil)
	NewDeadLetterObserver(WithMeter(meter))(context.Background(), babelqueue.DeadLetterEvent{Queue: "orders"})

	rm := collect(t, reader)
	for name, unit := range map[string]string{
		MetricProcessDuration: "s", MetricSentMessages: "{message}", MetricConsumedMessages: "{message}",
		MetricRetries: "{message}", MetricDeadLetters: "{message}",
	} {
		m, ok := findMetric(rm, name)
		if !ok {
			t.Errorf("metric %s not recorded", name)
			continue
		}
		if m.Unit != unit {
			t.Errorf("%s unit = %q, want %q", name, m.Unit, unit)
		}
		switch d := m.Data.(type) {
		case metricdata.Histogram[float64]:
			if name != MetricProcessDuration {
				t.Errorf("%s is a histogram", name)
			}
			if got := d.DataPoints[0].Bounds; len(got) != len(processDurationBuckets) || got[0] != 0.005 || got[len(got)-1] != 10 {
				t.Errorf("%s bounds = %v, want the semconv advisory boundaries", name, got)
			}
		case metricdata.Sum[int64]:
			if name == MetricProcessDuration || !d.IsMonotonic {
				t.Errorf("%s must be a monotonic counter", name)
			}
		}
	}
}

// ADR-0039 section 2: no URN, id, trace id, attempts, schema version, group or reason, on any point.
func TestMetrics_NeverCarryHighCardinalityAttributes(t *testing.T) {
	meter, reader := newMeter(t)
	tracer, _ := recorder(t)
	h := WrapHandler(tracer, func(context.Context, babelqueue.Envelope) error { return errors.New("x") },
		WithSystem("kafka"), WithConsumerGroup("g"), WithMeter(meter))
	_ = consume(t, h, "orders", 4)
	pub := NewPublisher(tracer, babelqueue.NewApp(babelqueue.NewInMemoryTransport()), WithSystem("kafka"), WithConsumerGroup("g"), WithMeter(meter))
	_, _ = pub.Publish(context.Background(), "urn:acme:a", nil)
	NewDeadLetterObserver(WithSystem("kafka"), WithMeter(meter))(context.Background(),
		babelqueue.DeadLetterEvent{Queue: "orders", DeadLetterQueue: "orders.dlq", Reason: "failed"})

	allowed := map[string]bool{
		"messaging.system": true, "messaging.operation.name": true, "messaging.destination.name": true, "error.type": true,
	}
	recs := flatten(t, collect(t, reader))
	if len(recs) == 0 {
		t.Fatal("nothing recorded")
	}
	for _, r := range recs {
		for k := range r.attrs {
			if !allowed[k] {
				t.Errorf("%s carries the forbidden attribute %q", r.metric, k)
			}
		}
	}
}

// The wiring: the core App reports a dead-letter it carried out to the otel observer.
func TestMetrics_DeadLetterThroughTheApp(t *testing.T) {
	meter, reader := newMeter(t)
	tr := babelqueue.NewInMemoryTransport()
	app := babelqueue.NewApp(tr,
		babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true),
		babelqueue.WithDeadLetterObserver(NewDeadLetterObserver(WithSystem("redis"), WithMeter(meter))))
	app.Handle("urn:acme:a", func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
	if _, err := app.Publish(context.Background(), "urn:acme:a", nil, babelqueue.WithQueue("orders")); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	recs := flatten(t, collect(t, reader))
	want := map[string]string{"messaging.system": "redis", "messaging.operation.name": "process", "messaging.destination.name": "orders"}
	if len(recs) != 1 || recs[0].key() != (metricRecord{MetricDeadLetters, "add", 0, want}).key() {
		t.Fatalf("records = %+v, want one %s with %v", recs, MetricDeadLetters, want)
	}
	if tr.Size("orders.dlq") != 1 {
		t.Errorf("the DLQ record was not written")
	}
}
