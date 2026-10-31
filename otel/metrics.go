package otel

import (
	"context"
	"log/slog"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The standard metric set (ADR-0039). Names, units and kinds are identical in every BabelQueue SDK.
const (
	// MetricProcessDuration is the handler's wall time in seconds, recorded once per invocation,
	// success or failure (histogram, unit "s").
	MetricProcessDuration = "messaging.process.duration"
	// MetricSentMessages counts publish attempts, success or failure (counter, unit "{message}").
	MetricSentMessages = "messaging.client.sent.messages"
	// MetricConsumedMessages counts the deliveries that enter the consumer wrapper (counter).
	MetricConsumedMessages = "messaging.client.consumed.messages"
	// MetricRetries counts the deliveries that enter the wrapper with top-level attempts > 0 (counter).
	MetricRetries = "babelqueue.retries"
	// MetricDeadLetters counts the dead-letters the SDK carried out (counter), fed by the
	// observer from [NewDeadLetterObserver].
	MetricDeadLetters = "babelqueue.dlq.messages"
)

const messageUnit = "{message}"

// processDurationBuckets is the semconv advisory bucket boundary set for messaging.process.duration.
var processDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}

// instruments holds the metric instruments of one wrapper, created once from its meter. A nil
// *instruments (no meter) records nothing; so does an instrument that could not be created.
type instruments struct {
	duration metric.Float64Histogram
	sentMsgs metric.Int64Counter
	consumed metric.Int64Counter
	retries  metric.Int64Counter
	dlq      metric.Int64Counter
}

func newInstruments(m metric.Meter) *instruments {
	if m == nil {
		return nil
	}
	in := &instruments{}
	var err error
	if in.duration, err = m.Float64Histogram(MetricProcessDuration,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of processing one delivery in the handler."),
		metric.WithExplicitBucketBoundaries(processDurationBuckets...)); err != nil {
		logInstrumentError(MetricProcessDuration, err)
	}
	in.sentMsgs = counter(m, MetricSentMessages, "Number of publish attempts.")
	in.consumed = counter(m, MetricConsumedMessages, "Number of deliveries that entered the consumer wrapper.")
	in.retries = counter(m, MetricRetries, "Number of deliveries that were already attempted before (attempts > 0).")
	in.dlq = deadLetterCounter(m)
	return in
}

func deadLetterCounter(m metric.Meter) metric.Int64Counter {
	return counter(m, MetricDeadLetters, "Number of dead-letters the SDK carried out.")
}

func counter(m metric.Meter, name, description string) metric.Int64Counter {
	c, err := m.Int64Counter(name, metric.WithUnit(messageUnit), metric.WithDescription(description))
	if err != nil {
		logInstrumentError(name, err)
		return nil
	}
	return c
}

func logInstrumentError(name string, err error) {
	slog.Error("babelqueue otel: metric instrument not created; it will record nothing", "metric", name, "error", err)
}

// metricAttrs builds the ADR-0039 section 2 attribute set: nothing but the system, the operation name,
// the destination when known and error.type on a failure. No URN, id, trace id, attempts, schema
// version or reason, ever (cardinality limit).
func metricAttrs(system, operation, destination, errType string) metric.MeasurementOption {
	kv := make([]attribute.KeyValue, 0, 4)
	kv = append(kv,
		attribute.String("messaging.system", system),
		attribute.String("messaging.operation.name", operation),
	)
	if !isBlank(destination) {
		kv = append(kv, attribute.String("messaging.destination.name", destination))
	}
	if errType != "" {
		kv = append(kv, attribute.String("error.type", errType))
	}
	return metric.WithAttributeSet(attribute.NewSet(kv...))
}

// delivered records a delivery entering the consumer wrapper: consumed +1, and retries +1 when
// the message was attempted before.
func (in *instruments) delivered(ctx context.Context, system string, env babelqueue.Envelope) {
	if in == nil {
		return
	}
	attrs := metricAttrs(system, "process", env.Meta.Queue, "")
	if in.consumed != nil {
		in.consumed.Add(ctx, 1, attrs)
	}
	if env.Attempts > 0 && in.retries != nil {
		in.retries.Add(ctx, 1, attrs)
	}
}

// processed records the handler's wall time; errType is non-empty when the handler failed.
func (in *instruments) processed(ctx context.Context, system, destination string, d time.Duration, errType string) {
	if in == nil || in.duration == nil {
		return
	}
	in.duration.Record(ctx, d.Seconds(), metricAttrs(system, "process", destination, errType))
}

// sent records one publish attempt; destination is empty when no envelope reached the transport.
func (in *instruments) sent(ctx context.Context, system, destination, errType string) {
	if in == nil || in.sentMsgs == nil {
		return
	}
	in.sentMsgs.Add(ctx, 1, metricAttrs(system, "send", destination, errType))
}

// NewDeadLetterObserver returns the [babelqueue.DeadLetterObserver] that feeds
// babelqueue.dlq.messages (ADR-0039 section 5): +1 per dead-letter the App carried out, with
// messaging.system from [WithSystem], messaging.operation.name "process" and
// messaging.destination.name the source queue when known. Register it on the runtime:
//
//	app := babelqueue.NewApp(transport,
//	    babelqueue.WithDeadLetterObserver(otel.NewDeadLetterObserver(
//	        otel.WithSystem("kafka"), otel.WithMeter(meter))))
//
// Without [WithMeter] the observer records nothing.
func NewDeadLetterObserver(opts ...Option) babelqueue.DeadLetterObserver {
	cfg := newConfig(opts)
	var dlq metric.Int64Counter
	if cfg.meter != nil {
		dlq = deadLetterCounter(cfg.meter)
	}
	return func(ctx context.Context, ev babelqueue.DeadLetterEvent) {
		if dlq == nil {
			return
		}
		dlq.Add(ctx, 1, metricAttrs(cfg.system, "process", ev.Queue, ""))
	}
}
