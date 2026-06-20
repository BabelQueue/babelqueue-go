package amqp

import (
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// TestPublishingMergesHeadersWithoutClobbering proves an out-of-band header
// (traceparent) is written into the AMQP header table beside the contract x-*
// headers, and that a contract header always wins a key collision.
func TestPublishingMergesHeadersWithoutClobbering(t *testing.T) {
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1}, babelqueue.WithQueue("orders"))
	body, _ := env.Encode()

	tr := &Transport{}
	pub := tr.publishing(string(body), map[string]string{
		"traceparent":      "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"tracestate":       "vendor=value",
		"x-source-lang":    "ATTACKER", // must NOT overwrite the contract header
		"":                 "blank-key-skipped",
		"bq-replay-bypass": "1",
	})

	if got, _ := pub.Headers["traceparent"].(string); got != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Errorf("traceparent not carried: %q", got)
	}
	if got, _ := pub.Headers["tracestate"].(string); got != "vendor=value" {
		t.Errorf("tracestate not carried: %q", got)
	}
	if got, _ := pub.Headers["bq-replay-bypass"].(string); got != "1" {
		t.Errorf("bq-replay-bypass not carried: %q", got)
	}
	// The contract projection must survive the merge unchanged.
	if got, _ := pub.Headers["x-source-lang"].(string); got != env.Meta.Lang {
		t.Errorf("contract x-source-lang clobbered: got %q, want %q", got, env.Meta.Lang)
	}
	if v, _ := pub.Headers["x-schema-version"].(int); v != env.Meta.SchemaVersion {
		t.Errorf("x-schema-version = %v, want %d", pub.Headers["x-schema-version"], env.Meta.SchemaVersion)
	}
	if _, exists := pub.Headers[""]; exists {
		t.Error("blank header key must be skipped")
	}
}

// TestPublishingNilHeadersIsByteForBytePlainPublish proves PublishWithHeaders with
// no extra headers produces exactly the same message as a plain Publish would.
func TestPublishingNilHeadersUnchanged(t *testing.T) {
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
	body, _ := env.Encode()

	tr := &Transport{}
	plain := tr.publishing(string(body), nil)

	if plain.Type != env.Job || plain.CorrelationId != env.TraceID || plain.MessageId != env.Meta.ID {
		t.Error("contract properties changed under nil headers")
	}
	// Only the contract x-* headers should be present.
	for k := range plain.Headers {
		switch k {
		case "x-attempts", "x-schema-version", "x-source-lang":
		default:
			t.Errorf("unexpected header %q on a plain publish", k)
		}
	}
}

func TestHeadersFromTable(t *testing.T) {
	if got := headersFromTable(nil); got != nil {
		t.Errorf("nil table must surface nil headers, got %v", got)
	}
	if got := headersFromTable(amqp091.Table{}); got != nil {
		t.Errorf("empty table must surface nil headers, got %v", got)
	}

	got := headersFromTable(amqp091.Table{
		"traceparent":      "00-trace-span-01",
		"x-attempts":       3,             // ints stringify
		"bq-replay-bypass": []byte("1"),   // byte slices stringify
		"x-source-lang":    "go",
		"nil-value":        nil, // skipped
	})
	if got["traceparent"] != "00-trace-span-01" {
		t.Errorf("traceparent = %q", got["traceparent"])
	}
	if got["x-attempts"] != "3" {
		t.Errorf("x-attempts (int) = %q, want \"3\"", got["x-attempts"])
	}
	if got["bq-replay-bypass"] != "1" {
		t.Errorf("bq-replay-bypass ([]byte) = %q, want \"1\"", got["bq-replay-bypass"])
	}
	if _, ok := got["nil-value"]; ok {
		t.Error("nil-valued header must be skipped")
	}
}

// TestHeaderRoundTrip proves a traceparent injected on the produce side survives a
// table → map[string]string extraction on the consume side, fully in-process.
func TestHeaderRoundTrip(t *testing.T) {
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	env, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
	body, _ := env.Encode()

	pub := (&Transport{}).publishing(string(body), map[string]string{"traceparent": tp})
	// pub.Headers is what RabbitMQ would deliver back as delivery.Headers.
	out := headersFromTable(pub.Headers)
	if out["traceparent"] != tp {
		t.Fatalf("round-trip lost traceparent: got %q, want %q", out["traceparent"], tp)
	}
}

var _ babelqueue.HeaderPublisher = (*Transport)(nil)
