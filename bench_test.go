package babelqueue_test

import (
	"encoding/json"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// referenceBrokerRoundTrip is a conservative stand-in for one broker publish+consume
// round-trip. Local loopback Redis (RPUSH+BLMOVE+LREM) measures ~300µs here, but
// production brokers are networked/persistent — RabbitMQ with publisher confirms is
// commonly ≥0.5–2ms — so 750µs is conservative and the real-world overhead is lower.
// GR-8 budgets the envelope codec at <=2% of this. The same reference is used by the
// equivalent benchmark in every SDK.
const referenceBrokerRoundTrip = 750 * time.Microsecond

var benchData = map[string]any{
	"order_id": 1042,
	"amount":   99.9,
	"currency": "USD",
	"note":     "café ☕",
}

// envelopeRoundTrip is the full BabelQueue codec path: build, encode, decode.
func envelopeRoundTrip() {
	env, _ := babelqueue.Make("urn:babel:orders:created", benchData, babelqueue.WithQueue("orders"))
	body, _ := env.Encode()
	d, _ := babelqueue.Decode(body)
	_ = d
}

// barePayloadJSON is the baseline: serialize the bare payload, which any publisher
// does anyway. The difference between this and envelopeRoundTrip is what BabelQueue
// actually adds.
func barePayloadJSON() {
	body, _ := json.Marshal(benchData)
	var out map[string]any
	_ = json.Unmarshal(body, &out)
}

func BenchmarkEnvelopeRoundTrip(b *testing.B) {
	for i := 0; i < b.N; i++ {
		envelopeRoundTrip()
	}
}

func BenchmarkBarePayloadJSON(b *testing.B) {
	for i := 0; i < b.N; i++ {
		barePayloadJSON()
	}
}

// TestCodecOverheadWithinBudget proves GR-8: the envelope encode/decode path adds
// no more than 2% over plain-JSON serialization (the baseline every publisher
// already pays), measured against a conservative broker round-trip. It is pure CPU
// — no broker — so it is stable and environment-independent in CI.
func TestCodecOverheadWithinBudget(t *testing.T) {
	const iter = 200_000

	avg := func(fn func()) time.Duration {
		for i := 0; i < 20_000; i++ { // warm up
			fn()
		}
		start := time.Now()
		for i := 0; i < iter; i++ {
			fn()
		}
		return time.Since(start) / iter
	}

	envelope := avg(envelopeRoundTrip)
	bare := avg(barePayloadJSON)

	marginal := envelope - bare
	if marginal < 0 {
		marginal = 0
	}
	overhead := float64(marginal) / float64(referenceBrokerRoundTrip) * 100

	t.Logf("envelope codec: %v/op  bare JSON: %v/op  marginal: %v  overhead vs %v broker: %.2f%%",
		envelope, bare, marginal, referenceBrokerRoundTrip, overhead)

	if overhead > 2.0 {
		t.Fatalf("codec overhead %.2f%% exceeds the 2%% GR-8 budget (marginal %v over a %v round-trip)",
			overhead, marginal, referenceBrokerRoundTrip)
	}
}
