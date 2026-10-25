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

// calibrationBareJSON is what the bare-payload baseline (barePayloadJSON) costs per op on the
// machine the 750µs reference above was calibrated against (an uninstrumented dev machine; this
// test reports ~1.4µs there). It is the unit that converts the reference into runner time, see
// TestCodecOverheadWithinBudget.
const calibrationBareJSON = 1400 * time.Nanosecond

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
// — no broker.
//
// The gate must not depend on how fast the runner is. Dividing the marginal cost by a fixed
// 750µs made it flake on slow shared CI runners: the codec's CPU time grows with a slow or
// contended runner while the constant reference does not (R1 measured 1.0–2.09% across runs of
// the same code, with an envelope/bare-JSON ratio of 4.5–4.8x each time). So the reference is
// scaled by the runner's own speed, measured in the same process: a runner that runs the bare
// JSON baseline k times slower than the calibration machine is also taken to have a broker
// round-trip budget k times larger. The check is then
//
//	marginal <= 2% * referenceBrokerRoundTrip * (bare / calibrationBareJSON)
//
// i.e. marginal/bare <= 0.02 * 750µs / 1.4µs (about 10.7). This removes the dependence on plain
// CPU speed. It does not make the ratio constant: the codec allocates more than bare JSON, so
// under heavy load envelope/bare drifts upwards (about 4.5-4.8x on R1 CI, 5.2x on a quiet dev
// machine, 6.1-6.3x on the same machine under load, i.e. up to ~20% above quiet), which still
// leaves about 2x headroom to the threshold. The 2% of a 750µs round-trip keeps its meaning on
// the calibration machine, and a codec that gets slower relative to plain JSON still fails.
func TestCodecOverheadWithinBudget(t *testing.T) {
	// GR-8 is a pure-CPU timing gate. The race detector and coverage instrumentation
	// both inflate the codec disproportionately (it executes more instrumented code and
	// touches more memory than bare JSON), so the measurement is meaningless under -race
	// or -cover. CI enforces this gate in a dedicated uninstrumented step instead.
	if raceEnabled {
		t.Skip("GR-8 overhead is a pure-CPU timing gate; skipped under -race (enforced in an uninstrumented CI step)")
	}
	if testing.CoverMode() != "" {
		t.Skip("GR-8 overhead is a pure-CPU timing gate; skipped under -cover (enforced in an uninstrumented CI step)")
	}

	const iter = 200_000
	const rounds = 7

	// measure returns the average per-op duration for fn over `iter` calls.
	measure := func(fn func()) time.Duration {
		start := time.Now()
		for i := 0; i < iter; i++ {
			fn()
		}
		return time.Since(start) / iter
	}

	for i := 0; i < 20_000; i++ { // warm up both paths
		envelopeRoundTrip()
		barePayloadJSON()
	}

	// The two paths are measured alternately inside every round and each keeps its fastest
	// (minimum) round. CPU contention, GC and scheduler preemption on shared CI runners only
	// ever ADD time, so the minimum is the closest estimate of the true pure-CPU cost. The
	// alternation spreads a slow stretch over both paths instead of one; the two minima are still
	// picked independently, so it narrows the ratio's drift without making it constant. A
	// genuinely slow codec is slow in every round, including its fastest, so this cannot mask a
	// real regression.
	envelope, bare := measure(envelopeRoundTrip), measure(barePayloadJSON)
	for r := 1; r < rounds; r++ {
		if d := measure(envelopeRoundTrip); d < envelope {
			envelope = d
		}
		if d := measure(barePayloadJSON); d < bare {
			bare = d
		}
	}

	marginal := envelope - bare
	if marginal < 0 {
		marginal = 0
	}
	// The broker round-trip budget on THIS runner, in runner time.
	reference := time.Duration(float64(referenceBrokerRoundTrip) * float64(bare) / float64(calibrationBareJSON))
	overhead := float64(marginal) / float64(reference) * 100

	t.Logf("envelope codec: %v/op  bare JSON: %v/op (%.2fx)  marginal: %v  runner-scaled broker reference: %v (%v x %.2f)  overhead: %.2f%% (best of %d rounds)",
		envelope, bare, float64(envelope)/float64(bare), marginal, reference, referenceBrokerRoundTrip,
		float64(bare)/float64(calibrationBareJSON), overhead, rounds)

	if overhead > 2.0 {
		t.Fatalf("codec overhead %.2f%% exceeds the 2%% GR-8 budget (marginal %v over a runner-scaled %v round-trip)",
			overhead, marginal, reference)
	}
}
