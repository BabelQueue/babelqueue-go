package babelqueue

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type retryBackoffCase struct {
	Name       string   `json:"name"`
	Base       float64  `json:"retry_backoff"`
	Multiplier float64  `json:"retry_backoff_multiplier"`
	Max        float64  `json:"retry_backoff_max"`
	Jitter     string   `json:"retry_jitter"`
	N          int      `json:"n"`
	Expect     *float64 `json:"expect_delay_seconds"`
	Range      *struct {
		Min float64 `json:"min"`
		Max float64 `json:"max"`
	} `json:"expect_delay_range_seconds"`
}

func seconds(f float64) time.Duration { return time.Duration(math.Round(f * float64(time.Second))) }

// TestConformanceRetryBackoff runs the `retry_backoff` section: d(n) = min(max, base *
// multiplier^(n-1)) with none/full jitter, on the helper the App uses for every retry.
func TestConformanceRetryBackoff(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "conformance", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(raw, &sections); err != nil {
		t.Fatal(err)
	}
	var section struct {
		Cases []retryBackoffCase `json:"cases"`
	}
	if err := json.Unmarshal(sections["retry_backoff"], &section); err != nil {
		t.Fatal(err)
	}
	cases := section.Cases
	if len(cases) == 0 {
		t.Fatal("retry_backoff section has no cases")
	}
	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			base, mx := seconds(c.Base), seconds(c.Max)
			switch c.Jitter {
			case "none":
				if c.Expect == nil {
					t.Fatal("none-jitter case without expect_delay_seconds")
				}
				got := retryDelay(base, c.Multiplier, mx, JitterNone, c.N, func() float64 { return 0.5 })
				if want := seconds(*c.Expect); got != want {
					t.Fatalf("d(%d) = %v, want %v", c.N, got, want)
				}
			case "full":
				if c.Range == nil {
					t.Fatal("full-jitter case without expect_delay_range_seconds")
				}
				for _, r := range []float64{0, 0.25, 0.5, 0.999999} {
					r := r
					got := retryDelay(base, c.Multiplier, mx, JitterFull, c.N, func() float64 { return r })
					if got < seconds(c.Range.Min) || got > seconds(c.Range.Max) {
						t.Fatalf("d(%d) with draw %v = %v, want in [%v, %v]", c.N, r, got, c.Range.Min, c.Range.Max)
					}
				}
			default:
				t.Fatalf("unknown jitter %q", c.Jitter)
			}
		})
	}
}

func TestRetryDelayDoesNotOverflow(t *testing.T) {
	if got := retryDelay(time.Second, 10, 0, JitterNone, 500, nil); got != time.Duration(math.MaxInt64) {
		t.Fatalf("unbounded growth = %v, want saturation at MaxInt64", got)
	}
	if got := retryDelay(time.Second, 10, time.Minute, JitterNone, 500, nil); got != time.Minute {
		t.Fatalf("capped growth = %v, want 1m", got)
	}
}

// TestAppRetryBackoffGrowsPerAttempt: the App feeds the n-th delay (n = attempts + 1) to Release.
func TestAppRetryBackoffGrowsPerAttempt(t *testing.T) {
	tr := &releasingTransport{InMemoryTransport: NewInMemoryTransport()}
	app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(10),
		WithRetryBackoff(time.Second), WithRetryBackoffMultiplier(2), WithRetryBackoffMax(5*time.Second))
	app.Handle("urn:babel:orders:created", func(context.Context, Envelope) error { return errors.New("boom") })
	for attempts := 0; attempts < 4; attempts++ {
		env, err := Make("urn:babel:orders:created", map[string]any{}, WithQueue("orders"))
		if err != nil {
			t.Fatal(err)
		}
		env.Attempts = attempts
		body, _ := env.Encode()
		if err := tr.Publish(context.Background(), "orders", string(body)); err != nil {
			t.Fatal(err)
		}
		if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
			t.Fatal(err)
		}
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second}
	if len(tr.delays) != len(want) {
		t.Fatalf("Release delays = %v, want %v", tr.delays, want)
	}
	for i := range want {
		if tr.delays[i] != want[i] {
			t.Fatalf("Release delays = %v, want %v", tr.delays, want)
		}
	}
}
