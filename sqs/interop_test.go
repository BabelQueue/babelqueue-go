//go:build interop

// Cross-language interop: the UNCHANGED Go consumer drains a queue produced by the
// Python SDK over a real SQS (LocalStack), proving byte-level parity. Excluded from
// the default + integration runs; driven by the interop harness:
//
//	go test -tags=interop -run TestCrossLanguage ./...
//
// with SQS_ENDPOINT + BQ_INTEROP_QUEUE pointing at the LocalStack queue the Python
// producer already populated.
package sqs_test

import (
	"context"
	"os"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"github.com/babelqueue/babelqueue-go/sqs"
)

func TestCrossLanguagePythonProducedConsumedByGo(t *testing.T) {
	endpoint := envOr("SQS_ENDPOINT", "http://localhost:4566")
	queue := envOr("BQ_INTEROP_QUEUE", "bq-interop")
	region := envOr("AWS_REGION", "us-east-1")
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	ctx := context.Background()
	tr, err := sqs.New(ctx, sqs.WithRegion(region), sqs.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue(queue))

	var seen babelqueue.Envelope
	got := false
	app.Handle("urn:babel:orders:created", func(_ context.Context, e babelqueue.Envelope) error {
		seen, got = e, true
		return nil
	})

	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline) && !got; {
		n, derr := app.Drain(ctx, queue, 1)
		if derr != nil {
			t.Fatalf("Drain: %v", derr)
		}
		if n == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if !got {
		t.Fatal("no message consumed from the Python-produced queue")
	}

	// The unchanged Go consumer read the Python producer's bytes — parity.
	if seen.Meta.Lang != "python" {
		t.Errorf("meta.lang = %q, want \"python\" (cross-language proof)", seen.Meta.Lang)
	}
	if seen.URN() != "urn:babel:orders:created" {
		t.Errorf("urn = %q, want urn:babel:orders:created", seen.URN())
	}
	if v, _ := seen.Data["order_id"].(float64); v != 4242 {
		t.Errorf("data.order_id = %v, want 4242", seen.Data["order_id"])
	}
	if seen.TraceID == "" {
		t.Error("trace_id missing — must be preserved across the hop")
	}
	t.Logf("OK Go consumed Python-produced message: urn=%s lang=%s trace=%s data=%v",
		seen.URN(), seen.Meta.Lang, seen.TraceID, seen.Data)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
