//go:build integration

// Integration test for the SQS transport against LocalStack/ElasticMQ. It is
// excluded from the default unit run (no broker) and executed in CI with
//
//	go test -tags=integration ./...
//
// pointed at a LocalStack service (SQS_ENDPOINT, AWS_REGION). It exercises the
// real AWS SDK path — New's config loading, GetQueueUrl, SendMessage,
// ReceiveMessage and DeleteMessage — that the unit tests fake.
package sqs_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	babelqueue "github.com/babelqueue/babelqueue-go"
	"github.com/babelqueue/babelqueue-go/sqs"
)

func TestLocalStackRoundTrip(t *testing.T) {
	endpoint := getenv("SQS_ENDPOINT", "http://localhost:4566")
	region := getenv("AWS_REGION", "us-east-1")
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		t.Skipf("aws config unavailable: %v", err)
	}
	raw := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) { o.BaseEndpoint = aws.String(endpoint) })

	const queue = "bq-it-orders"
	if _, err := raw.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(queue)}); err != nil {
		t.Skipf("LocalStack unreachable (CreateQueue): %v", err)
	}

	tr, err := sqs.New(ctx, sqs.WithRegion(region), sqs.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue(queue))

	var seen babelqueue.Envelope
	app.Handle("urn:babel:orders:created", func(_ context.Context, e babelqueue.Envelope) error {
		seen = e
		return nil
	})

	id, err := app.Publish(ctx, "urn:babel:orders:created", map[string]any{"order_id": 1042})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// SQS is eventually consistent — poll a few times.
	drained := 0
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline) && drained == 0; {
		n, err := app.Drain(ctx, queue, 1)
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
		drained += n
		if n == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if drained != 1 {
		t.Fatalf("drained %d messages, want 1", drained)
	}
	if seen.URN() != "urn:babel:orders:created" || seen.Meta.ID != id {
		t.Errorf("consumed urn=%q id=%q, want urn:babel:orders:created / %q", seen.URN(), seen.Meta.ID, id)
	}
	if seen.Data["order_id"].(float64) != 1042 {
		t.Errorf("data.order_id = %v, want 1042", seen.Data["order_id"])
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
