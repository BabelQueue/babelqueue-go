package amqp_test

import (
	"context"
	"os"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	bqamqp "github.com/babelqueue/babelqueue-go/amqp"
)

func brokerURL() string {
	if url := os.Getenv("AMQP_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@localhost:5672/"
}

// TestAMQPRoundTrip is an integration test: it skips unless a RabbitMQ broker is
// reachable (CI provides one as a service container).
func TestAMQPRoundTrip(t *testing.T) {
	tr := bqamqp.New(brokerURL())
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	const queue = "bq-test-amqp"

	env, err := babelqueue.Make("urn:babel:test:ping", map[string]any{"n": 1}, babelqueue.WithQueue(queue))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := env.Encode()

	if err := tr.Publish(ctx, queue, string(body)); err != nil {
		t.Skipf("rabbitmq not reachable: %v", err)
	}

	msg, err := tr.Pop(ctx, queue, time.Second)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if msg == nil {
		t.Fatal("expected a message")
	}
	if err := tr.Ack(ctx, msg); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	got, err := babelqueue.Decode([]byte(msg.Body))
	if err != nil || !got.Accepts() {
		t.Fatalf("decoded message not accepted: %v", err)
	}
	if got.URN() != "urn:babel:test:ping" {
		t.Errorf("urn = %q", got.URN())
	}
}

// TestAMQPAppEndToEnd runs a publish + drain through the App over RabbitMQ.
func TestAMQPAppEndToEnd(t *testing.T) {
	tr := bqamqp.New(brokerURL())
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	if err := tr.Publish(ctx, "bq-test-amqp-app", `{"probe":true}`); err != nil {
		t.Skipf("rabbitmq not reachable: %v", err)
	}
	_, _ = tr.Pop(ctx, "bq-test-amqp-app", time.Second) // drain the probe

	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("bq-test-amqp-app"))
	done := make(chan babelqueue.Envelope, 1)
	app.Handle("urn:babel:test:created", func(_ context.Context, env babelqueue.Envelope) error {
		done <- env
		return nil
	})

	if _, err := app.Publish(ctx, "urn:babel:test:created", map[string]any{"order_id": 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(ctx, "bq-test-amqp-app", 1); err != nil {
		t.Fatal(err)
	}

	select {
	case env := <-done:
		if env.URN() != "urn:babel:test:created" {
			t.Errorf("urn = %q", env.URN())
		}
	default:
		t.Fatal("handler was not invoked")
	}
}
