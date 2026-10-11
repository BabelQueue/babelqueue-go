package babelqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// keyedTransport serves one message and records every re-publish: through the plain Publish it
// has no access to the original message, through PublishFrom ([babelqueue.KeyedRepublisher]) it
// does.
type keyedTransport struct {
	msg       *babelqueue.ReceivedMessage
	served    bool
	plain     []string
	fromQueue []string
	fromMsg   []*babelqueue.ReceivedMessage
}

func (k *keyedTransport) Publish(_ context.Context, queue, _ string) error {
	k.plain = append(k.plain, queue)
	return nil
}

func (k *keyedTransport) PublishFrom(_ context.Context, queue, _ string, from *babelqueue.ReceivedMessage) error {
	k.fromQueue = append(k.fromQueue, queue)
	k.fromMsg = append(k.fromMsg, from)
	return nil
}

func (k *keyedTransport) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	if k.served {
		return nil, nil
	}
	k.served = true
	return k.msg, nil
}

func (k *keyedTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error { return nil }

// The App hands the received message to a KeyedRepublisher for its retry and dead-letter
// re-publishes instead of calling the plain Publish.
func TestAppRepublishUsesKeyedRepublisher(t *testing.T) {
	const urn = "urn:babel:orders:created"
	env, err := babelqueue.Make(urn, map[string]any{"order_id": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()

	cases := []struct {
		name      string
		opts      []babelqueue.AppOption
		wantQueue string
	}{
		{"retry", []babelqueue.AppOption{babelqueue.WithMaxAttempts(3)}, "orders"},
		{"dead letter", []babelqueue.AppOption{babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true)}, "orders.dlq"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			received := &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders"}
			tr := &keyedTransport{msg: received}
			app := babelqueue.NewApp(tr, c.opts...)
			app.Handle(urn, func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
			if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
				t.Fatal(err)
			}
			if len(tr.plain) != 0 {
				t.Fatalf("plain Publish calls = %v; a KeyedRepublisher must take over the re-publish", tr.plain)
			}
			if len(tr.fromQueue) != 1 || tr.fromQueue[0] != c.wantQueue || tr.fromMsg[0] != received {
				t.Fatalf("PublishFrom queues = %v, same message = %v; want one call on %q with the received message",
					tr.fromQueue, len(tr.fromMsg) == 1 && tr.fromMsg[0] == received, c.wantQueue)
			}
		})
	}
}
