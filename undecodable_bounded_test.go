package babelqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// inPlaceTransport is a generic App invariant fixture: an in-place Release, NO broker delivery
// counter and NO Poison flag. Release re-queues the re-encoded body with attempts+1 and declines
// with ErrReleaseUnsupported a body the codec cannot decode. It proves the App's own bounded
// re-publish path terminates for any such transport. It does not model Redis: the Redis
// transports flag codec-rejected bodies Poison so they never reach this path (see
// redis/legacy_poison_app_test.go for that proof against the real transport). Every delivery is
// counted.
type inPlaceTransport struct {
	*babelqueue.InMemoryTransport
	queue      []string
	deliveries int
	acked      int
	dlq        []string
}

func (p *inPlaceTransport) Pop(_ context.Context, _ string, _ time.Duration) (*babelqueue.ReceivedMessage, error) {
	if len(p.queue) == 0 {
		return nil, nil
	}
	body := p.queue[0]
	p.queue = p.queue[1:]
	p.deliveries++
	return &babelqueue.ReceivedMessage{Queue: "default", Body: body}, nil
}

func (p *inPlaceTransport) Publish(_ context.Context, queue, body string) error {
	if queue == "default" {
		p.queue = append(p.queue, body)
		return nil
	}
	p.dlq = append(p.dlq, queue+"|"+body)
	return nil
}

func (p *inPlaceTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error {
	p.acked++
	return nil
}

func (p *inPlaceTransport) Release(_ context.Context, msg *babelqueue.ReceivedMessage, _ time.Duration) error {
	env, err := babelqueue.Decode([]byte(msg.Body))
	if err != nil && !errors.Is(err, babelqueue.ErrNoIdentity) {
		return babelqueue.ErrReleaseUnsupported
	}
	env.Attempts++
	out, err := env.Encode()
	if err != nil {
		return babelqueue.ErrReleaseUnsupported
	}
	p.queue = append(p.queue, string(out))
	return nil
}

// MAJOR (in-place transport without a Poison flag): with the default App settings (no dead-letter, StrategyFail,
// maxAttempts 3) an undecodable body must terminate within maxAttempts deliveries and leave the
// queue empty. It must not be redelivered forever.
func TestUndecodableBodyIsBoundedUnderDefaultSettings(t *testing.T) {
	for _, body := range []string{`not-json`, `{"job":`, `[1,2]`, `{"job":"urn:x","attempts":0,"meta":"x"}`} {
		t.Run(body, func(t *testing.T) {
			tr := &inPlaceTransport{InMemoryTransport: babelqueue.NewInMemoryTransport(), queue: []string{body}}
			app := babelqueue.NewApp(tr)

			// A cap well above maxAttempts: an unbounded loop would hit it and fail below.
			processed, err := app.Drain(context.Background(), "default", 50)
			if err != nil {
				t.Fatal(err)
			}
			if processed != 3 || tr.deliveries != 3 {
				t.Fatalf("processed %d / deliveries %d, want exactly maxAttempts (3)", processed, tr.deliveries)
			}
			if len(tr.queue) != 0 {
				t.Fatalf("queue not empty after drain: %q", tr.queue)
			}
			if len(tr.dlq) != 0 {
				t.Fatalf("nothing may be dead-lettered with dead-lettering off: %q", tr.dlq)
			}
		})
	}
}

// Same body with dead-lettering on: the raw body is written to the DLQ inside a poison record,
// at the first delivery, and the queue ends empty.
func TestUndecodableBodyDeadLettersRawWhenEnabled(t *testing.T) {
	const body = `not-json`
	tr := &inPlaceTransport{InMemoryTransport: babelqueue.NewInMemoryTransport(), queue: []string{body}}
	app := babelqueue.NewApp(tr, babelqueue.WithDeadLetter(true))

	processed, err := app.Drain(context.Background(), "default", 50)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 || tr.deliveries != 1 {
		t.Fatalf("processed %d / deliveries %d, want 1", processed, tr.deliveries)
	}
	if len(tr.dlq) != 1 || !strings.HasPrefix(tr.dlq[0], "default.dlq|") {
		t.Fatalf("dlq = %q, want exactly one record on default.dlq", tr.dlq)
	}
	var rec struct {
		Raw        string `json:"raw"`
		DeadLetter struct {
			Reason string `json:"reason"`
		} `json:"dead_letter"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(tr.dlq[0], "default.dlq|")), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Raw != body || rec.DeadLetter.Reason != "poison" {
		t.Fatalf("poison record = %+v, want raw %q and reason poison", rec, body)
	}
	if len(tr.queue) != 0 {
		t.Fatalf("queue not empty: %q", tr.queue)
	}
}
