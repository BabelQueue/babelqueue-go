package redis

import (
	"context"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	goredis "github.com/redis/go-redis/v9"
)

// legacyAppTransport drives a real App over the REAL default-layout delivery builder
// ([Transport.legacyMessage], which decides Poison) and the real Release decision. Only the
// Redis calls are replaced (Pop reads an in-memory list, DeadLetter and Ack record), so no server is
// needed. It exists so the App-level proof does not copy the transport's decision into a fake.
type legacyAppTransport struct {
	tr         *Transport
	queue      []string
	deliveries int
	acked      int
	released   int
	dlq        []string // "<dlq queue>|<body>"
}

func (l *legacyAppTransport) Publish(_ context.Context, queue, body string) error {
	if queue == "q" {
		l.queue = append(l.queue, body)
		return nil
	}
	l.dlq = append(l.dlq, queue+"|"+body)
	return nil
}

func (l *legacyAppTransport) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	if len(l.queue) == 0 {
		return nil, nil
	}
	body := l.queue[0]
	l.queue = l.queue[1:]
	l.deliveries++
	return l.tr.legacyMessage("q", body, "0123456789abcdef0123456789abcdef"), nil
}

func (l *legacyAppTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error {
	l.acked++
	return nil
}

func (l *legacyAppTransport) Release(ctx context.Context, msg *babelqueue.ReceivedMessage, delay time.Duration) error {
	l.released++
	return l.tr.Release(ctx, msg, delay) // real decision; returns before any Redis call for an undecodable body
}

func (l *legacyAppTransport) DeadLetter(_ context.Context, _ *babelqueue.ReceivedMessage, dlq, body string) error {
	l.dlq = append(l.dlq, dlq+"|"+body)
	return nil
}

// Binding §1.1 (poison instead of loss) in the default :processing layout: an undecodable body is
// written to the DLQ with its raw bytes on the FIRST delivery, in every App configuration
// (including dead-lettering off, where it used to end in a silent drop after 3 deliveries with a
// zero envelope as the only trace). Nothing is retried, released or handled.
func TestLegacyUndecodableBodyIsDeadLetteredRawThroughApp(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	configs := map[string][]babelqueue.AppOption{
		"default":           nil,
		"dead-letter off":   {babelqueue.WithDeadLetter(false)},
		"dead-letter on":    {babelqueue.WithDeadLetter(true)},
		"strategy delete":   {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete)},
		"strategy release":  {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease)},
		"max attempts 1":    {babelqueue.WithMaxAttempts(1)},
		"custom dlq suffix": {babelqueue.WithDeadLetterSuffix(".dead")},
	}
	bodies := []string{`not-json`, `{"job":`, `[1,2]`, `{"job":"urn:x","attempts":0,"meta":"x"}`}

	for name, opts := range configs {
		for _, body := range bodies {
			t.Run(name+"/"+body, func(t *testing.T) {
				l := &legacyAppTransport{tr: NewWithClient(client), queue: []string{body}}
				app := babelqueue.NewApp(l, opts...)
				handled := 0
				app.Handle("urn:x", func(context.Context, babelqueue.Envelope) error { handled++; return nil })

				processed, err := app.Drain(context.Background(), "q", 50)
				if err != nil {
					t.Fatal(err)
				}
				if processed != 1 || l.deliveries != 1 {
					t.Fatalf("processed %d / deliveries %d, want exactly 1", processed, l.deliveries)
				}
				suffix := ".dlq"
				if name == "custom dlq suffix" {
					suffix = ".dead"
				}
				want := "q" + suffix + "|" + body
				if len(l.dlq) != 1 || l.dlq[0] != want {
					t.Fatalf("dlq = %q, want exactly [%q] (raw bytes)", l.dlq, want)
				}
				if handled != 0 || l.released != 0 || l.acked != 0 || len(l.queue) != 0 {
					t.Fatalf("handled %d released %d acked %d queue %q: want none (DeadLetter settles the reservation)", handled, l.released, l.acked, l.queue)
				}
			})
		}
	}
}

// An identity-less but codec-valid body is NOT poison: it decodes and the App decides.
func TestLegacyMessageFlagsOnlyCodecRejectedBodiesAsPoison(t *testing.T) {
	tr := NewWithClient(goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"}))
	t.Cleanup(func() { _ = tr.Close() })
	tok := "0123456789abcdef0123456789abcdef"
	for body, want := range map[string]bool{
		`not-json`: true,
		`{"job":"urn:x","attempts":0,"meta":"x"}`: true,
		`{"job":"urn:x","attempts":0}`:            false,
		`{"attempts":0,"data":{}}`:                false,
	} {
		if got := tr.legacyMessage("q", body, tok).Poison; got != want {
			t.Fatalf("legacyMessage(%q).Poison = %v, want %v", body, got, want)
		}
	}
}
