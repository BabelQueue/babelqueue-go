package redis

import (
	"context"
	"strings"
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
// written to the DLQ as an ADR-0035 poison record on the FIRST delivery, in every App configuration
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
		"strategy fail":     {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyFail), babelqueue.WithDeadLetter(false)},
		// ADR-0036 decision 4: the identity-less body goes to the DLQ even when the strategy and the
		// dead-letter setting would otherwise drop it or retry it.
		"strategy delete, dead-letter off": {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete), babelqueue.WithDeadLetter(false)},
	}
	bodies := []string{`not-json`, `{"job":`, `[1,2]`, `{"job":"urn:x","attempts":0,"meta":"x"}`, "\xff\xfe not-json",
		// Identity-less bodies (ADR-0036 decision 4, K-E): neither a valid job nor a valid urn.
		`{"attempts":0,"data":{}}`, `{"job":5,"attempts":0}`, `{"job":" ","urn":"","attempts":0}`, `{"urn":7,"attempts":0}`,
		// A decodable envelope whose data carries a raw 0xFF byte: the codec accepts it (U+FFFD), but
		// invalid UTF-8 is poison (binding §1.1), so it must never reach the "urn:x" handler.
		"{\"job\":\"urn:x\",\"attempts\":0,\"data\":{\"n\":\"\xff\"},\"meta\":{\"id\":\"1\",\"queue\":\"q\"}}"}

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
				if len(l.dlq) != 1 {
					t.Fatalf("dlq = %q, want exactly one record", l.dlq)
				}
				record, ok := strings.CutPrefix(l.dlq[0], "q"+suffix+"|")
				if !ok {
					t.Fatalf("dlq entry %q is not addressed to q%s", l.dlq[0], suffix)
				}
				assertPoisonRecord(t, record, body, "q")
				if handled != 0 || l.released != 0 || l.acked != 0 || len(l.queue) != 0 {
					t.Fatalf("handled %d released %d acked %d queue %q: want none (DeadLetter settles the reservation)", handled, l.released, l.acked, l.queue)
				}
			})
		}
	}
}

// Every body the SDK cannot hand to a handler is flagged Poison, an identity-less one included
// (ADR-0036 decision 4, binding §1.1 "Identity-less body"): it decodes, but it has neither a valid job
// nor a valid urn, so it is no unknown URN either.
func TestLegacyMessageFlagsCodecRejectedAndIdentityLessBodiesAsPoison(t *testing.T) {
	tr := NewWithClient(goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"}))
	t.Cleanup(func() { _ = tr.Close() })
	tok := "0123456789abcdef0123456789abcdef"
	for body, want := range map[string]bool{
		`not-json`: true,
		`{"job":"urn:x","attempts":0,"meta":"x"}`: true,
		`{"job":"urn:x","attempts":0}`:            false,
		`{"urn":"urn:x","attempts":0}`:            false, // the inbound alias is an identity
		`{"attempts":0,"data":{}}`:                true,  // identity-less
		`{"job":"","urn":" ","attempts":0}`:       true,  // identity-less
		`null`:                                    true,
		"{\"job\":\"urn:x\",\"attempts\":0,\"data\":{\"n\":\"\xff\"}}": true, // decodable, invalid UTF-8
	} {
		if got := tr.legacyMessage("q", body, tok).Poison; got != want {
			t.Fatalf("legacyMessage(%q).Poison = %v, want %v", body, got, want)
		}
	}
}

// zsetAppTransport is legacyAppTransport for the Laravel (zset) layout: it drives a real App over
// the REAL delivery builder ([zsetMessage], which decides Poison) and records DeadLetter. Only the
// Redis calls are replaced; the reserved member is the head with a trailing space, a stand-in for the
// reserve script's re-encoded member (so it is not the script's own {job, job} poison answer).
type zsetAppTransport struct {
	queue      []string
	deliveries int
	acked      int
	released   int
	dlq        []string // "<dlq queue>|<body>"
}

func (z *zsetAppTransport) Publish(_ context.Context, queue, body string) error {
	if queue == "q" {
		z.queue = append(z.queue, body)
		return nil
	}
	z.dlq = append(z.dlq, queue+"|"+body)
	return nil
}

func (z *zsetAppTransport) Pop(context.Context, string, time.Duration) (*babelqueue.ReceivedMessage, error) {
	if len(z.queue) == 0 {
		return nil, nil
	}
	body := z.queue[0]
	z.queue = z.queue[1:]
	z.deliveries++
	return zsetMessage("q", body, body+" "), nil
}

func (z *zsetAppTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error {
	z.acked++
	return nil
}

func (z *zsetAppTransport) Release(context.Context, *babelqueue.ReceivedMessage, time.Duration) error {
	z.released++
	return babelqueue.ErrReleaseUnsupported
}

func (z *zsetAppTransport) DeadLetter(_ context.Context, _ *babelqueue.ReceivedMessage, dlq, body string) error {
	z.dlq = append(z.dlq, dlq+"|"+body)
	return nil
}

// ADR-0036 decision 4 on the Laravel (zset) layout: an identity-less body is dead-lettered as an
// ADR-0035 poison record on the first delivery in EVERY App configuration: with on_unknown_urn:
// delete and with dead-lettering off (the default), where it used to be dropped (delete) or retried
// and then dropped (DLQ off). The handler never runs and nothing is retried or released.
func TestZsetIdentityLessBodyIsDeadLetteredAsPoisonThroughApp(t *testing.T) {
	configs := map[string][]babelqueue.AppOption{
		"default (dead-letter off, fail)": nil,
		"strategy delete":                 {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete)},
		"strategy delete, dead-letter on": {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete), babelqueue.WithDeadLetter(true)},
		"strategy release":                {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease)},
		"strategy dead_letter":            {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDeadLetter)},
		"dead-letter on":                  {babelqueue.WithDeadLetter(true)},
		"max attempts 1":                  {babelqueue.WithMaxAttempts(1)},
	}
	bodies := []string{`{"attempts":0,"data":{}}`, `{"job":5,"attempts":0}`, `{"job":" ","urn":"","attempts":0}`, `{"urn":7,"attempts":0}`}
	for name, opts := range configs {
		for _, body := range bodies {
			t.Run(name+"/"+body, func(t *testing.T) {
				z := &zsetAppTransport{queue: []string{body}}
				app := babelqueue.NewApp(z, opts...)
				handled := 0
				app.Handle("urn:x", func(context.Context, babelqueue.Envelope) error { handled++; return nil })

				processed, err := app.Drain(context.Background(), "q", 50)
				if err != nil {
					t.Fatal(err)
				}
				if processed != 1 || z.deliveries != 1 {
					t.Fatalf("processed %d / deliveries %d, want exactly 1", processed, z.deliveries)
				}
				if len(z.dlq) != 1 {
					t.Fatalf("dlq = %q, want exactly one record", z.dlq)
				}
				record, ok := strings.CutPrefix(z.dlq[0], "q.dlq|")
				if !ok {
					t.Fatalf("dlq entry %q is not addressed to q.dlq", z.dlq[0])
				}
				assertPoisonRecord(t, record, body, "q")
				if handled != 0 || z.released != 0 || z.acked != 0 || len(z.queue) != 0 {
					t.Fatalf("handled %d released %d acked %d queue %q: want none (DeadLetter settles the reservation)", handled, z.released, z.acked, z.queue)
				}
			})
		}
	}
}

// A valid envelope whose URN has no handler is still an unknown URN, not poison: the strategies of
// error-handling.md section 3 keep applying to it (ADR-0036 decision 4).
func TestZsetValidEnvelopeWithUnknownURNIsNotPoison(t *testing.T) {
	z := &zsetAppTransport{queue: []string{`{"job":"urn:unmapped","attempts":0,"data":{}}`}}
	app := babelqueue.NewApp(z, babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete))
	if _, err := app.Drain(context.Background(), "q", 5); err != nil {
		t.Fatal(err)
	}
	if len(z.dlq) != 0 || z.acked != 1 {
		t.Fatalf("dlq %q acked %d: an unknown URN under delete must be acked and dropped", z.dlq, z.acked)
	}
}

// A framed body whose inner bytes are not valid UTF-8 is poison and must NOT be unframed: the
// frame's JSON decode would substitute U+FFFD, so the message body is the exact stored value.
func TestLegacyMessageFramedInvalidUTF8KeepsExactStoredBytes(t *testing.T) {
	tr := NewWithClient(goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"}))
	t.Cleanup(func() { _ = tr.Close() })
	tok := "0123456789abcdef0123456789abcdef"
	raw := "{\"__bq_frame\":1,\"body\":\"{\\\"job\\\":\\\"urn:x\\\",\\\"attempts\\\":0,\\\"data\\\":{\\\"n\\\":\\\"\xff\\\"}}\",\"headers\":{}}"
	msg := tr.legacyMessage("q", raw, tok)
	if !msg.Poison {
		t.Fatalf("Poison = false, want true")
	}
	if msg.Body != raw {
		t.Fatalf("Body = %q, want the exact stored bytes %q", msg.Body, raw)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("Headers = %v, want none", msg.Headers)
	}
	if strings.Contains(msg.Body, "\uFFFD") {
		t.Fatalf("Body contains U+FFFD: %q", msg.Body)
	}
}
