package babelqueue_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// gatedTransport hands the App one message and records every settlement path: plain Publish,
// Ack, Release and the optional DeadLetterer capability.
type gatedTransport struct {
	*babelqueue.InMemoryTransport
	next *babelqueue.ReceivedMessage

	published   []string // "queue|body"
	acks        int
	releases    int
	deadLetters []string // "queue|body"
	dlErr       error
}

func (g *gatedTransport) Pop(_ context.Context, _ string, _ time.Duration) (*babelqueue.ReceivedMessage, error) {
	m := g.next
	g.next = nil
	return m, nil
}

func (g *gatedTransport) Publish(_ context.Context, queue, body string) error {
	g.published = append(g.published, queue+"|"+body)
	return nil
}

func (g *gatedTransport) Ack(context.Context, *babelqueue.ReceivedMessage) error {
	g.acks++
	return nil
}

func (g *gatedTransport) Release(context.Context, *babelqueue.ReceivedMessage, time.Duration) error {
	g.releases++
	return babelqueue.ErrReleaseUnsupported
}

// gatedDLTransport adds the DeadLetterer capability.
type gatedDLTransport struct{ *gatedTransport }

func (g gatedDLTransport) DeadLetter(_ context.Context, _ *babelqueue.ReceivedMessage, dlq, body string) error {
	if g.dlErr != nil {
		return g.dlErr
	}
	g.deadLetters = append(g.deadLetters, dlq+"|"+body)
	return nil
}

// A body the core Decode accepts (attempts: null) but the transport flagged as poison.
const decodableButPoison = `{"job":"urn:bq:orders:created","attempts":null,"data":{"n":1}}`

// A transport-flagged poison delivery goes to the DLQ under EVERY configuration (default
// deadLetter=false/StrategyFail included) as an ADR-0035 record (spliced or wrapped, with a
// dead_letter block), never reaches a handler, is never retried or re-published as a rebuilt
// envelope, and the reservation is settled exactly once.
func TestAppTransportPoisonIsDeadLetteredRawUnderEveryConfiguration(t *testing.T) {
	bodies := map[string]string{
		"decodable (attempts null)": decodableButPoison,
		"not json":                  "not-json \x00 {\"keep\":  \"me\"}",
		"invalid utf-8":             "{\"job\":\"urn:bq:orders:created\"} \xff\xfe",
	}
	configs := map[string][]babelqueue.AppOption{
		"defaults":      nil,
		"fail strategy": {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyFail)},
		"delete":        {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyDelete)},
		"release":       {babelqueue.WithUnknownURNStrategy(babelqueue.StrategyRelease), babelqueue.WithDeadLetter(true)},
	}
	for bn, body := range bodies {
		for cn, cfg := range configs {
			for _, gated := range []bool{false, true} {
				name := bn + "/" + cn + "/republish+ack"
				if gated {
					name = bn + "/" + cn + "/DeadLetterer"
				}
				t.Run(name, func(t *testing.T) {
					base := &gatedTransport{
						InMemoryTransport: babelqueue.NewInMemoryTransport(),
						next:              &babelqueue.ReceivedMessage{Body: body, Queue: "orders", Poison: true},
					}
					var tr babelqueue.Transport = base
					if gated {
						tr = gatedDLTransport{base}
					}
					handled := 0
					app := babelqueue.NewApp(tr, append([]babelqueue.AppOption{babelqueue.WithMaxAttempts(5)}, cfg...)...)
					app.Handle("urn:bq:orders:created", func(context.Context, babelqueue.Envelope) error { handled++; return nil })
					if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
						t.Fatal(err)
					}
					if handled != 0 || base.releases != 0 {
						t.Fatalf("handled=%d releases=%d; a poison delivery must never be handled or retried", handled, base.releases)
					}
					if gated {
						if len(base.deadLetters) != 1 || len(base.published) != 0 || base.acks != 0 {
							t.Fatalf("deadLetters=%q published=%q acks=%d; want one gated DLQ write and nothing else",
								base.deadLetters, base.published, base.acks)
						}
						assertPoisonRecord(t, base.deadLetters[0], body)
						return
					}
					if len(base.published) != 1 || base.acks != 1 {
						t.Fatalf("published=%q acks=%d; want exactly one DLQ publish then one ack", base.published, base.acks)
					}
					assertPoisonRecord(t, base.published[0], body)
				})
			}
		}
	}
}

// A failed poison DLQ write never acknowledges or silently drops the message (DeadLetterer or not).
func TestAppTransportPoisonDeadLetterFailureIsNotSettled(t *testing.T) {
	base := &gatedTransport{
		InMemoryTransport: babelqueue.NewInMemoryTransport(),
		next:              &babelqueue.ReceivedMessage{Body: decodableButPoison, Queue: "orders", Poison: true},
		dlErr:             errors.New("dlq down"),
	}
	var hookErrs []error
	app := babelqueue.NewApp(gatedDLTransport{base},
		babelqueue.WithDeadLetterErrorHandler(func(_ context.Context, _ *babelqueue.ReceivedMessage, err error) {
			hookErrs = append(hookErrs, err)
		}))
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if base.acks != 0 || len(base.published) != 0 || len(base.deadLetters) != 0 {
		t.Fatalf("acks=%d published=%q deadLetters=%q; a failed DLQ write must leave the message unsettled",
			base.acks, base.published, base.deadLetters)
	}
	if len(hookErrs) != 1 {
		t.Fatalf("hook calls = %d, want 1", len(hookErrs))
	}
}

// Every App-initiated DLQ write of an ordinary failure also goes through the DeadLetterer
// capability when present: nothing is published and nothing is acked on top of it.
func TestAppUsesDeadLettererForExhaustedRetries(t *testing.T) {
	env, err := babelqueue.Make("urn:bq:orders:created", map[string]any{"n": 1}, babelqueue.WithQueue("orders"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := env.Encode()
	base := &gatedTransport{
		InMemoryTransport: babelqueue.NewInMemoryTransport(),
		next:              &babelqueue.ReceivedMessage{Body: string(raw), Queue: "orders"},
	}
	app := babelqueue.NewApp(gatedDLTransport{base}, babelqueue.WithMaxAttempts(1), babelqueue.WithDeadLetter(true))
	app.Handle("urn:bq:orders:created", func(context.Context, babelqueue.Envelope) error { return errors.New("boom") })
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(base.deadLetters) != 1 || len(base.published) != 0 || base.acks != 0 {
		t.Fatalf("deadLetters=%d published=%d acks=%d; want one gated write and no publish/ack", len(base.deadLetters), len(base.published), base.acks)
	}
}

// A version-gated route and an undecodable (non-flagged) body use the capability as well.
func TestAppUsesDeadLettererForGatedAndUndecodable(t *testing.T) {
	const gateRoute = "babelqueue:unsupported-schema-version:2?bq-job=urn%3Ababel%3Aorders%3Acreated"
	for name, msg := range map[string]*babelqueue.ReceivedMessage{
		"version gated": {Body: `{"job":"x"}`, Queue: "orders", RouteURN: gateRoute},
		"undecodable":   {Body: "not-json", Queue: "orders"},
	} {
		t.Run(name, func(t *testing.T) {
			base := &gatedTransport{InMemoryTransport: babelqueue.NewInMemoryTransport(), next: msg}
			app := babelqueue.NewApp(gatedDLTransport{base}, babelqueue.WithDeadLetter(true))
			if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
				t.Fatal(err)
			}
			if len(base.deadLetters) != 1 || len(base.published) != 0 || base.acks != 0 {
				t.Fatalf("deadLetters=%d published=%d acks=%d", len(base.deadLetters), len(base.published), base.acks)
			}
		})
	}
}

// assertPoisonRecord checks the "<queue>|<record>" the fake transport captured: the record is the
// ADR-0035 shape of body (spliced when it is a strict JSON object, otherwise a "raw" wrapper with
// base64 for invalid UTF-8) and carries a dead_letter block with reason "poison" and the source queue.
func assertPoisonRecord(t *testing.T, captured, body string) {
	t.Helper()
	record, ok := strings.CutPrefix(captured, "orders.dlq|")
	if !ok {
		t.Fatalf("captured %q is not addressed to orders.dlq", captured)
	}
	var got struct {
		Raw         *string `json:"raw"`
		RawEncoding *string `json:"raw_encoding"`
		DeadLetter  struct {
			Reason        string `json:"reason"`
			OriginalQueue string `json:"original_queue"`
		} `json:"dead_letter"`
	}
	if err := json.Unmarshal([]byte(record), &got); err != nil {
		t.Fatalf("record %q is not valid JSON: %v", record, err)
	}
	if got.DeadLetter.Reason != "poison" || got.DeadLetter.OriginalQueue != "orders" {
		t.Fatalf("dead_letter = %+v; want reason poison, original_queue orders (record %q)", got.DeadLetter, record)
	}
	switch {
	case !utf8.ValidString(body):
		if got.Raw == nil || got.RawEncoding == nil || *got.RawEncoding != "base64" {
			t.Fatalf("record %q: want raw + raw_encoding base64 for invalid UTF-8", record)
		}
		decoded, err := base64.StdEncoding.DecodeString(*got.Raw)
		if err != nil || string(decoded) != body {
			t.Fatalf("record %q: raw does not decode to the exact body bytes (%v)", record, err)
		}
	case isSpliceableObject(body):
		if got.Raw != nil || !strings.HasPrefix(record, strings.TrimSuffix(body, "}")) {
			t.Fatalf("record %q: want the body spliced byte for byte", record)
		}
	default:
		if got.Raw == nil || *got.Raw != body || got.RawEncoding != nil {
			t.Fatalf("record %q: want a plain raw wrapper of the body", record)
		}
	}
}

// isSpliceableObject reports whether body is a strict JSON object without a top-level dead_letter key.
func isSpliceableObject(body string) bool {
	var members map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &members) != nil {
		return false
	}
	_, hasDeadLetter := members["dead_letter"]
	return !hasDeadLetter
}
