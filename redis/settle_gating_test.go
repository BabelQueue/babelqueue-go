package redis

import (
	"context"
	"strings"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// These tests are live (Redis required; skipped locally, enforced in CI with BQ_REQUIRE_REDIS=1).

// MAJOR 1: cjson.decode accepts 1e400 (inf) and NaN, cjson.encode refuses them. The reserve
// script must not fail after its LPOP: the message lands in :reserved byte for byte, is flagged
// Poison and is never lost.
func TestZsetReserveNeverLosesUnencodableBody(t *testing.T) {
	bodies := []string{
		`{"job":"urn:x","attempts":0,"data":{"n":1e400}}`,
		`{"job":"urn:x","attempts":0,"data":{"n":-1e400}}`,
		`{"job":"urn:x","attempts":0,"data":{"n":NaN}}`,
		`{"job":"urn:x","attempts":0,"data":{"n":Infinity}}`,
	}
	for _, body := range bodies {
		t.Run(body, func(t *testing.T) {
			f := newLive(t)
			tr := f.transport(WithLaravelLayout(""))
			if err := tr.Publish(f.ctx, f.queue, body); err != nil {
				t.Fatal(err)
			}
			msg, err := tr.reserveOnce(f.ctx, f.queue, 1000)
			if err != nil {
				t.Fatalf("reserve failed after the LPOP would have run: %v", err)
			}
			if msg == nil {
				t.Fatal("message lost: reserve returned nothing")
			}
			if msg.Body != body || !msg.Poison {
				t.Fatalf("Body=%q Poison=%v; want the raw body flagged poison", msg.Body, msg.Poison)
			}
			k := tr.Keys(f.queue)
			if n, _ := f.c.LLen(f.ctx, k.Ready).Result(); n != 0 {
				t.Fatalf("ready length = %d, want 0", n)
			}
			members, _ := f.c.ZRange(f.ctx, k.Reserved, 0, -1).Result()
			if len(members) != 1 || members[0] != body {
				t.Fatalf(":reserved = %q, want the raw body as the only member", members)
			}
			if err := tr.DeadLetter(f.ctx, msg, f.queue+".dlq", msg.Body); err != nil {
				t.Fatal(err)
			}
			dlq, _ := f.c.LRange(f.ctx, tr.Keys(f.queue+".dlq").Ready, 0, -1).Result()
			if len(dlq) != 1 || dlq[0] != body {
				t.Fatalf("dlq = %q, want the raw body", dlq)
			}
			if n, _ := f.c.ZCard(f.ctx, k.Reserved).Result(); n != 0 {
				t.Fatalf(":reserved residue after DeadLetter: %d", n)
			}
		})
	}
}

// MAJOR 2: a poison head the Lua flagged ({job, job}) is dead-lettered by the App itself as an ADR-0035 poison record,
// under the DEFAULT configuration (deadLetter=false, StrategyFail), including a body the core
// Decode accepts (attempts null / missing). The handler never runs and nothing is re-published
// to the source queue.
func TestZsetPoisonThroughAppGoesToDLQRaw(t *testing.T) {
	poison := []string{
		"not-json",
		`{"job":"urn:bq:orders:created","attempts":"3"}`,
		`{"job":"urn:bq:orders:created","attempts":null,"data":{"id":1}}`,
		`{"job":"urn:bq:orders:created","data":{"id":2}}`,
		// ADR-0036 decision 4: identity-less bodies are poison too, under delete and with
		// dead-lettering off (the default here), where they used to be dropped / retried.
		`{"attempts":0,"data":{"id":3}}`,
		`{"job":5,"urn":" ","attempts":0}`,
	}
	for _, strategy := range []string{babelqueue.StrategyFail, babelqueue.StrategyDelete, babelqueue.StrategyRelease} {
		t.Run(strategy, func(t *testing.T) {
			f := newLive(t)
			tr := f.transport(WithLaravelLayout(""))
			for _, p := range poison {
				if err := tr.Publish(f.ctx, f.queue, p); err != nil {
					t.Fatal(err)
				}
			}
			handled := 0
			app := babelqueue.NewApp(tr, babelqueue.WithUnknownURNStrategy(strategy))
			app.Handle("urn:bq:orders:created", func(context.Context, babelqueue.Envelope) error { handled++; return nil })
			if _, err := app.Drain(f.ctx, f.queue, len(poison)); err != nil {
				t.Fatal(err)
			}
			if handled != 0 {
				t.Fatalf("handler ran %d times for poison bodies", handled)
			}
			dlq, _ := f.c.LRange(f.ctx, tr.Keys(f.queue+".dlq").Ready, 0, -1).Result()
			if len(dlq) != len(poison) {
				t.Fatalf("dlq = %q, want %d poison records", dlq, len(poison))
			}
			for i := range poison {
				assertPoisonRecord(t, dlq[i], poison[i], f.queue)
			}
			k := tr.Keys(f.queue)
			if n, _ := f.c.LLen(f.ctx, k.Ready).Result(); n != 0 {
				t.Fatalf("source ready length = %d: a poison body was retried through the source queue", n)
			}
			if n, _ := f.c.ZCard(f.ctx, k.Delayed).Result(); n != 0 {
				t.Fatalf("source delayed = %d", n)
			}
			if n, _ := f.c.ZCard(f.ctx, k.Reserved).Result(); n != 0 {
				t.Fatalf(":reserved residue: %d", n)
			}
		})
	}
}

// MAJOR 3 (Laravel layout): DeadLetter is gated on the :reserved member. A's reservation expired
// and was re-reserved by B (a different member: attempts+1), so A's DeadLetter writes nothing.
func TestZsetDeadLetterIsGatedLateConsumer(t *testing.T) {
	f := newLive(t)
	body := `{"job":"urn:bq:orders:created","trace_id":"t","data":{"id":1},"meta":{"id":"i","queue":"orders","lang":"go","schema_version":1,"created_at":1},"attempts":0}`
	a := f.transport(WithLaravelLayout(""), WithRetryAfter(90*time.Second))
	b := f.transport(WithLaravelLayout(""), WithRetryAfter(90*time.Second))
	if err := a.Publish(f.ctx, f.queue, body); err != nil {
		t.Fatal(err)
	}
	first, err := a.reserveOnce(f.ctx, f.queue, 1000)
	if err != nil || first == nil {
		t.Fatalf("A reserve: %v %v", first, err)
	}
	second, err := b.reserveOnce(f.ctx, f.queue, 1091)
	if err != nil || second == nil {
		t.Fatalf("B reserve: %v %v", second, err)
	}
	dlq := f.queue + ".dlq"
	if err := a.DeadLetter(f.ctx, first, dlq, first.Body); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.c.LLen(f.ctx, a.Keys(dlq).Ready).Result(); n != 0 {
		t.Fatalf("late consumer wrote %d DLQ records; B still owns the message", n)
	}
	if n, _ := f.c.ZCard(f.ctx, a.Keys(f.queue).Reserved).Result(); n != 1 {
		t.Fatalf(":reserved = %d, want B's reservation intact", n)
	}
	if len(*f.warns) == 0 || !strings.Contains((*f.warns)[0], "reservation expired") {
		t.Fatalf("warnings = %v, want 'reservation expired'", *f.warns)
	}
	// The rightful owner dead-letters exactly once.
	if err := b.DeadLetter(f.ctx, second, dlq, second.Body); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.c.LLen(f.ctx, b.Keys(dlq).Ready).Result(); n != 1 {
		t.Fatalf("dlq length = %d, want 1", n)
	}
	if n, _ := f.c.LLen(f.ctx, b.Keys(dlq).Notify).Result(); n != 1 {
		t.Fatalf("dlq notify tokens = %d, want 1", n)
	}
	if n, _ := f.c.ZCard(f.ctx, b.Keys(f.queue).Reserved).Result(); n != 0 {
		t.Fatalf(":reserved residue: %d", n)
	}
}

// legacyLate sets up a legacy-layout delivery whose reservation expired: A reserved at t=1000,
// the sweep at t=1011 returned it, B reserved it.
func legacyLate(t *testing.T, f *liveFixture) (a, b *Transport, first, second *babelqueue.ReceivedMessage) {
	t.Helper()
	a = f.transport(WithRetryAfter(10 * time.Second))
	b = f.transport(WithRetryAfter(10 * time.Second))
	if err := a.Publish(f.ctx, f.queue, sweepBody); err != nil {
		t.Fatal(err)
	}
	first, err := a.reserveOnce(f.ctx, f.queue, 1000)
	if err != nil || first == nil {
		t.Fatalf("A reserve: %v %v", first, err)
	}
	second, err = b.reserveOnce(f.ctx, f.queue, 1011)
	if err != nil || second == nil {
		t.Fatalf("B reserve: %v %v", second, err)
	}
	return a, b, first, second
}

// MAJOR 3 (default layout): a late consumer's DeadLetter writes nothing and keeps B's copy.
func TestLegacyDeadLetterIsGatedLateConsumer(t *testing.T) {
	f := newLive(t)
	a, b, first, second := legacyLate(t, f)
	dlq := f.queue + ".dlq"
	if err := a.DeadLetter(f.ctx, first, dlq, first.Body); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.c.LLen(f.ctx, dlq).Result(); n != 0 {
		t.Fatalf("late consumer wrote %d DLQ records", n)
	}
	if n, _ := f.c.LLen(f.ctx, f.queue+":processing").Result(); n != 1 {
		t.Fatalf("processing = %d, want B's copy intact", n)
	}
	if len(*f.warns) == 0 || !strings.Contains((*f.warns)[0], "reservation expired") {
		t.Fatalf("warnings = %v", *f.warns)
	}
	if err := b.DeadLetter(f.ctx, second, dlq, second.Body); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.c.LLen(f.ctx, dlq).Result(); n != 1 {
		t.Fatalf("dlq = %d, want exactly 1", n)
	}
	if n, _ := f.c.LLen(f.ctx, f.queue+":processing").Result(); n != 0 {
		t.Fatalf("processing residue: %d", n)
	}
}

// MAJOR 3: a delayed (delay > 0) release in the default layout is a gated move, not an
// ErrReleaseUnsupported republish+ack. A late consumer's release re-queues nothing; the owner's
// release re-queues the message exactly once (the layout has no delayed set, so it is immediate).
func TestLegacyDelayedReleaseIsGated(t *testing.T) {
	f := newLive(t)
	a, b, first, second := legacyLate(t, f)
	if err := a.releaseAt(f.ctx, first, 5*time.Second, 1011.5); err != nil {
		t.Fatalf("late release: %v (must be a gated no-op, not ErrReleaseUnsupported)", err)
	}
	if n, _ := f.c.LLen(f.ctx, f.queue).Result(); n != 0 {
		t.Fatalf("late consumer re-queued %d copies", n)
	}
	if n, _ := f.c.LLen(f.ctx, f.queue+":processing").Result(); n != 1 {
		t.Fatalf("processing = %d, want B's copy intact", n)
	}
	if err := b.releaseAt(f.ctx, second, 5*time.Second, 1012); err != nil {
		t.Fatalf("owner release: %v", err)
	}
	if n, _ := f.c.LLen(f.ctx, f.queue).Result(); n != 1 {
		t.Fatalf("ready = %d, want exactly 1 re-queued copy", n)
	}
	if n, _ := f.c.LLen(f.ctx, f.queue+":processing").Result(); n != 0 {
		t.Fatalf("processing residue: %d", n)
	}
	if n, _ := f.c.ZCard(f.ctx, f.queue+":processing:deadlines").Result(); n != 0 {
		t.Fatalf("deadline residue: %d", n)
	}
}

// MAJOR 4 (acceptance A13/K1): two byte-identical copies reserved by two consumers. One consumer's
// ack removes exactly one copy from :processing and never the other consumer's deadline member.
func TestLegacyIdenticalCopiesAckIndependently(t *testing.T) {
	for _, ackFirst := range []string{"A", "B"} {
		t.Run("ack "+ackFirst+" first", func(t *testing.T) {
			f := newLive(t)
			a := f.transport(WithRetryAfter(10 * time.Second))
			b := f.transport(WithRetryAfter(10 * time.Second))
			for i := 0; i < 2; i++ {
				if err := a.Publish(f.ctx, f.queue, sweepBody); err != nil {
					t.Fatal(err)
				}
			}
			ma, err := a.reserveOnce(f.ctx, f.queue, 1000)
			if err != nil || ma == nil {
				t.Fatalf("A reserve: %v %v", ma, err)
			}
			mb, err := b.reserveOnce(f.ctx, f.queue, 1001)
			if err != nil || mb == nil {
				t.Fatalf("B reserve: %v %v", mb, err)
			}
			ha, hb := ma.Handle.(legacyHandle), mb.Handle.(legacyHandle)
			if ha.raw != hb.raw || ha.token == hb.token {
				t.Fatalf("want identical raw copies with distinct tokens: %q/%q %q/%q", ha.raw, hb.raw, ha.token, hb.token)
			}
			processing, deadlines := f.queue+":processing", f.queue+":processing:deadlines"
			if n, _ := f.c.LLen(f.ctx, processing).Result(); n != 2 {
				t.Fatalf("processing = %d, want both copies", n)
			}

			first, second := ma, mb
			firstT, secondT := a, b
			keep := hb
			if ackFirst == "B" {
				first, second, firstT, secondT, keep = mb, ma, b, a, ha
			}
			if err := firstT.Ack(f.ctx, first); err != nil {
				t.Fatal(err)
			}
			if n, _ := f.c.LLen(f.ctx, processing).Result(); n != 1 {
				t.Fatalf("processing after one ack = %d, want exactly one copy removed", n)
			}
			if n, _ := f.c.ZCard(f.ctx, deadlines).Result(); n != 1 {
				t.Fatalf("deadlines after one ack = %d, want 1", n)
			}
			if _, err := f.c.ZScore(f.ctx, deadlines, keep.token+":"+keep.raw).Result(); err != nil {
				t.Fatalf("the other consumer's deadline member was removed: %v", err)
			}
			if len(*f.warns) != 0 {
				t.Fatalf("unexpected warnings after a valid ack: %v", *f.warns)
			}
			if err := secondT.Ack(f.ctx, second); err != nil {
				t.Fatal(err)
			}
			if n, _ := f.c.LLen(f.ctx, processing).Result(); n != 0 {
				t.Fatalf("processing residue: %d", n)
			}
			if n, _ := f.c.ZCard(f.ctx, deadlines).Result(); n != 0 {
				t.Fatalf("deadline residue: %d", n)
			}
		})
	}
}

// MINOR 1 (zset layout): bodies the reserve script's cjson accepts but the core Decode rejects
// (string meta, string data, numeric trace_id, fractional attempts) are poison at reserve: with the
// DEFAULT App settings they land in the DLQ as ADR-0035 poison records, the handler never runs, and nothing is
// left in the source queue, :delayed or :reserved (no zero-envelope push + ack, no lost body).
func TestZsetCodecRejectedBodiesGoToDLQRaw(t *testing.T) {
	bodies := []string{
		`{"job":"urn:bq:orders:created","attempts":0,"meta":"x"}`,
		`{"job":"urn:bq:orders:created","attempts":0,"data":"s"}`,
		`{"job":"urn:bq:orders:created","attempts":0,"trace_id":5}`,
		`{"job":"urn:bq:orders:created","attempts":1.5}`,
	}
	f := newLive(t)
	tr := f.transport(WithLaravelLayout(""))
	for _, b := range bodies {
		if err := tr.Publish(f.ctx, f.queue, b); err != nil {
			t.Fatal(err)
		}
	}
	handled := 0
	app := babelqueue.NewApp(tr)
	app.Handle("urn:bq:orders:created", func(context.Context, babelqueue.Envelope) error { handled++; return nil })
	if _, err := app.Drain(f.ctx, f.queue, len(bodies)); err != nil {
		t.Fatal(err)
	}
	if handled != 0 {
		t.Fatalf("handler ran %d times", handled)
	}
	dlq, _ := f.c.LRange(f.ctx, tr.Keys(f.queue+".dlq").Ready, 0, -1).Result()
	if len(dlq) != len(bodies) {
		t.Fatalf("dlq = %q, want %d poison records", dlq, len(bodies))
	}
	for i := range bodies {
		assertPoisonRecord(t, dlq[i], bodies[i], f.queue)
	}
	k := tr.Keys(f.queue)
	if n, _ := f.c.LLen(f.ctx, k.Ready).Result(); n != 0 {
		t.Fatalf("source ready length = %d", n)
	}
	if n, _ := f.c.ZCard(f.ctx, k.Delayed).Result(); n != 0 {
		t.Fatalf("source delayed = %d", n)
	}
	if n, _ := f.c.ZCard(f.ctx, k.Reserved).Result(); n != 0 {
		t.Fatalf(":reserved residue: %d", n)
	}
}

// MAJOR (default legacy :processing layout): an undecodable body under the DEFAULT App settings
// (deadLetter=false, StrategyFail, maxAttempts 3) must terminate AND must not be lost (binding
// §1.1, ADR-0033): the transport flags it Poison, so the App delivers it exactly once and writes
// an ADR-0035 poison record to <queue>.dlq through the gated DeadLetter, even with dead-lettering off. Nothing
// stays in the queue, :processing or the deadline set. Before the original fix Release re-queued it
// verbatim and it looped forever; before the Poison flag it was dropped after 3 deliveries.
func TestLegacyUndecodableBodyIsBoundedThroughApp(t *testing.T) {
	// The identity-less body (ADR-0036 decision 4) takes the same route, also under delete.
	bodies := []string{`not-json`, `{"job":"urn:x","attempts":0,"meta":"x"}`, `{"attempts":0,"data":{"id":3}}`}
	for _, body := range bodies {
		for _, strategy := range []string{babelqueue.StrategyFail, babelqueue.StrategyDelete} {
			t.Run(strategy+"/"+body, func(t *testing.T) {
				f := newLive(t)
				tr := f.transport()
				if err := tr.Publish(f.ctx, f.queue, body); err != nil {
					t.Fatal(err)
				}
				app := babelqueue.NewApp(tr, babelqueue.WithUnknownURNStrategy(strategy))
				// Far above one delivery: an unbounded loop would consume the whole cap.
				processed, err := app.Drain(f.ctx, f.queue, 20)
				if err != nil {
					t.Fatal(err)
				}
				if processed != 1 {
					t.Fatalf("processed %d deliveries, want exactly 1 (poison goes straight to the DLQ)", processed)
				}
				dlq, _ := f.c.LRange(f.ctx, tr.Keys(f.queue+".dlq").Ready, 0, -1).Result()
				if len(dlq) != 1 {
					t.Fatalf("dlq = %q, want exactly one poison record (never silently dropped)", dlq)
				}
				assertPoisonRecord(t, dlq[0], body, f.queue)
				k := tr.Keys(f.queue)
				if n, _ := f.c.LLen(f.ctx, k.Ready).Result(); n != 0 {
					t.Fatalf("ready length = %d, want 0", n)
				}
				if n, _ := f.c.LLen(f.ctx, tr.processing(f.queue)).Result(); n != 0 {
					t.Fatalf(":processing residue: %d", n)
				}
				if n, _ := f.c.ZCard(f.ctx, tr.deadlines(f.queue)).Result(); n != 0 {
					t.Fatalf("deadline residue: %d", n)
				}
			})
		}
	}
}
