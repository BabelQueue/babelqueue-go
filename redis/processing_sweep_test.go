package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// liveFixture is a Transport on a live Redis with a unique queue name; its keys are deleted
// before and after (never FLUSHDB: the broker may be shared). Without a broker the test skips, or
// fails under BQ_REQUIRE_REDIS.
type liveFixture struct {
	t     *testing.T
	ctx   context.Context
	queue string
	c     *goredis.Client
	warns *[]string
}

func newLive(t *testing.T) *liveFixture {
	t.Helper()
	client := connectForCase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	f := &liveFixture{t: t, ctx: ctx, queue: fmt.Sprintf("bqlive%d", time.Now().UnixNano()), c: client, warns: &[]string{}}
	f.del()
	t.Cleanup(func() { f.del(); _ = client.Close() })
	return f
}

func (f *liveFixture) del() {
	lk := LaravelKeys(DefaultKeyPrefix, f.queue)
	dk := LaravelKeys(DefaultKeyPrefix, f.queue+".dlq")
	keys := []string{f.queue, f.queue + ":processing", f.queue + ":processing:deadlines", f.queue + ".dlq",
		lk.Ready, lk.Delayed, lk.Reserved, lk.Notify, dk.Ready, dk.Delayed, dk.Reserved, dk.Notify}
	if err := f.c.Del(context.Background(), keys...).Err(); err != nil {
		f.t.Fatalf("cleanup: %v", err)
	}
}

// transport builds a consumer/producer on the fixture's queue; pass WithLaravelLayout("") for the
// zset layout.
func (f *liveFixture) transport(opts ...Option) *Transport {
	tr := NewWithClient(f.c, opts...)
	warns := f.warns
	tr.warnFn = func(msg string, args ...any) { *warns = append(*warns, msg) }
	return tr
}

const sweepBody = `{"job": "urn:bq:orders:created",  "attempts": 2, "data": {"id": 1}}`

func TestReservationTokenFormat(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 500; i++ {
		tok, err := newToken()
		if err != nil {
			t.Fatal(err)
		}
		if !hexToken.MatchString(tok) {
			t.Fatalf("token %q is not 32 lowercase hex digits", tok)
		}
		if seen[tok] {
			t.Fatalf("token %q repeated", tok)
		}
		seen[tok] = true
	}
}

// TestTokenSourceIsCryptoRand locks the rule that tokens come from crypto/rand only: the
// transport source must not import math/rand (or math/rand/v2).
func TestTokenSourceIsCryptoRand(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "redis.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	var hasCrypto bool
	for _, imp := range f.Imports {
		switch strings.Trim(imp.Path.Value, `"`) {
		case "math/rand", "math/rand/v2":
			t.Fatalf("redis.go imports %s; reservation tokens must come from crypto/rand only", imp.Path.Value)
		case "crypto/rand":
			hasCrypto = true
		}
	}
	if !hasCrypto {
		t.Fatal("redis.go does not import crypto/rand")
	}
}

// TestLegacyKill9 simulates a consumer that dies after reserving: a second consumer's sweep puts
// the item back byte-identically with attempts unchanged, and an item that never had a deadline
// member stays in :processing.
func TestLegacyKill9(t *testing.T) {
	f := newLive(t)
	dead := f.transport(WithRetryAfter(10 * time.Second))
	if err := dead.Publish(f.ctx, f.queue, sweepBody); err != nil {
		t.Fatal(err)
	}
	if _, err := dead.reserveOnce(f.ctx, f.queue, 1000); err != nil { // "kill -9": never acked
		t.Fatal(err)
	}
	const deadlineLess = `{"job": "urn:bq:orders:created", "attempts": 0}`
	if err := f.c.RPush(f.ctx, f.queue+":processing", deadlineLess).Err(); err != nil {
		t.Fatal(err)
	}

	survivor := f.transport(WithRetryAfter(10 * time.Second))
	msg, err := survivor.reserveOnce(f.ctx, f.queue, 1011)
	if err != nil || msg == nil {
		t.Fatalf("reserve after expiry: msg=%v err=%v", msg, err)
	}
	if msg.Body != sweepBody {
		t.Fatalf("body changed by recovery: %q", msg.Body)
	}
	var head struct {
		Attempts int `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(msg.Body), &head); err != nil || head.Attempts != 2 {
		t.Fatalf("attempts changed by recovery: %d (%v)", head.Attempts, err)
	}
	items, _ := f.c.LRange(f.ctx, f.queue+":processing", 0, -1).Result()
	if len(items) != 2 || items[0] != deadlineLess || items[1] != sweepBody {
		t.Fatalf("processing = %q; the deadline-less item must stay untouched", items)
	}
	if err := survivor.Ack(f.ctx, msg); err != nil {
		t.Fatal(err)
	}
	items, _ = f.c.LRange(f.ctx, f.queue+":processing", 0, -1).Result()
	if len(items) != 1 || items[0] != deadlineLess {
		t.Fatalf("processing after ack = %q", items)
	}
}

// TestLegacyNullRetryAfter: a retry_after:null consumer writes +inf deadlines, still sweeps, and
// leaves no residue (nor a warning) after its own ack.
func TestLegacyNullRetryAfter(t *testing.T) {
	f := newLive(t)
	tr := f.transport(WithoutRetryAfter())
	if err := tr.Publish(f.ctx, f.queue, sweepBody); err != nil {
		t.Fatal(err)
	}
	// A finite, expired member written by another consumer must be moved by this null consumer's sweep.
	const other = `{"job": "urn:bq:other", "attempts": 0}`
	if err := f.c.RPush(f.ctx, f.queue+":processing", other).Err(); err != nil {
		t.Fatal(err)
	}
	if err := f.c.ZAdd(f.ctx, f.queue+":processing:deadlines", goredis.Z{Score: 5, Member: strings.Repeat("a", 32) + ":" + other}).Err(); err != nil {
		t.Fatal(err)
	}
	msg, err := tr.reserveOnce(f.ctx, f.queue, 1000)
	if err != nil || msg == nil {
		t.Fatalf("reserve: %v %v", msg, err)
	}
	// The sweep ran (the expired foreign member was recovered to the ready list)...
	if n, _ := f.c.LLen(f.ctx, f.queue).Result(); n != 1 {
		t.Fatalf("ready length = %d, want the swept foreign item", n)
	}
	// ... and the new reservation is +inf.
	zs, _ := f.c.ZRangeWithScores(f.ctx, f.queue+":processing:deadlines", 0, -1).Result()
	if len(zs) != 1 || zs[0].Score < 1e300 {
		t.Fatalf("deadlines = %v, want one +inf member", zs)
	}
	if err := tr.Ack(f.ctx, msg); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.c.ZCard(f.ctx, f.queue+":processing:deadlines").Result(); n != 0 {
		t.Fatalf("deadlines left: %d", n)
	}
	for _, w := range *f.warns {
		if strings.Contains(w, "reservation expired") {
			t.Fatalf("unexpected warning %q", w)
		}
	}
}

// TestLegacyLateAckKeepsOtherCopy: an ack whose reservation was swept and re-reserved by another
// consumer touches nothing and warns.
func TestLegacyLateAckKeepsOtherCopy(t *testing.T) {
	f := newLive(t)
	a := f.transport(WithRetryAfter(10 * time.Second))
	b := f.transport(WithRetryAfter(10 * time.Second))
	if err := a.Publish(f.ctx, f.queue, sweepBody); err != nil {
		t.Fatal(err)
	}
	first, _ := a.reserveOnce(f.ctx, f.queue, 1000)
	second, err := b.reserveOnce(f.ctx, f.queue, 1011)
	if err != nil || second == nil {
		t.Fatalf("second reserve: %v %v", second, err)
	}
	if err := a.Ack(f.ctx, first); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.c.LLen(f.ctx, f.queue+":processing").Result(); n != 1 {
		t.Fatalf("late ack removed the other consumer's copy (processing=%d)", n)
	}
	if len(*f.warns) == 0 || !strings.Contains((*f.warns)[0], "reservation expired") {
		t.Fatalf("warnings = %v, want a 'reservation expired' warning", *f.warns)
	}
}

// TestZsetRecovery: an unacked reservation is migrated back after retry_after and the second
// consumer sees attempts+1.
func TestZsetRecovery(t *testing.T) {
	f := newLive(t)
	body := `{"job":"urn:bq:orders:created","trace_id":"t","data":{"id":1},"meta":{"id":"i","queue":"orders","lang":"go","schema_version":1,"created_at":1},"attempts":0}`
	first := f.transport(WithLaravelLayout(""), WithRetryAfter(90*time.Second))
	if err := first.Publish(f.ctx, f.queue, body); err != nil {
		t.Fatal(err)
	}
	m1, err := first.reserveOnce(f.ctx, f.queue, 1000)
	if err != nil || m1 == nil || m1.DeliveryCount != 1 {
		t.Fatalf("first reserve: %+v %v", m1, err)
	}
	second := f.transport(WithLaravelLayout(""), WithRetryAfter(90*time.Second))
	if m, _ := second.reserveOnce(f.ctx, f.queue, 1089); m != nil {
		t.Fatalf("delivered before retry_after elapsed: %+v", m)
	}
	m2, err := second.reserveOnce(f.ctx, f.queue, 1091)
	if err != nil || m2 == nil {
		t.Fatalf("recovered reserve: %+v %v", m2, err)
	}
	if m2.DeliveryCount != 2 {
		t.Fatalf("DeliveryCount = %d, want 2", m2.DeliveryCount)
	}
	if err := second.Ack(f.ctx, m2); err != nil {
		t.Fatal(err)
	}
	k := second.Keys(f.queue)
	if n, _ := f.c.ZCard(f.ctx, k.Reserved).Result(); n != 0 {
		t.Fatalf(":reserved residue after ack: %d", n)
	}
}

// TestZsetPoisonGoesToDLQ (A1): an undecodable body and a string attempts are both reserved raw,
// can be dead-lettered and acked, and nothing is lost or left in :reserved.
func TestZsetPoisonGoesToDLQ(t *testing.T) {
	f := newLive(t)
	tr := f.transport(WithLaravelLayout(""))
	poison := []string{"not-json", `{"job":"urn:bq:orders:created","attempts":"3"}`}
	for _, p := range poison {
		if err := tr.Publish(f.ctx, f.queue, p); err != nil {
			t.Fatal(err)
		}
	}
	dlq := f.queue + ".dlq"
	for range poison {
		msg, err := tr.reserveOnce(f.ctx, f.queue, 1000)
		if err != nil || msg == nil {
			t.Fatalf("reserve: %v %v", msg, err)
		}
		if h := msg.Handle.(zsetHandle); h.job != h.reserved {
			t.Fatalf("poison head was re-encoded: %q", h.reserved)
		}
		if err := tr.Publish(f.ctx, dlq, msg.Body); err != nil {
			t.Fatal(err)
		}
		if err := tr.Ack(f.ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := f.c.LRange(f.ctx, tr.Keys(dlq).Ready, 0, -1).Result()
	if len(got) != 2 || got[0] != poison[0] || got[1] != poison[1] {
		t.Fatalf("dlq = %q, want both poison bodies untouched", got)
	}
	if n, _ := f.c.ZCard(f.ctx, tr.Keys(f.queue).Reserved).Result(); n != 0 {
		t.Fatalf(":reserved residue: %d", n)
	}
	// The dead-letter publish above created notify tokens on the DLQ; clean them with the queue.
	_ = f.c.Del(f.ctx, tr.Keys(dlq).Notify, tr.Keys(dlq).Ready).Err()
}

// TestZsetReleasePreservesUnknownKeys (A2): release moves the reservation to :delayed in one
// step, only `attempts` changes, unknown keys keep value and type, and the score is
// floor(now)+ceil(delay).
func TestZsetReleasePreservesUnknownKeys(t *testing.T) {
	f := newLive(t)
	body := `{"job":"urn:bq:orders:created","trace_id":"t","data":{"id":1,"e":{},"l":[],"f":1.0},"meta":{"id":"i","queue":"orders","lang":"php","schema_version":1,"created_at":1,"x_meta":{"k":[1,2]}},"attempts":0,"x_top":{"empty":{},"list":[]}}`
	tr := f.transport(WithLaravelLayout(""))
	if err := tr.Publish(f.ctx, f.queue, body); err != nil {
		t.Fatal(err)
	}
	msg, err := tr.reserveOnce(f.ctx, f.queue, 1000.3)
	if err != nil || msg == nil {
		t.Fatalf("reserve: %v %v", msg, err)
	}
	if err := tr.releaseAt(f.ctx, msg, 2300*time.Millisecond, 1000.3); err != nil {
		t.Fatal(err)
	}
	k := tr.Keys(f.queue)
	if n, _ := f.c.ZCard(f.ctx, k.Reserved).Result(); n != 0 {
		t.Fatalf(":reserved residue: %d", n)
	}
	zs, _ := f.c.ZRangeWithScores(f.ctx, k.Delayed, 0, -1).Result()
	if len(zs) != 1 || zs[0].Score != 1003 {
		t.Fatalf("delayed = %v, want one member scored 1003", zs)
	}
	got, _ := zs[0].Member.(string)
	var gotTop, wantTop map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got), &gotTop); err != nil {
		t.Fatalf("released body: %v", err)
	}
	_ = json.Unmarshal([]byte(body), &wantTop)
	if string(gotTop["attempts"]) != "1" {
		t.Fatalf("attempts = %s, want 1", gotTop["attempts"])
	}
	for _, key := range []string{"x_top", "data", "meta", "job", "trace_id"} {
		if !jsonEqual(gotTop[key], wantTop[key]) || (key == "x_top" && !bytes.Contains(gotTop[key], []byte(`"empty":{}`))) {
			t.Errorf("key %q changed: got %s want %s", key, gotTop[key], wantTop[key])
		}
	}
}

// TestZsetNoFrame (A3): in the Laravel layout PublishWithHeaders drops the headers and stores the
// bare envelope, and the pop script reserves it with attempts+1.
func TestZsetNoFrame(t *testing.T) {
	f := newLive(t)
	body := `{"job":"urn:bq:orders:created","trace_id":"t","data":{},"meta":{"id":"i","queue":"orders","lang":"go","schema_version":1,"created_at":1},"attempts":0}`
	tr := f.transport(WithLaravelLayout(""))
	if err := tr.PublishWithHeaders(f.ctx, f.queue, body, map[string]string{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}); err != nil {
		t.Fatal(err)
	}
	stored, _ := f.c.LRange(f.ctx, tr.Keys(f.queue).Ready, 0, -1).Result()
	if len(stored) != 1 || stored[0] != body || strings.Contains(stored[0], "__bq_frame") {
		t.Fatalf("stored = %q, want the bare envelope", stored)
	}
	msg, err := tr.reserveOnce(f.ctx, f.queue, 1000)
	if err != nil || msg == nil || msg.DeliveryCount != 1 {
		t.Fatalf("reserve: %+v %v", msg, err)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("zset mode must not carry headers: %v", msg.Headers)
	}
	members, _ := f.c.ZRange(f.ctx, tr.Keys(f.queue).Reserved, 0, -1).Result()
	if len(members) != 1 || !strings.Contains(members[0], `"attempts":1`) {
		t.Fatalf("reserved = %q, want attempts 1", members)
	}
}

// TestZsetDelayedBlockingSlice covers the 2 s delay path and A4: a delayed message that falls due
// during a positive-timeout blocking Pop arrives within one wait slice of becoming due, long before
// the timeout.
func TestZsetDelayedBlockingSlice(t *testing.T) {
	f := newLive(t)
	tr := f.transport(WithLaravelLayout(""))
	body := `{"job":"urn:bq:orders:created","trace_id":"t","data":{},"meta":{"id":"i","queue":"orders","lang":"go","schema_version":1,"created_at":1},"attempts":0}`
	// start is taken BEFORE the publish clock is read, so it never lags the score's base time.
	start := time.Now()
	if err := tr.publishDelayedAt(f.ctx, f.queue, body, 2*time.Second, tr.nowFn()); err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(f.ctx, f.queue, 30*time.Second)
	if err != nil || msg == nil {
		t.Fatalf("pop: %v %v", msg, err)
	}
	// The score is floor(now)+2 with now read after start, so the message becomes due strictly more
	// than 1 s after start: the wait is always above 1.0 s (no flaky lower bound). The upper bound
	// proves the Pop did not sleep to its 30 s timeout.
	if el := time.Since(start); el < time.Second || el > 3500*time.Millisecond {
		t.Fatalf("delivered after %v, want within [1.0s, 3.5s]", el)
	}
	if err := tr.Ack(f.ctx, msg); err != nil {
		t.Fatal(err)
	}
}
