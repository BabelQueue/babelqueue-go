package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// The behavioural half of the conformance `redis_layout` section: every block other than the
// static `cases` is a small state machine ({state, steps[{op, now, expect}]}) that is replayed
// against a live Redis with a pinned clock. The runner needs a broker (REDIS_URL, default
// localhost:6379); without one each case skips, and fails when BQ_REQUIRE_REDIS is set.

var hexToken = regexp.MustCompile(`^[0-9a-f]{32}$`)

type layoutBlock struct {
	Description string       `json:"description"`
	Cases       []layoutCase `json:"cases"`
}

type layoutCase struct {
	Name               string       `json:"name"`
	Layout             string       `json:"layout"`
	Queue              string       `json:"queue"`
	RetryAfter         *float64     `json:"retry_after"`
	MigrationBatchSize *int         `json:"migration_batch_size"`
	State              layoutState  `json:"state"`
	Steps              []layoutStep `json:"steps"`
}

type scored struct {
	Member     string          `json:"member"`
	MemberJSON json.RawMessage `json:"member_json"`
	BodyJSON   json.RawMessage `json:"body_json"`
	Token      string          `json:"token"`
	Raw        string          `json:"raw"`
	Score      any             `json:"score"`
}

type layoutState struct {
	Ready           []string `json:"ready"`
	Processing      []string `json:"processing"`
	Notify          int      `json:"notify"`
	Delayed         []scored `json:"delayed"`
	Reserved        []scored `json:"reserved"`
	Deadlines       []scored `json:"deadlines"`
	DelayedGenerate *struct {
		Count  int    `json:"count"`
		Member string `json:"member"`
		Score  any    `json:"score"`
	} `json:"delayed_generate"`
}

type layoutStep struct {
	Op          string                     `json:"op"`
	Now         float64                    `json:"now"`
	As          string                     `json:"as"`
	Delay       float64                    `json:"delay"`
	Body        string                     `json:"body"`
	Reservation json.RawMessage            `json:"reservation"`
	Expect      map[string]json.RawMessage `json:"expect"`
}

func loadLayoutBlocks(t *testing.T) map[string]layoutBlock {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var top struct {
		RedisLayout map[string]json.RawMessage `json:"redis_layout"`
	}
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	blocks := map[string]layoutBlock{}
	for name, b := range top.RedisLayout {
		if name == "description" || name == "cases" {
			continue
		}
		var blk layoutBlock
		if err := json.Unmarshal(b, &blk); err != nil {
			t.Fatalf("redis_layout.%s: %v", name, err)
		}
		blocks[name] = blk
	}
	return blocks
}

func scoreOf(v any) float64 {
	switch s := v.(type) {
	case float64:
		return s
	case string:
		if s == "+inf" {
			return math.Inf(1)
		}
		f, _ := strconv.ParseFloat(s, 64)
		return f
	}
	return math.NaN()
}

func secondsToDuration(s float64) time.Duration {
	return time.Duration(math.Round(s * float64(time.Second)))
}

func runLayoutBlocks(t *testing.T) {
	blocks := loadLayoutBlocks(t)
	names := make([]string, 0, len(blocks))
	for n := range blocks {
		names = append(names, n)
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatal("redis_layout has no behavioural blocks")
	}
	for _, name := range names {
		blk := blocks[name]
		t.Run(name, func(t *testing.T) {
			for _, c := range blk.Cases {
				t.Run(c.Name, func(t *testing.T) { runLayoutCase(t, c) })
			}
		})
	}
}

type caseRun struct {
	t     *testing.T
	ctx   context.Context
	tr    *Transport
	c     layoutCase
	queue string
	keys  Keys // zset layout key names
	warns []string
	held  map[string]heldMessage
}

type heldMessage struct{ msg any }

var (
	probeOnce sync.Once
	probeErr  error
	probeURL  string
)

// redisURL returns the broker URL for live cases; the reachability probe runs once per process so
// an absent broker costs one short timeout, not one per case.
func redisURL() string {
	if u := os.Getenv("REDIS_URL"); u != "" {
		return u
	}
	return "redis://localhost:6379/0"
}

func connectForCase(t *testing.T) *goredis.Client {
	t.Helper()
	probeOnce.Do(func() {
		probeURL = redisURL()
		opts, err := goredis.ParseURL(probeURL)
		if err != nil {
			probeErr = err
			return
		}
		c := goredis.NewClient(opts)
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		probeErr = c.Ping(ctx).Err()
	})
	if probeErr != nil {
		skipOrFailNoRedis(t, "no redis broker at %s: %v", probeURL, probeErr)
	}
	opts, _ := goredis.ParseURL(probeURL)
	return goredis.NewClient(opts)
}

func runLayoutCase(t *testing.T, c layoutCase) {
	client := connectForCase(t)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queue := fmt.Sprintf("bqconf%d-%s", time.Now().UnixNano(), c.Queue)
	var opts []Option
	if c.Layout == "zset" {
		opts = append(opts, WithLaravelLayout(""))
	}
	if c.RetryAfter == nil {
		opts = append(opts, WithoutRetryAfter())
	} else {
		opts = append(opts, WithRetryAfter(secondsToDuration(*c.RetryAfter)))
	}
	if c.MigrationBatchSize != nil {
		opts = append(opts, WithMigrationBatchSize(*c.MigrationBatchSize))
	}
	tr := NewWithClient(client, opts...)
	r := &caseRun{t: t, ctx: ctx, tr: tr, c: c, queue: queue, held: map[string]heldMessage{}}
	tr.warnFn = func(msg string, args ...any) { r.warns = append(r.warns, msg) }
	if c.Layout == "zset" {
		r.keys = tr.Keys(queue)
	}
	r.cleanup()
	defer r.cleanup()
	r.seed()
	for i, s := range c.Steps {
		r.step(i, s)
	}
}

func (r *caseRun) allKeys() []string {
	q := r.queue
	keys := []string{q, q + ":processing", q + ":processing:deadlines", q + ".dlq"}
	lk := LaravelKeys(DefaultKeyPrefix, q)
	return append(keys, lk.Ready, lk.Delayed, lk.Reserved, lk.Notify, lk.Ready+".dlq")
}

// cleanup deletes only this case's own keys (never FLUSHDB: the broker may be shared).
func (r *caseRun) cleanup() {
	if err := r.tr.client.Del(context.Background(), r.allKeys()...).Err(); err != nil {
		r.t.Fatalf("cleanup: %v", err)
	}
}

func (r *caseRun) readyKey() string {
	if r.c.Layout == "zset" {
		return r.keys.Ready
	}
	return r.queue
}

func (r *caseRun) seed() {
	st := r.c.State
	rc := r.tr.client
	ctx := r.ctx
	must := func(err error) {
		r.t.Helper()
		if err != nil {
			r.t.Fatalf("seed: %v", err)
		}
	}
	for _, v := range st.Ready {
		must(rc.RPush(ctx, r.readyKey(), v).Err())
	}
	for _, v := range st.Processing {
		must(rc.RPush(ctx, r.queue+":processing", v).Err())
	}
	if r.c.Layout == "zset" {
		for i := 0; i < st.Notify; i++ {
			must(rc.RPush(ctx, r.keys.Notify, "1").Err())
		}
		for _, d := range st.Delayed {
			must(rc.ZAdd(ctx, r.keys.Delayed, goredis.Z{Score: scoreOf(d.Score), Member: d.Member}).Err())
		}
		for _, d := range st.Reserved {
			must(rc.ZAdd(ctx, r.keys.Reserved, goredis.Z{Score: scoreOf(d.Score), Member: d.Member}).Err())
		}
		if g := st.DelayedGenerate; g != nil {
			zs := make([]goredis.Z, 0, g.Count)
			for i := 0; i < g.Count; i++ {
				zs = append(zs, goredis.Z{Score: scoreOf(g.Score), Member: fmt.Sprintf(g.Member, i)})
			}
			must(rc.ZAdd(ctx, r.keys.Delayed, zs...).Err())
		}
	}
	for _, d := range st.Deadlines {
		member := d.Member
		if member == "" {
			member = d.Token + ":" + d.Raw
		}
		must(rc.ZAdd(ctx, r.queue+":processing:deadlines", goredis.Z{Score: scoreOf(d.Score), Member: member}).Err())
	}
}

func (r *caseRun) step(i int, s layoutStep) {
	t := r.t
	t.Helper()
	label := fmt.Sprintf("step %d (%s)", i, s.Op)
	var outcome, handlerBody string
	reported := -1
	switch s.Op {
	case "reserve":
		msg, err := r.tr.reserveOnce(r.ctx, r.queue, s.Now)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		switch {
		case msg == nil:
			outcome = "empty"
		default:
			outcome = "delivered"
			if h, ok := msg.Handle.(zsetHandle); ok && h.job == h.reserved {
				outcome = "poison"
			}
			handlerBody = msg.Body
			reported = msg.DeliveryCount
			if s.As != "" {
				r.held[s.As] = heldMessage{msg: msg}
			}
		}
	case "migrate":
		if err := r.tr.migrate(r.ctx, r.keys, s.Now); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	case "sweep":
		if err := r.tr.sweep(r.ctx, r.queue, s.Now); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	case "push_delayed":
		if err := r.tr.publishDelayedAt(r.ctx, r.queue, s.Body, secondsToDuration(s.Delay), s.Now); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	case "release", "ack", "dead_letter":
		msg := r.reservation(label, s)
		var err error
		switch s.Op {
		case "release":
			err = r.tr.releaseAt(r.ctx, msg, secondsToDuration(s.Delay), s.Now)
		case "ack":
			err = r.tr.Ack(r.ctx, msg)
		case "dead_letter":
			err = r.tr.DeadLetter(r.ctx, msg, r.queue+".dlq", msg.Body)
		}
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	default:
		t.Fatalf("%s: unknown op", label)
	}
	r.assert(label, s, outcome, handlerBody, reported)
}

func (r *caseRun) reservation(label string, s layoutStep) *babelqueue.ReceivedMessage {
	var name string
	if err := json.Unmarshal(s.Reservation, &name); err == nil {
		h, ok := r.held[name]
		if !ok {
			r.t.Fatalf("%s: no held reservation %q", label, name)
		}
		return h.msg.(*babelqueue.ReceivedMessage)
	}
	var lit struct {
		Token string `json:"token"`
		Raw   string `json:"raw"`
	}
	if err := json.Unmarshal(s.Reservation, &lit); err != nil {
		r.t.Fatalf("%s: bad reservation: %v", label, err)
	}
	return &babelqueue.ReceivedMessage{Body: lit.Raw, Queue: r.queue, Handle: legacyHandle{raw: lit.Raw, token: lit.Token}}
}

func (r *caseRun) assert(label string, s layoutStep, outcome, handlerBody string, reported int) {
	t := r.t
	t.Helper()
	rc := r.tr.client
	ctx := r.ctx
	for key, raw := range s.Expect {
		switch key {
		case "outcome":
			var want string
			mustUnmarshal(t, raw, &want)
			if outcome != want {
				t.Errorf("%s: outcome = %q, want %q", label, outcome, want)
			}
		case "handler_body":
			var want string
			mustUnmarshal(t, raw, &want)
			if handlerBody != want {
				t.Errorf("%s: handler_body = %q, want %q", label, handlerBody, want)
			}
		case "reported_attempts":
			var want int
			mustUnmarshal(t, raw, &want)
			if reported != want {
				t.Errorf("%s: reported_attempts = %d, want %d", label, reported, want)
			}
		case "ready":
			var want []string
			mustUnmarshal(t, raw, &want)
			got := lrange(t, rc, ctx, r.readyKey())
			assertList(t, label, "ready", got, want)
		case "processing":
			var want []string
			mustUnmarshal(t, raw, &want)
			got := lrange(t, rc, ctx, r.processingKey())
			assertList(t, label, "processing", got, want)
		case "ready_generate":
			var g struct {
				Count  int    `json:"count"`
				Member string `json:"member"`
			}
			mustUnmarshal(t, raw, &g)
			want := make([]string, g.Count)
			for i := range want {
				want[i] = fmt.Sprintf(g.Member, i)
			}
			assertList(t, label, "ready_generate", lrange(t, rc, ctx, r.readyKey()), want)
		case "notify":
			var want int64
			mustUnmarshal(t, raw, &want)
			n, err := rc.LLen(ctx, r.keys.Notify).Result()
			if err != nil || n != want {
				t.Errorf("%s: notify = %d (%v), want %d", label, n, err, want)
			}
		case "ready_count":
			var want int64
			mustUnmarshal(t, raw, &want)
			n, err := rc.LLen(ctx, r.readyKey()).Result()
			if err != nil || n != want {
				t.Errorf("%s: ready_count = %d (%v), want %d", label, n, err, want)
			}
		case "dlq_count":
			var want int64
			mustUnmarshal(t, raw, &want)
			n, err := rc.LLen(ctx, r.queue+".dlq").Result()
			if err != nil || n != want {
				t.Errorf("%s: dlq_count = %d (%v), want %d", label, n, err, want)
			}
		case "delayed":
			r.assertZset(label, "delayed", r.keys.Delayed, raw, false)
		case "reserved":
			r.assertZset(label, "reserved", r.keys.Reserved, raw, false)
		case "deadlines":
			r.assertZset(label, "deadlines", r.queue+":processing:deadlines", raw, true)
		case "tokens_distinct":
			var want bool
			mustUnmarshal(t, raw, &want)
			got := r.distinctTokens()
			if got != want {
				t.Errorf("%s: tokens_distinct = %v, want %v", label, got, want)
			}
		default:
			t.Errorf("%s: unsupported expect key %q", label, key)
		}
	}
}

func (r *caseRun) processingKey() string { return r.queue + ":processing" }

func (r *caseRun) distinctTokens() bool {
	members, err := r.tr.client.ZRange(r.ctx, r.queue+":processing:deadlines", 0, -1).Result()
	if err != nil {
		r.t.Fatalf("deadlines: %v", err)
	}
	seen := map[string]bool{}
	for _, m := range members {
		if len(m) < 34 {
			continue
		}
		tok := m[:32]
		if seen[tok] {
			return false
		}
		seen[tok] = true
	}
	return true
}

func lrange(t *testing.T, rc *goredis.Client, ctx context.Context, key string) []string {
	t.Helper()
	v, err := rc.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		t.Fatalf("lrange %s: %v", key, err)
	}
	return v
}

func assertList(t *testing.T, label, name string, got, want []string) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: %s = %q, want %q", label, name, got, want)
	}
}

func mustUnmarshal(t *testing.T, raw json.RawMessage, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("expect: %v", err)
	}
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

// assertZset compares a sorted set with the expected members. Order is by score then member (the
// Redis order); expected lists are written in that order.
func (r *caseRun) assertZset(label, name, key string, raw json.RawMessage, deadlines bool) {
	t := r.t
	t.Helper()
	var want []scored
	mustUnmarshal(t, raw, &want)
	zs, err := r.tr.client.ZRangeWithScores(r.ctx, key, 0, -1).Result()
	if err != nil {
		t.Fatalf("%s: zrange %s: %v", label, key, err)
	}
	if len(zs) != len(want) {
		t.Errorf("%s: %s has %d members, want %d (%v)", label, name, len(zs), len(want), zs)
		return
	}
	used := make([]bool, len(zs))
	for _, w := range want {
		matched := false
		for i, z := range zs {
			if used[i] || scoreOf(w.Score) != z.Score {
				continue
			}
			member, _ := z.Member.(string)
			if r.memberMatches(w, member, deadlines) {
				used[i], matched = true, true
				break
			}
		}
		if !matched {
			t.Errorf("%s: %s lacks %+v; has %v", label, name, w, zs)
		}
	}
}

func (r *caseRun) memberMatches(w scored, member string, deadlines bool) bool {
	switch {
	case deadlines && w.Token != "":
		if len(member) < 34 || member[32] != ':' {
			return false
		}
		tok := member[:32]
		if w.Token == "<any>" {
			if !hexToken.MatchString(tok) {
				return false
			}
		} else if tok != w.Token {
			return false
		}
		return member[33:] == w.Raw
	case len(w.MemberJSON) > 0:
		return jsonEqual([]byte(member), w.MemberJSON)
	case len(w.BodyJSON) > 0:
		return jsonEqual([]byte(member), w.BodyJSON)
	}
	return member == w.Member
}

// TestRedisDelayConformance locks the delay-score formula floor(now)+ceil(delay) (§1.6). It is
// pure: no broker.
func TestRedisDelayConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m struct {
		RedisDelay struct {
			Cases []struct {
				Name        string  `json:"name"`
				Now         float64 `json:"now"`
				Delay       float64 `json:"delay"`
				ExpectScore int64   `json:"expect_score"`
			} `json:"cases"`
		} `json:"redis_delay"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.RedisDelay.Cases) == 0 {
		t.Fatal("redis_delay has no cases")
	}
	for _, c := range m.RedisDelay.Cases {
		t.Run(c.Name, func(t *testing.T) {
			if got := delayScore(c.Now, secondsToDuration(c.Delay)); got != c.ExpectScore {
				t.Errorf("delayScore(%v, %vs) = %d, want %d", c.Now, c.Delay, got, c.ExpectScore)
			}
		})
	}
}
