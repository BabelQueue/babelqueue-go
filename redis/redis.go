// Package redis is a Redis-backed [babelqueue.Transport] for the BabelQueue Go
// runtime. It uses the reliable-queue pattern: RPUSH to produce, BLMOVE the head
// to a per-queue processing list to reserve (so an in-flight message survives a
// worker crash), and LREM from that list to acknowledge.
//
//	tr, _ := redis.New("redis://localhost:6379/0")
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// This default layout is a Go-owned reliable queue. For a *shared* queue with Laravel
// workers, [WithLaravelLayout] switches the key names and the reservation to the
// broker-bindings.md §1.1 layout (see "Laravel-compatible layout" below).
//
// It also implements the optional [babelqueue.HeaderPublisher] capability: out-of-band
// transport headers (e.g. a W3C traceparent for cross-hop span linkage, ADR-0028, or
// the bq-replay-bypass marker, ADR-0027) ride beside the frozen envelope (GR-1) — never
// in it — and are surfaced back to the consumer on [babelqueue.ReceivedMessage.Headers].
//
// Redis stores only the raw list value (the LREM ack handle *is* that value), so —
// unlike AMQP headers or SQS MessageAttributes — there is no native per-message
// metadata channel. To carry headers the transport owns a tiny JSON *frame* distinct
// from the wire envelope:
//
//	{"__bq_frame":1,"headers":{"traceparent":"00-..."},"body":"<raw wire envelope>"}
//
// RPUSH stores the frame, so the LREM ack handle stays byte-for-byte what was pushed
// and the reliable-queue semantics (RPUSH/BLMOVE/LREM/processing list) are untouched.
// Framing is opt-in and backward compatible: only PublishWithHeaders with a non-empty
// map writes a frame; plain Publish (and PublishWithHeaders with no headers) stores the
// bare envelope byte-for-byte, exactly as before. Pop detects frame-vs-bare by the
// reserved "__bq_frame" sentinel — a frozen wire envelope can never carry it — and a
// bare value consumes with Headers=nil and Handle=value, so cross-version queues
// interoperate.
//
// # Laravel-compatible layout
//
// [WithLaravelLayout] puts the transport on the key names of broker-bindings.md §1.1,
// `<prefix><name>` (ready list), `:delayed` and `:reserved` (sorted sets) and `:notify`
// (blocking-pop wake-up), with `queues:` as the Laravel default prefix. [Transport.Keys]
// returns them. Publish pushes to the ready list and signals the notify list.
//
// Pop first migrates due `:delayed` members and then expired `:reserved` members back to the
// ready list (score <= floor(now)), then runs Laravel's pop script: it LPOPs the head, decodes
// it with cjson (under pcall), increments `attempts`, writes the re-encoded member to
// `:reserved` scored floor(now)+retry_after and consumes one notify token. The handler receives
// the raw head byte for byte; the attempt count it sees is job.attempts+1 (DeliveryCount). Ack
// is ZREM of the `:reserved` member. A head that cjson cannot decode, or whose `attempts` is not
// a number, is written to `:reserved` raw and delivered as poison (A1): the App writes it to the
// DLQ and acknowledges it, so nothing is lost and nothing stays in `:reserved`.
//
// The re-encoded `:reserved` member is the documented Laravel compatibility exception (ADR-0033)
// to the lossless re-emit rule of ADR-0031: only the Laravel pop script touches the payload with
// cjson. The SDK itself always re-emits through its codec, which preserves unknown keys.
//
// [Transport.Release] (A2) moves the reservation to `:delayed` in one script — ZREM from
// `:reserved`, ZADD to `:delayed` at floor(now)+ceil(delay) with the SDK-encoded body
// (attempts+1, unknown keys preserved). [Transport.PublishDelayed] schedules a message the same
// way (no notify token: the migrate writes one per moved item). A reservation that is never
// acknowledged is recovered once its deadline passes and counts as an attempt; retry_after
// defaults to 90 s, [WithoutRetryAfter] disables recovery and a negative [WithRetryAfter] is a
// configuration error. Every blocking wait is sliced to at most one second with a migrate before
// each slice, so a delayed message that falls due while a consumer waits is delivered within one
// second.
//
// There is no out-of-band header channel in this layout (A3): PublishWithHeaders drops the
// headers and writes the bare envelope, so a traceparent is not carried through Redis lists
// here (trace_id stays in the envelope, GR-4). A "__bq_frame" value is never written.
//
// # Default (legacy) layout recovery
//
// The default `<queue>` + `<queue>:processing` layout keeps its names. Each reservation also
// writes a deadline member `<token>:<raw>` (token: 32 lowercase hex from crypto/rand) to the
// `<queue>:processing:deadlines` sorted set, scored floor(now)+retry_after (+inf when
// [WithoutRetryAfter]). A sweep before every reserve moves expired members back to the ready
// list (body and attempts unchanged); ack, release and dead-letter are gated on removing the
// reservation's own deadline member, so a late ack can never delete another consumer's copy.
// Items without a deadline member (reserved by an older version) are never touched.
package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	goredis "github.com/redis/go-redis/v9"
)

// frameVersion is the current header-frame schema version. It is the value of the
// reserved "__bq_frame" discriminator key and lets Pop tell a transport frame from a
// bare wire envelope without structural guessing.
const frameVersion = 1

// headerFrame is the transport-owned envelope-frame the Redis list value carries when
// out-of-band headers accompany a message. It is NOT the wire envelope (GR-1): Body is
// the raw, unchanged wire envelope string, and Headers travels beside it. The reserved
// "__bq_frame" key (which the frozen envelope never has) is the frame discriminator.
type headerFrame struct {
	Version int               `json:"__bq_frame"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body"`
}

// Transport implements babelqueue.Transport over Redis.
type Transport struct {
	client           *goredis.Client
	processingSuffix string

	// laravel switches to the broker-bindings.md §1.1 key layout; keyPrefix is its prefix.
	laravel    bool
	keyPrefix  string
	retryAfter time.Duration
	// retryAfterNull disables recovery of the zset layout's :reserved members and gives legacy
	// deadline members the score +inf (ADR-0033; the "retry_after: null" setting).
	retryAfterNull bool
	// batchSize is migration_batch_size: members moved per migrate / sweep call (-1 = all).
	batchSize int
	// cfgErr is a configuration error (e.g. a negative retry_after) reported by New and by
	// every operation of a Transport built with NewWithClient.
	cfgErr error

	// nowFn returns the current time as fractional epoch seconds; tests pin it.
	nowFn func() float64
	// warnFn is the warning sink (slog by default); tests capture it.
	warnFn func(msg string, args ...any)
}

// DefaultKeyPrefix is Laravel's default Redis queue key prefix (`queues:`).
const DefaultKeyPrefix = "queues:"

// DefaultRetryAfter is the reservation window (Laravel's `retry_after` default, 90 s): a
// reserved member whose deadline has passed is made ready again.
const DefaultRetryAfter = 90 * time.Second

// DefaultMigrationBatchSize means "move every due member" (-1), the Laravel behaviour.
const DefaultMigrationBatchSize = -1

// ErrDelayedUnsupported is returned by delayed publishing and release in the default layout,
// which has no delayed set; use [WithLaravelLayout].
var ErrDelayedUnsupported = errors.New("babelqueue/redis: delayed publish needs the Laravel-compatible layout")

// ErrInvalidRetryAfter reports a negative retry_after (a configuration error, §1.7).
var ErrInvalidRetryAfter = errors.New("babelqueue/redis: retry_after must not be negative")

// Keys are the Redis key names of one logical queue in the Laravel-compatible layout
// (broker-bindings.md §1.1).
type Keys struct {
	Ready    string // list
	Delayed  string // sorted set, scored by available-at
	Reserved string // sorted set, scored by the retry-after deadline
	Notify   string // list, blocking-pop wake-up
}

// LaravelKeys returns the §1.1 key names for queue under prefix.
func LaravelKeys(prefix, queue string) Keys {
	ready := prefix + queue
	return Keys{Ready: ready, Delayed: ready + ":delayed", Reserved: ready + ":reserved", Notify: ready + ":notify"}
}

// Option customizes New / NewWithClient.
type Option func(*Transport)

// WithProcessingSuffix overrides the per-queue processing-list suffix
// (default ":processing").
func WithProcessingSuffix(suffix string) Option {
	return func(t *Transport) { t.processingSuffix = suffix }
}

// WithLaravelLayout switches the transport to the Laravel-compatible key layout of
// broker-bindings.md §1.1 under prefix (empty means [DefaultKeyPrefix]). The default layout
// (a `<queue>` list plus a `<queue>:processing` list) is unchanged when this option is not used.
func WithLaravelLayout(prefix string) Option {
	return func(t *Transport) {
		if prefix == "" {
			prefix = DefaultKeyPrefix
		}
		t.laravel = true
		t.keyPrefix = prefix
	}
}

// WithRetryAfter sets the reservation window (default [DefaultRetryAfter]). In the Laravel
// layout an expired :reserved member is made ready again and counts as an attempt; in the
// default layout an expired deadline member is swept back to the ready list. A negative value
// is a configuration error ([ErrInvalidRetryAfter]); use [WithoutRetryAfter] to disable recovery.
func WithRetryAfter(d time.Duration) Option {
	return func(t *Transport) {
		if d < 0 {
			t.cfgErr = ErrInvalidRetryAfter
			return
		}
		t.retryAfter = d
		t.retryAfterNull = false
	}
}

// WithoutRetryAfter is the "retry_after: null" setting: this consumer never recovers expired
// :reserved members (Laravel layout) and its legacy deadline members score +inf, so it never
// sweeps its own reservations (it still sweeps other consumers' expired members).
func WithoutRetryAfter() Option {
	return func(t *Transport) { t.retryAfterNull = true }
}

// WithMigrationBatchSize bounds how many members one migrate (Laravel layout) or sweep
// (default layout) call moves; -1 (the default, [DefaultMigrationBatchSize]) moves them all.
// Zero or a value below -1 is ignored.
func WithMigrationBatchSize(n int) Option {
	return func(t *Transport) {
		if n == -1 || n > 0 {
			t.batchSize = n
		}
	}
}

// New connects to Redis from a redis:// or rediss:// URL.
func New(url string, opts ...Option) (*Transport, error) {
	options, err := goredis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	t := NewWithClient(goredis.NewClient(options), opts...)
	if t.cfgErr != nil {
		_ = t.client.Close()
		return nil, t.cfgErr
	}
	return t, nil
}

// NewWithClient wraps an existing go-redis client (bring your own pool/TLS/auth). A
// configuration error from an option (a negative retry_after) is reported by every operation.
func NewWithClient(client *goredis.Client, opts ...Option) *Transport {
	t := &Transport{
		client:           client,
		processingSuffix: ":processing",
		retryAfter:       DefaultRetryAfter,
		batchSize:        DefaultMigrationBatchSize,
		nowFn:            wallClock,
		warnFn:           func(msg string, args ...any) { slog.Warn(msg, args...) },
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func wallClock() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// delayScore is the one score formula of the contract (redis_delay): floor(now)+ceil(delay),
// in whole epoch seconds. It is also the reservation deadline floor(now)+retry_after.
func delayScore(now float64, delay time.Duration) int64 {
	if delay < 0 {
		delay = 0
	}
	secs := int64((delay + time.Second - 1) / time.Second)
	return int64(math.Floor(now)) + secs
}

func (t *Transport) processing(queue string) string {
	return queue + t.processingSuffix
}

func (t *Transport) deadlines(queue string) string {
	return t.processing(queue) + ":deadlines"
}

// deadlineArg is the score of a new reservation: floor(now)+retry_after, or +inf for null.
func (t *Transport) deadlineArg(now float64) string {
	if t.retryAfterNull {
		return "+inf"
	}
	return strconv.FormatInt(delayScore(now, t.retryAfter), 10)
}

// Keys returns the key names the transport uses for queue. In the Laravel layout
// ([WithLaravelLayout]) these are the §1.1 names; in the default layout Ready is the queue
// list itself, Reserved is its processing list, and Delayed and Notify are empty (unused).
func (t *Transport) Keys(queue string) Keys {
	if t.laravel {
		return LaravelKeys(t.keyPrefix, queue)
	}
	return Keys{Ready: queue, Reserved: t.processing(queue)}
}

// Atomic scripts. Only the Laravel pop script touches the payload (cjson).
var (
	// KEYS: ready, notify. ARGV: value.
	pushScript = goredis.NewScript(`
redis.call('rpush', KEYS[1], ARGV[1])
redis.call('rpush', KEYS[2], 1)
return 1`)

	// KEYS: source sorted set, ready, notify. ARGV: floor(now), batch (-1 = all). Moves members
	// whose score is <= floor(now), at most batch of them, one notify token per moved item.
	migrateScript = goredis.NewScript(`
local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if (next(val) ~= nil) then
  redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)
  for i = 1, #val, 100 do
    redis.call('rpush', KEYS[2], unpack(val, i, math.min(i + 99, #val)))
    for j = i, math.min(i + 99, #val) do
      redis.call('rpush', KEYS[3], 1)
    end
  end
end
return #val`)

	// KEYS: ready, reserved, notify. ARGV: deadline score. Laravel's pop script (§1.1): LPOP the
	// head; a head cjson cannot decode, or whose attempts is not a number, is written raw to
	// :reserved and returned as {job, job} (A1, poison); otherwise attempts+1 is re-encoded into
	// :reserved, one notify token is consumed and {job, reserved} is returned. Nothing after the
	// LPOP may raise (Lua has no rollback): cjson.decode accepts 1e400 and NaN, but cjson.encode
	// refuses to serialise them, so the encode is under pcall too and a body that cannot be
	// re-encoded takes the poison path with its raw bytes.
	reserveScript = goredis.NewScript(`
local job = redis.call('lpop', KEYS[1])
if not job then
  return false
end
local ok, payload = pcall(cjson.decode, job)
if ok and type(payload) == 'table' and type(payload['attempts']) == 'number' then
  payload['attempts'] = payload['attempts'] + 1
  local encoded, reserved = pcall(cjson.encode, payload)
  if encoded then
    redis.call('zadd', KEYS[2], ARGV[1], reserved)
    redis.call('lpop', KEYS[3])
    return {job, reserved}
  end
end
redis.call('zadd', KEYS[2], ARGV[1], job)
return {job, job}`)

	// KEYS: reserved, delayed. ARGV: reserved member, score, body. A2: leave :reserved and
	// enter :delayed in one step.
	releaseScript = goredis.NewScript(`
redis.call('zrem', KEYS[1], ARGV[1])
redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])
return 1`)

	// Laravel layout gated dead-letter. KEYS: reserved, dlq ready, dlq notify. ARGV: reserved
	// member, body. Only the consumer that still owns the :reserved member (ZREM returns 1)
	// writes the dead-letter record; an expired, migrated or re-reserved reservation writes
	// nothing.
	zsetDeadLetterScript = goredis.NewScript(`
if redis.call('zrem', KEYS[1], ARGV[1]) == 1 then
  redis.call('rpush', KEYS[2], ARGV[2])
  redis.call('rpush', KEYS[3], 1)
  return 1
end
return 0`)

	// Default (legacy) layout. KEYS: ready, processing, deadlines. ARGV: deadline score or +inf,
	// token. Reserves the head and records the deadline member <token>:<raw>.
	legacyReserveScript = goredis.NewScript(`
local raw = redis.call('lmove', KEYS[1], KEYS[2], 'LEFT', 'RIGHT')
if not raw then
  return false
end
redis.call('zadd', KEYS[3], ARGV[1], ARGV[2] .. ':' .. raw)
return raw`)

	// KEYS: ready, processing, deadlines. ARGV: floor(now), batch (-1 = all). Moves expired
	// reservations back to the ready list (body and attempts untouched). A member that is not
	// <32 byte token>:<raw> is only removed; an orphan (LREM finds nothing) is only removed.
	legacySweepScript = goredis.NewScript(`
local members = redis.call('zrangebyscore', KEYS[3], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local moved = 0
for _, m in ipairs(members) do
  if #m >= 34 and string.sub(m, 33, 33) == ':' then
    local raw = string.sub(m, 34)
    if redis.call('lrem', KEYS[2], 1, raw) == 1 then
      redis.call('rpush', KEYS[1], raw)
      moved = moved + 1
    end
  end
  redis.call('zrem', KEYS[3], m)
end
return moved`)

	// KEYS: processing, deadlines. ARGV: token, raw. Gated ack: only the holder of the
	// reservation's own deadline member removes (exactly one copy of) the processing item.
	legacyAckScript = goredis.NewScript(`
if redis.call('zrem', KEYS[2], ARGV[1] .. ':' .. ARGV[2]) == 1 then
  redis.call('lrem', KEYS[1], 1, ARGV[2])
  return 1
end
return 0`)

	// KEYS: processing, deadlines, destination list. ARGV: token, raw, body. Gated release /
	// dead-letter: after the gate, drop the processing item and push body to the destination.
	legacyMoveScript = goredis.NewScript(`
if redis.call('zrem', KEYS[2], ARGV[1] .. ':' .. ARGV[2]) == 1 then
  redis.call('lrem', KEYS[1], 1, ARGV[2])
  redis.call('rpush', KEYS[3], ARGV[3])
  return 1
end
return 0`)
)

// zsetHandle is the ack handle of a Laravel-layout reservation: the raw head (the handler's
// body) and the member that was written to :reserved (cjson re-encoded, or the raw head itself
// for a poison pop).
type zsetHandle struct{ job, reserved string }

// legacyHandle is the ack handle of a default-layout reservation: the stored list value and the
// token of its deadline member.
type legacyHandle struct{ raw, token string }

// newToken returns a reservation token: 128 bits from crypto/rand as 32 lowercase hex digits.
// math/rand must never be used here (tokens must be unguessable and collision-free).
func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (t *Transport) publishValue(ctx context.Context, queue, value string) error {
	if t.cfgErr != nil {
		return t.cfgErr
	}
	if !t.laravel {
		return t.client.RPush(ctx, queue, value).Err()
	}
	k := t.Keys(queue)
	return pushScript.Run(ctx, t.client, []string{k.Ready, k.Notify}, value).Err()
}

// waitSlice bounds one blocking wait: due delayed and expired reserved members are migrated
// before each slice, so a message that falls due during a wait is delivered within one slice
// (A4), whether the caller asked to wait indefinitely or for a positive timeout.
const waitSlice = time.Second

// migrate moves due :delayed members and then expired :reserved members (unless retry_after is
// null) to the ready list, in that order (§1.6, §1.7). now is fractional epoch seconds.
func (t *Transport) migrate(ctx context.Context, k Keys, now float64) error {
	floor := strconv.FormatInt(int64(math.Floor(now)), 10)
	batch := strconv.Itoa(t.batchSize)
	sources := []string{k.Delayed}
	if !t.retryAfterNull {
		sources = append(sources, k.Reserved)
	}
	for _, src := range sources {
		if err := migrateScript.Run(ctx, t.client, []string{src, k.Ready, k.Notify}, floor, batch).Err(); err != nil {
			return err
		}
	}
	return nil
}

// sweep moves expired default-layout reservations back to the ready list (§1.7). It runs
// before every reserve, whatever the retry_after setting.
func (t *Transport) sweep(ctx context.Context, queue string, now float64) error {
	floor := strconv.FormatInt(int64(math.Floor(now)), 10)
	keys := []string{queue, t.processing(queue), t.deadlines(queue)}
	return legacySweepScript.Run(ctx, t.client, keys, floor, strconv.Itoa(t.batchSize)).Err()
}

// reserveOnce is one non-blocking reservation attempt at the pinned clock now: migrate (Laravel
// layout) or sweep (default layout), then the layout's reserve script. It returns (nil, nil)
// when the queue is empty.
func (t *Transport) reserveOnce(ctx context.Context, queue string, now float64) (*babelqueue.ReceivedMessage, error) {
	if t.cfgErr != nil {
		return nil, t.cfgErr
	}
	if t.laravel {
		return t.reserveZset(ctx, queue, now)
	}
	return t.reserveLegacy(ctx, queue, now)
}

func (t *Transport) reserveZset(ctx context.Context, queue string, now float64) (*babelqueue.ReceivedMessage, error) {
	k := t.Keys(queue)
	if err := t.migrate(ctx, k, now); err != nil {
		return nil, err
	}
	res, err := reserveScript.Run(ctx, t.client, []string{k.Ready, k.Reserved, k.Notify}, t.deadlineArg(now)).Slice()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) != 2 {
		return nil, fmt.Errorf("babelqueue/redis: unexpected reserve reply %v", res)
	}
	job, _ := res[0].(string)
	reserved, _ := res[1].(string)
	return &babelqueue.ReceivedMessage{
		Body:          job,
		Queue:         queue,
		Handle:        zsetHandle{job: job, reserved: reserved},
		DeliveryCount: deliveryCount(job, reserved),
		Poison:        isPoisonHead(job, reserved),
	}, nil
}

// isPoisonHead reports whether a zset reservation is poison. Two sources, both dead-lettered by the
// App raw and gated, in every configuration:
//   - A1: the reserve script returned {job, job} for a head it could not decode or re-encode (cjson
//     failure, attempts not a number).
//   - The script accepted the head (cjson is more lenient than the SDK codec: a string "meta", a
//     numeric "trace_id", a fractional "attempts" ...) but the core Decode rejects it. No handler or
//     retry can ever use such a body, and Release could not re-encode it without destroying it
//     (there is no push + ack fallback in this layout), so it is poison, not a retry candidate.
//
// An identity-less body (ErrNoIdentity) is not poison here: it decodes, and the App decides.
func isPoisonHead(job, reserved string) bool {
	if job == reserved {
		return true
	}
	return !decodesForRelease(job)
}

// decodesForRelease is true when raw decodes into an envelope the SDK codec can re-encode
// (identity-less bodies included, which the App routes itself).
//
// It runs once per reservation and the App decodes the same body again right after: the extra
// parse is the price of classifying poison where the bytes are still raw. The cost is measured
// by BenchmarkDecodesForRelease and is small next to the Redis round trip of the reserve.
func decodesForRelease(raw string) bool {
	_, err := babelqueue.Decode([]byte(raw))
	return err == nil || errors.Is(err, babelqueue.ErrNoIdentity)
}

// deliveryCount is the attempt count of a zset reservation: the head's attempts plus one. A
// poison head (written to :reserved raw, job == reserved) was not incremented and counts as 1.
func deliveryCount(job, reserved string) int {
	if job == reserved {
		return 1
	}
	var head struct {
		Attempts float64 `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(job), &head); err != nil || head.Attempts < 0 {
		return 1
	}
	return int(head.Attempts) + 1
}

func (t *Transport) reserveLegacy(ctx context.Context, queue string, now float64) (*babelqueue.ReceivedMessage, error) {
	if err := t.sweep(ctx, queue, now); err != nil {
		return nil, err
	}
	token, err := newToken()
	if err != nil {
		return nil, err
	}
	keys := []string{queue, t.processing(queue), t.deadlines(queue)}
	raw, err := legacyReserveScript.Run(ctx, t.client, keys, t.deadlineArg(now), token).Text()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return t.legacyMessage(queue, raw, token), nil
}

// legacyMessage builds the delivery for a default-layout reservation. A body the SDK codec
// rejects (not JSON, not an object, a string "meta", a fractional "attempts" ...) is flagged
// Poison: this layout has no delivery counter and Release cannot re-encode such a body, so the
// App would otherwise re-publish a zero envelope and eventually drop the message, losing its
// bytes. Flagged, it is dead-lettered raw through the gated [Transport.DeadLetter] on the first
// delivery in every configuration (binding §1.1: poison instead of loss), the same route the
// zset layout uses. An identity-less body (ErrNoIdentity) is not poison; the App decides.
func (t *Transport) legacyMessage(queue, raw, token string) *babelqueue.ReceivedMessage {
	body, headers := unframe(raw)
	return &babelqueue.ReceivedMessage{
		Body:    body,
		Queue:   queue,
		Handle:  legacyHandle{raw: raw, token: token},
		Headers: headers,
		Poison:  !decodesForRelease(body),
	}
}

// sleepCtx waits d or until ctx ends (returning its error).
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Pop reserves the next message of queue, waiting up to timeout. timeout <= 0 means "wait
// until a message arrives or ctx ends"; a positive timeout returns (nil, nil) when nothing
// arrived in time.
//
// Laravel layout: every iteration migrates due :delayed / expired :reserved members, runs the
// pop script and, when empty, waits at most one second (BLPOP on :notify, or a sleep for the
// last sub-second stretch because BLPOP cannot wait less than a second). Default layout: every
// iteration sweeps expired reservations and runs the non-blocking reserve script; between empty
// polls it sleeps 100 ms, doubling up to 1 s, reset by a message, never beyond the timeout.
func (t *Transport) Pop(ctx context.Context, queue string, timeout time.Duration) (*babelqueue.ReceivedMessage, error) {
	if t.cfgErr != nil {
		return nil, t.cfgErr
	}
	var limit time.Time
	if timeout > 0 {
		limit = time.Now().Add(timeout)
	}
	backoff := 100 * time.Millisecond
	for {
		msg, err := t.reserveOnce(ctx, queue, t.nowFn())
		if err != nil || msg != nil {
			return msg, err
		}
		wait := waitSlice
		if !t.laravel {
			wait = backoff
			backoff = min(backoff*2, time.Second)
		}
		if timeout > 0 {
			remaining := time.Until(limit)
			if remaining <= 0 {
				return nil, nil
			}
			wait = min(wait, remaining)
		}
		if t.laravel && wait >= time.Second {
			// Woken by a token, or timed out: either way look at the ready list again. The ready
			// list is the source of truth; the notify list is only a wake-up hint.
			if err := t.client.BLPop(ctx, wait, t.Keys(queue).Notify).Err(); err != nil && !errors.Is(err, goredis.Nil) {
				return nil, err
			}
			continue
		}
		if err := sleepCtx(ctx, wait); err != nil {
			return nil, err
		}
	}
}

// Publish appends body to queue (RPUSH). The bare wire envelope is stored byte-for-byte
// (no frame), so a non-otel publisher and a cross-version consumer interoperate exactly
// as before.
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	return t.publishValue(ctx, queue, body)
}

// PublishWithHeaders appends body together with out-of-band transport headers
// ([babelqueue.HeaderPublisher]). In the default layout, when the header map is non-empty the
// value RPUSHed is a transport-owned frame ({"__bq_frame":1,"headers":…,"body":<raw envelope>})
// that carries the headers beside the frozen envelope (GR-1) — e.g. a W3C traceparent for
// cross-hop span linkage (ADR-0028). Blank keys/values are dropped; if nothing survives
// (or the map is nil/empty) it degrades to a byte-identical bare [Transport.Publish].
//
// In the Laravel-compatible layout (A3) no frame is ever written: the headers are dropped and
// the bare envelope is published, because a framed element would break Laravel workers and
// the pop script's cjson decode. A traceparent is therefore not carried through that layout;
// trace_id stays in the envelope (GR-4).
func (t *Transport) PublishWithHeaders(ctx context.Context, queue, body string, headers map[string]string) error {
	if t.laravel {
		return t.publishValue(ctx, queue, body)
	}
	value, err := frameValue(body, headers)
	if err != nil {
		return err
	}
	return t.publishValue(ctx, queue, value)
}

// frameValue is the pure produce-side decision: it returns the exact string to RPUSH for
// body + headers. With no usable headers it returns body verbatim (the bare form, so plain
// Publish and PublishWithHeaders-without-headers store byte-identical values); otherwise it
// returns the transport-owned frame JSON. Kept pure so the framing decision is unit-testable
// without a broker.
func frameValue(body string, headers map[string]string) (string, error) {
	clean := sanitizeHeaders(headers)
	if len(clean) == 0 {
		return body, nil
	}
	frame, err := json.Marshal(headerFrame{Version: frameVersion, Headers: clean, Body: body})
	if err != nil {
		return "", err
	}
	return string(frame), nil
}

// PublishDelayed schedules env on queue for delivery after delay (Laravel layout only): ZADD to
// `:delayed` with score floor(now)+ceil(delay). No notify token is written — the migrate that
// moves it writes one (§1.6).
func (t *Transport) PublishDelayed(ctx context.Context, queue string, env babelqueue.Envelope, delay time.Duration) error {
	body, err := env.Encode()
	if err != nil {
		return err
	}
	return t.publishDelayedAt(ctx, queue, string(body), delay, t.nowFn())
}

func (t *Transport) publishDelayedAt(ctx context.Context, queue, body string, delay time.Duration, now float64) error {
	if t.cfgErr != nil {
		return t.cfgErr
	}
	if !t.laravel {
		return ErrDelayedUnsupported
	}
	score := delayScore(now, delay)
	return t.client.ZAdd(ctx, t.Keys(queue).Delayed, goredis.Z{Score: float64(score), Member: body}).Err()
}

// Release returns a reserved message to the queue to be redelivered after delay
// ([babelqueue.Releaser]).
//
// Laravel layout (A2): one script removes the reservation from `:reserved` and adds the body to
// `:delayed` at floor(now)+ceil(delay); the body is the message re-encoded by the SDK codec with
// attempts = job.attempts+1, unknown keys and their types preserved (ADR-0031). A body that
// does not decode yields [babelqueue.ErrReleaseUnsupported] (the App falls back to
// re-publish + ack).
//
// Default layout: the release is a gated move back to the ready list with attempts+1 (the
// deadline-member gate of §1.7: a consumer whose reservation expired releases nothing). That
// layout has no delayed set, so a positive delay is not honoured — the message is re-queued
// at once, exactly what the App's re-publish fallback did, but now behind the gate so an
// expired reservation can no longer produce a duplicate. A body that does not decode yields
// [babelqueue.ErrReleaseUnsupported] here too: releasing it verbatim would never advance its
// attempts (this layout has no delivery counter), so the App would redeliver it forever. The
// App does not normally reach this branch for such a body: [Transport.Pop] flags it
// [babelqueue.ReceivedMessage.Poison], so the App dead-letters the raw body before any release
// (binding §1.1); the check stays as defense for a caller that releases a message directly.
func (t *Transport) Release(ctx context.Context, msg *babelqueue.ReceivedMessage, delay time.Duration) error {
	return t.releaseAt(ctx, msg, delay, t.nowFn())
}

func (t *Transport) releaseAt(ctx context.Context, msg *babelqueue.ReceivedMessage, delay time.Duration, now float64) error {
	if t.cfgErr != nil {
		return t.cfgErr
	}
	switch h := msg.Handle.(type) {
	case zsetHandle:
		body, ok := bodyForRelease(h.job)
		if !ok {
			return babelqueue.ErrReleaseUnsupported
		}
		k := t.Keys(msg.Queue)
		score := strconv.FormatInt(delayScore(now, delay), 10)
		return releaseScript.Run(ctx, t.client, []string{k.Reserved, k.Delayed}, h.reserved, score, body).Err()
	case legacyHandle:
		body, ok := bodyForRelease(msg.Body)
		if !ok {
			return babelqueue.ErrReleaseUnsupported
		}
		framed, err := frameValue(body, msg.Headers)
		if err != nil {
			return err
		}
		return t.gatedMove(ctx, msg.Queue, h, t.Keys(msg.Queue).Ready, framed)
	}
	return babelqueue.ErrReleaseUnsupported
}

// bodyForRelease re-encodes a raw body through the SDK codec with attempts+1. Unknown top-level
// and meta keys are carried verbatim by the codec (ADR-0031). ok is false when the body cannot
// be decoded into an envelope.
func bodyForRelease(raw string) (string, bool) {
	env, err := babelqueue.Decode([]byte(raw))
	if err != nil && !errors.Is(err, babelqueue.ErrNoIdentity) {
		return "", false
	}
	env.Attempts++
	out, err := env.Encode()
	if err != nil {
		return "", false
	}
	return string(out), true
}

// Pop-side helpers for the default layout's gate: the move scripts return 1 when the caller
// still owned its reservation and 0 when it had expired (and was swept or re-reserved).
func (t *Transport) gatedMove(ctx context.Context, queue string, h legacyHandle, dest, body string) error {
	keys := []string{t.processing(queue), t.deadlines(queue), dest}
	n, err := legacyMoveScript.Run(ctx, t.client, keys, h.token, h.raw, body).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		t.warnFn("babelqueue/redis: reservation expired", "queue", queue)
	}
	return nil
}

// DeadLetter writes body to dlqQueue and drops the reservation in one gated step
// ([babelqueue.DeadLetterer], §1.7). Default layout: behind the deadline-member gate shared with
// Ack and Release. Laravel layout: one script that ZREMs the reserved member and, only when that
// removed it, pushes body (and a notify token) to the dead-letter queue. A consumer whose
// reservation expired writes nothing and logs a warning, so the message is never both
// dead-lettered and handled by its new owner.
func (t *Transport) DeadLetter(ctx context.Context, msg *babelqueue.ReceivedMessage, dlqQueue, body string) error {
	if t.cfgErr != nil {
		return t.cfgErr
	}
	switch h := msg.Handle.(type) {
	case zsetHandle:
		k, dk := t.Keys(msg.Queue), t.Keys(dlqQueue)
		n, err := zsetDeadLetterScript.Run(ctx, t.client, []string{k.Reserved, dk.Ready, dk.Notify}, h.reserved, body).Int()
		if err != nil {
			return err
		}
		if n == 0 {
			t.warnFn("babelqueue/redis: reservation expired", "queue", msg.Queue)
		}
		return nil
	case legacyHandle:
		return t.gatedMove(ctx, msg.Queue, h, dlqQueue, body)
	}
	return fmt.Errorf("babelqueue/redis: unrecognised reservation handle %T", msg.Handle)
}

// Ack acknowledges a reservation. Laravel layout: ZREM of the :reserved member. Default layout:
// the gated ack — remove the reservation's own deadline member and, only if that removal
// succeeded, exactly one copy of the processing item; an expired reservation is a no-op that
// logs a warning, so a late ack never deletes another consumer's copy.
func (t *Transport) Ack(ctx context.Context, msg *babelqueue.ReceivedMessage) error {
	if t.cfgErr != nil {
		return t.cfgErr
	}
	switch h := msg.Handle.(type) {
	case zsetHandle:
		return t.client.ZRem(ctx, t.Keys(msg.Queue).Reserved, h.reserved).Err()
	case legacyHandle:
		keys := []string{t.processing(msg.Queue), t.deadlines(msg.Queue)}
		n, err := legacyAckScript.Run(ctx, t.client, keys, h.token, h.raw).Int()
		if err != nil {
			return err
		}
		if n == 0 {
			t.warnFn("babelqueue/redis: reservation expired", "queue", msg.Queue)
		}
		return nil
	}
	return fmt.Errorf("babelqueue/redis: unrecognised ack handle %T", msg.Handle)
}

// Close releases the underlying client.
func (t *Transport) Close() error {
	return t.client.Close()
}

// sanitizeHeaders copies headers, dropping blank keys and blank values. It returns nil
// when nothing survives, so callers can treat the result as "no headers" with len == 0.
func sanitizeHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string, len(headers))
	for k, v := range headers {
		if k == "" || v == "" {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// unframe interprets a stored Redis list value. A value is a header frame iff it is a
// JSON object carrying the reserved "__bq_frame" discriminator (a frozen wire envelope
// never has it); then it yields the unframed wire envelope body plus the carried headers
// (nil when the frame has none). Any other value — a bare envelope, a non-JSON value, or
// JSON without the sentinel — is returned verbatim as the body with nil headers, so
// older/cross-version queue values consume exactly as before.
func unframe(value string) (body string, headers map[string]string) {
	// Cheap reject: a frame is always a JSON object, and the sentinel key must appear.
	// This avoids a full unmarshal for the overwhelmingly common bare-envelope case
	// while still being correct (the substring check only short-circuits negatives).
	if len(value) == 0 || value[0] != '{' || !containsSentinel(value) {
		return value, nil
	}
	var f headerFrame
	if err := json.Unmarshal([]byte(value), &f); err != nil || f.Version == 0 {
		// Not a parseable frame (or sentinel absent / zero): treat as a bare value.
		return value, nil
	}
	return f.Body, sanitizeHeaders(f.Headers)
}

// containsSentinel reports whether value contains the reserved frame discriminator key.
// It is a fast pre-check before the JSON parse in unframe.
func containsSentinel(value string) bool {
	const sentinel = `"__bq_frame"`
	for i := 0; i+len(sentinel) <= len(value); i++ {
		if value[i:i+len(sentinel)] == sentinel {
			return true
		}
	}
	return false
}

var (
	_ babelqueue.Transport       = (*Transport)(nil)
	_ babelqueue.HeaderPublisher = (*Transport)(nil)
	_ babelqueue.Releaser        = (*Transport)(nil)
)
