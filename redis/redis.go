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
// returns them. Publish pushes to the ready list and signals the notify list; Pop first
// moves due delayed and expired reserved members back to the ready list, then reserves the
// head into the reserved sorted set scored by its retry-after deadline; Ack removes it from
// that set. The reserved value is the stored value byte-for-byte, so payload identity (§1.2)
// holds. Not implemented: incrementing payload.attempts in Redis (the Laravel reserve script
// does that with cjson; the BabelQueue App counts attempts itself) and an API to schedule a
// delayed publish (the available-at formula is not defined by the contract).
package redis

import (
	"context"
	"encoding/json"
	"errors"
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
}

// DefaultKeyPrefix is Laravel's default Redis queue key prefix (`queues:`).
const DefaultKeyPrefix = "queues:"

// DefaultRetryAfter is the reservation window of the Laravel layout (Laravel's
// `retry_after` default, 90 s): a reserved member whose deadline has passed is made ready again.
const DefaultRetryAfter = 90 * time.Second

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

// WithRetryAfter sets the reservation window of the Laravel layout (default
// [DefaultRetryAfter]). It has no effect on the default layout.
func WithRetryAfter(d time.Duration) Option {
	return func(t *Transport) { t.retryAfter = d }
}

// New connects to Redis from a redis:// or rediss:// URL.
func New(url string, opts ...Option) (*Transport, error) {
	options, err := goredis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return NewWithClient(goredis.NewClient(options), opts...), nil
}

// NewWithClient wraps an existing go-redis client (bring your own pool/TLS/auth).
func NewWithClient(client *goredis.Client, opts ...Option) *Transport {
	t := &Transport{client: client, processingSuffix: ":processing", retryAfter: DefaultRetryAfter}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *Transport) processing(queue string) string {
	return queue + t.processingSuffix
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

// Atomic Laravel-layout scripts. None touches the payload (no cjson).
var (
	// KEYS: ready, notify. ARGV: value.
	pushScript = goredis.NewScript(`
redis.call('rpush', KEYS[1], ARGV[1])
redis.call('rpush', KEYS[2], 1)
return 1`)

	// KEYS: source sorted set, ready, notify. ARGV: now. Moves members whose score is due.
	migrateScript = goredis.NewScript(`
local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1])
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

	// KEYS: ready, reserved, notify. ARGV: deadline. Reserves the head, scored by its deadline,
	// and consumes one notify token like Laravel's pop script (broker-bindings §1.1): the notify
	// list holds one token per ready value, so it must shrink as values are reserved.
	reserveScript = goredis.NewScript(`
local job = redis.call('lpop', KEYS[1])
if job then
  redis.call('zadd', KEYS[2], ARGV[1], job)
  redis.call('lpop', KEYS[3])
end
return job`)
)

func (t *Transport) publishValue(ctx context.Context, queue, value string) error {
	if !t.laravel {
		return t.client.RPush(ctx, queue, value).Err()
	}
	k := t.Keys(queue)
	return pushScript.Run(ctx, t.client, []string{k.Ready, k.Notify}, value).Err()
}

// laravelWaitSlice bounds one blocking wait on the notify list when the caller asked to wait
// indefinitely (timeout <= 0). The wait is repeated in slices so due delayed and expired
// reserved members are still migrated to the ready list while the queue is otherwise idle.
const laravelWaitSlice = time.Second

// popLaravel reserves the next ready value in the Laravel layout. timeout <= 0 means "wait
// until a value arrives or ctx ends", the same as the default layout's blocking pop; a
// positive timeout returns [goredis.Nil] when nothing arrived in time.
func (t *Transport) popLaravel(ctx context.Context, queue string, timeout time.Duration) (string, error) {
	k := t.Keys(queue)
	reserve := func() (string, error) {
		deadline := strconv.FormatInt(time.Now().Add(t.retryAfter).Unix(), 10)
		return reserveScript.Run(ctx, t.client, []string{k.Ready, k.Reserved, k.Notify}, deadline).Text()
	}
	for {
		now := strconv.FormatInt(time.Now().Unix(), 10)
		for _, src := range []string{k.Delayed, k.Reserved} {
			if err := migrateScript.Run(ctx, t.client, []string{src, k.Ready, k.Notify}, now).Err(); err != nil {
				return "", err
			}
		}
		// Reserve first: the ready list is the source of truth, the notify list only a wake-up
		// hint. A ready value without a notify token (a producer that did not push one, or a
		// token already consumed) must never be starved by a blocking wait on the notify list.
		value, err := reserve()
		if !errors.Is(err, goredis.Nil) {
			return value, err
		}
		wait := timeout
		if wait <= 0 {
			wait = laravelWaitSlice
		}
		if err := t.client.BLPop(ctx, wait, k.Notify).Err(); err != nil && !errors.Is(err, goredis.Nil) {
			return "", err
		}
		// Woken by a token, or timed out: either way look at the ready list again.
		value, err = reserve()
		if !errors.Is(err, goredis.Nil) || timeout > 0 {
			return value, err
		}
		// Indefinite wait and still nothing: migrate again and keep waiting.
	}
}

// Publish appends body to queue (RPUSH). The bare wire envelope is stored byte-for-byte
// (no frame), so a non-otel publisher and a cross-version consumer interoperate exactly
// as before.
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	return t.publishValue(ctx, queue, body)
}

// PublishWithHeaders appends body together with out-of-band transport headers
// ([babelqueue.HeaderPublisher]). When the header map is non-empty the value RPUSHed is
// a transport-owned frame ({"__bq_frame":1,"headers":…,"body":<raw envelope>}) that
// carries the headers beside the frozen envelope (GR-1) — e.g. a W3C traceparent for
// cross-hop span linkage (ADR-0028). Blank keys/values are dropped; if nothing survives
// (or the map is nil/empty) it degrades to a byte-identical bare [Transport.Publish], so
// nothing regresses.
func (t *Transport) PublishWithHeaders(ctx context.Context, queue, body string, headers map[string]string) error {
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

// Pop atomically moves the head of queue to its processing list and returns it,
// blocking up to timeout. It returns (nil, nil) when no message arrives in time.
//
// The reserved value may be a header frame (written by PublishWithHeaders) or a bare
// wire envelope (written by Publish or an older/cross-version publisher). Either way the
// returned Handle is the *stored* value byte-for-byte, so Ack's LREM still matches.
func (t *Transport) Pop(ctx context.Context, queue string, timeout time.Duration) (*babelqueue.ReceivedMessage, error) {
	var value string
	var err error
	if t.laravel {
		value, err = t.popLaravel(ctx, queue, timeout)
	} else {
		value, err = t.client.BLMove(ctx, queue, t.processing(queue), "LEFT", "RIGHT", timeout).Result()
	}
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	body, headers := unframe(value)
	return &babelqueue.ReceivedMessage{Body: body, Queue: queue, Handle: value, Headers: headers}, nil
}

// Ack removes the reserved message from its processing list (LREM). The handle is the
// raw stored list value (frame or bare body), so the LREM matches what was RPUSHed.
func (t *Transport) Ack(ctx context.Context, msg *babelqueue.ReceivedMessage) error {
	handle, _ := msg.Handle.(string)
	if t.laravel {
		return t.client.ZRem(ctx, t.Keys(msg.Queue).Reserved, handle).Err()
	}
	return t.client.LRem(ctx, t.processing(msg.Queue), 1, handle).Err()
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
)
