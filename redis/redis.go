// Package redis is a Redis-backed [babelqueue.Transport] for the BabelQueue Go
// runtime. It uses the reliable-queue pattern: RPUSH to produce, BLMOVE the head
// to a per-queue processing list to reserve (so an in-flight message survives a
// worker crash), and LREM from that list to acknowledge.
//
//	tr, _ := redis.New("redis://localhost:6379/0")
//	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))
//
// This is a Go-owned reliable queue; full parity with Laravel's reserved-set
// reservation on a *shared* Redis queue is a separate task.
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
package redis

import (
	"context"
	"encoding/json"
	"errors"
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
}

// Option customizes New / NewWithClient.
type Option func(*Transport)

// WithProcessingSuffix overrides the per-queue processing-list suffix
// (default ":processing").
func WithProcessingSuffix(suffix string) Option {
	return func(t *Transport) { t.processingSuffix = suffix }
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
	t := &Transport{client: client, processingSuffix: ":processing"}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *Transport) processing(queue string) string {
	return queue + t.processingSuffix
}

// Publish appends body to queue (RPUSH). The bare wire envelope is stored byte-for-byte
// (no frame), so a non-otel publisher and a cross-version consumer interoperate exactly
// as before.
func (t *Transport) Publish(ctx context.Context, queue, body string) error {
	return t.client.RPush(ctx, queue, body).Err()
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
	return t.client.RPush(ctx, queue, value).Err()
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
	value, err := t.client.BLMove(ctx, queue, t.processing(queue), "LEFT", "RIGHT", timeout).Result()
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
