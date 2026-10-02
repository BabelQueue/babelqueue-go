package babelqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// extrasBody is an envelope carrying an unknown top-level key, unknown meta keys
// (scalar + nested object) and one forbidden key of each kind.
const extrasBody = `{"job":"urn:babel:orders:created","trace_id":"7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b",` +
	`"data":{"order_id":1042},"meta":{"id":"f1e2d3c4-b5a6-4789-90ab-cdef01234567","queue":"orders",` +
	`"lang":"php","schema_version":1,"created_at":1749132727000,"vendor_flag":true,` +
	`"vendor_ctx":{"region":"eu-west-1","hops":[1,2]},"ts":5},"attempts":0,"extra_top":{"k":"<v&>"},"timestamp":1}`

// assertExtras checks that the unknown keys of extrasBody survived into body and
// that no forbidden key was re-emitted.
func assertExtras(t *testing.T, body []byte) {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, body)
	}
	if !reflect.DeepEqual(doc["extra_top"], map[string]any{"k": "<v&>"}) {
		t.Errorf("extra_top = %#v", doc["extra_top"])
	}
	meta, _ := doc["meta"].(map[string]any)
	if meta["vendor_flag"] != true {
		t.Errorf("meta.vendor_flag = %#v", meta["vendor_flag"])
	}
	wantCtx := map[string]any{"region": "eu-west-1", "hops": []any{float64(1), float64(2)}}
	if !reflect.DeepEqual(meta["vendor_ctx"], wantCtx) {
		t.Errorf("meta.vendor_ctx = %#v", meta["vendor_ctx"])
	}
	if _, ok := doc["timestamp"]; ok {
		t.Error("forbidden top-level timestamp re-emitted")
	}
	if _, ok := meta["ts"]; ok {
		t.Error("forbidden meta.ts re-emitted")
	}
}

func decodeExtras(t *testing.T) Envelope {
	t.Helper()
	env, err := Decode([]byte(extrasBody))
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// TestEncodeWithoutExtrasIsByteIdentical proves the custom codec does not change
// the bytes of an envelope that has no unknown keys: every valid golden fixture
// encodes exactly as the plain field struct did before extras existed.
func TestEncodeWithoutExtrasIsByteIdentical(t *testing.T) {
	files := []string{"order-created.json", "urn-alias.json", "dead-lettered.json", "unicode-and-numbers.json"}
	for _, f := range files {
		t.Run(f, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("testdata", "conformance", "fixtures", f))
			if err != nil {
				t.Fatal(err)
			}
			env, err := Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			if len(env.extras) != 0 || len(env.metaExtras) != 0 {
				t.Fatalf("golden fixture unexpectedly has extras: %v %v", env.extras, env.metaExtras)
			}
			got, err := env.Encode()
			if err != nil {
				t.Fatal(err)
			}
			want, err := marshalCompact(envelopeFields{
				Job: env.Job, TraceID: env.TraceID, Data: env.Data, Meta: env.Meta,
				Attempts: env.Attempts, DeadLetter: env.DeadLetter,
			})
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Errorf("encode drifted:\n got %s\nwant %s", got, want)
			}
		})
	}
}

// TestEncodeExtrasAfterKnownFields pins the layout: known fields in canonical
// order, then the unknown keys (sorted), with no HTML escaping.
func TestEncodeExtrasAfterKnownFields(t *testing.T) {
	env := decodeExtras(t)
	got, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"job":"urn:babel:orders:created","trace_id":"7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b",` +
		`"data":{"order_id":1042},"meta":{"id":"f1e2d3c4-b5a6-4789-90ab-cdef01234567","queue":"orders",` +
		`"lang":"php","schema_version":1,"created_at":1749132727000,` +
		`"vendor_ctx":{"region":"eu-west-1","hops":[1,2]},"vendor_flag":true},"attempts":0,"extra_top":{"k":"<v&>"}}`
	if string(got) != want {
		t.Errorf("encode:\n got %s\nwant %s", got, want)
	}
	if w := env.Warnings(); len(w) != 2 {
		t.Errorf("warnings = %q, want 2 (timestamp, meta.ts)", w)
	}
}

func TestDecodeRejectsNonObjectData(t *testing.T) {
	if _, err := Decode([]byte(`{"job":"urn:x","trace_id":"t","data":[1],"meta":{"schema_version":1},"attempts":0}`)); err == nil {
		t.Fatal("array data must be rejected")
	}
}

func TestDecodeRejectsNonObjectMeta(t *testing.T) {
	if _, err := Decode([]byte(`{"job":"urn:x","trace_id":"t","data":{},"meta":[],"attempts":0}`)); err == nil {
		t.Fatal("array meta must be rejected")
	}
}

// Re-emit path: retry (attempts++) keeps unknown keys.
func TestRetryPreservesExtras(t *testing.T) {
	tr := NewInMemoryTransport()
	app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(3))
	var seen []Envelope
	app.Handle("urn:babel:orders:created", func(_ context.Context, env Envelope) error {
		seen = append(seen, env)
		if len(seen) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	if err := tr.Publish(context.Background(), "orders", extrasBody); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 0); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatalf("handler called %d times, want 2", len(seen))
	}
	if seen[1].Attempts != 1 {
		t.Errorf("attempts after retry = %d, want 1", seen[1].Attempts)
	}
	body, err := seen[1].Encode()
	if err != nil {
		t.Fatal(err)
	}
	assertExtras(t, body)
}

// Re-emit path: dead-lettering (dead_letter added) keeps unknown keys.
func TestDeadLetterPreservesExtras(t *testing.T) {
	tr := NewInMemoryTransport()
	app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(1), WithDeadLetter(true))
	app.Handle("urn:babel:orders:created", func(context.Context, Envelope) error { return errors.New("boom") })
	if err := tr.Publish(context.Background(), "orders", extrasBody); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 0); err != nil {
		t.Fatal(err)
	}
	msg, err := tr.Pop(context.Background(), "orders.dlq", 0)
	if err != nil || msg == nil {
		t.Fatalf("no dead-lettered message: %v", err)
	}
	assertExtras(t, []byte(msg.Body))
	env, _ := Decode([]byte(msg.Body))
	if env.DeadLetter == nil || env.DeadLetter.Reason != "failed" {
		t.Errorf("dead_letter = %+v", env.DeadLetter)
	}

	// Annotate directly as well.
	annotated, err := Annotate(decodeExtras(t), "failed", "orders", 3, errors.New("x")).Encode()
	if err != nil {
		t.Fatal(err)
	}
	assertExtras(t, annotated)
}

// Re-emit path: redrive (dead_letter removed, attempts reset) keeps unknown keys.
func TestRedrivePreservesExtras(t *testing.T) {
	tr := NewInMemoryTransport()
	dl, err := Annotate(decodeExtras(t), "failed", "orders", 3, errors.New("boom")).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Publish(context.Background(), "orders.dlq", string(dl)); err != nil {
		t.Fatal(err)
	}
	res, err := Redrive(context.Background(), tr, "orders.dlq", RedriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Redriven != 1 {
		t.Fatalf("redriven = %d, want 1", res.Redriven)
	}
	msg, err := tr.Pop(context.Background(), "orders", 0)
	if err != nil || msg == nil {
		t.Fatalf("no redriven message: %v", err)
	}
	assertExtras(t, []byte(msg.Body))
	env, _ := Decode([]byte(msg.Body))
	if env.DeadLetter != nil || env.Attempts != 0 {
		t.Errorf("redrive did not reset: dead_letter=%+v attempts=%d", env.DeadLetter, env.Attempts)
	}
}

// releasingTransport records Release calls; releaseErr is returned from Release.
type releasingTransport struct {
	*InMemoryTransport
	delays     []time.Duration
	releaseErr error
}

func (r *releasingTransport) Release(_ context.Context, _ *ReceivedMessage, d time.Duration) error {
	r.delays = append(r.delays, d)
	return r.releaseErr
}

func TestAppRetryPrefersReleaser(t *testing.T) {
	tr := &releasingTransport{InMemoryTransport: NewInMemoryTransport()}
	app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(3), WithRetryBackoff(7*time.Second))
	app.Handle("urn:babel:orders:created", func(context.Context, Envelope) error { return errors.New("boom") })
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(tr.delays) != 1 || tr.delays[0] != 7*time.Second {
		t.Fatalf("Release delays = %v, want [7s]", tr.delays)
	}
	if tr.Size("orders") != 0 {
		t.Error("a released retry must not be re-published")
	}
}

func TestAppReleaseUnsupportedFallsBackToRepublish(t *testing.T) {
	tr := &releasingTransport{InMemoryTransport: NewInMemoryTransport(),
		releaseErr: fmt.Errorf("no handle: %w", ErrReleaseUnsupported)}
	app := NewApp(tr, WithDefaultQueue("orders"),
		WithUnknownURNStrategy(StrategyRelease), WithUnknownURNReleaseDelay(time.Second))
	if _, err := app.Publish(context.Background(), "urn:babel:nobody", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(tr.delays) != 1 || tr.delays[0] != time.Second {
		t.Fatalf("Release delays = %v, want [1s]", tr.delays)
	}
	if tr.Size("orders") != 1 {
		t.Errorf("fallback must re-publish; size = %d", tr.Size("orders"))
	}
}

func TestAppReleaseErrorNeverRepublishes(t *testing.T) {
	tr := &releasingTransport{InMemoryTransport: NewInMemoryTransport(), releaseErr: errors.New("throttled")}
	app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(3))
	app.Handle("urn:babel:orders:created", func(context.Context, Envelope) error { return errors.New("boom") })
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(tr.delays) != 1 || tr.delays[0] != 0 {
		t.Fatalf("Release delays = %v, want [0s] (default backoff)", tr.delays)
	}
	if tr.Size("orders") != 0 {
		t.Errorf("a failed in-place release must not re-publish a copy; size = %d", tr.Size("orders"))
	}
}

func TestAppUnknownURNReleaseDefaultsToZeroDelay(t *testing.T) {
	tr := &releasingTransport{InMemoryTransport: NewInMemoryTransport()}
	app := NewApp(tr, WithDefaultQueue("orders"), WithUnknownURNStrategy(StrategyRelease))
	if _, err := app.Publish(context.Background(), "urn:babel:nobody", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(tr.delays) != 1 || tr.delays[0] != 0 {
		t.Fatalf("Release delays = %v, want [0s]", tr.delays)
	}
}

func TestDecodeCaseVariantKnownKeysAreNotExtras(t *testing.T) {
	raw := `{"JOB":"urn:babel:orders:created","trace_id":"t","data":{},` +
		`"meta":{"Queue":"orders","schema_version":1,"TS":5},"Attempts":2,"Timestamp":1}`
	env, err := Decode([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if env.Job != "urn:babel:orders:created" || env.Meta.Queue != "orders" || env.Attempts != 2 {
		t.Fatalf("case-folded fields not decoded: %+v", env)
	}
	if len(env.extras) != 0 || len(env.metaExtras) != 0 {
		t.Errorf("case variants leaked into extras: top=%v meta=%v", env.extras, env.metaExtras)
	}
	if w := env.Warnings(); len(w) != 2 {
		t.Errorf("warnings = %v, want 2 (Timestamp, meta.TS)", w)
	}
	body, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"JOB", "Attempts", "Timestamp"} {
		if _, ok := doc[k]; ok {
			t.Errorf("re-encoded body carries %q: %s", k, body)
		}
	}
}

// deliveryCountTransport stamps a native delivery count on every Pop and releases
// in place, like SQS: the body is never rewritten between deliveries.
type deliveryCountTransport struct {
	*InMemoryTransport
	deliveries int
	ackErr     error
	acks       int
}

func (d *deliveryCountTransport) Pop(ctx context.Context, q string, to time.Duration) (*ReceivedMessage, error) {
	msg, err := d.InMemoryTransport.Pop(ctx, q, to)
	if msg != nil {
		d.deliveries++
		msg.DeliveryCount = d.deliveries
	}
	return msg, err
}

func (d *deliveryCountTransport) Release(ctx context.Context, msg *ReceivedMessage, _ time.Duration) error {
	return d.InMemoryTransport.Publish(ctx, msg.Queue, msg.Body) // same body, back on the queue
}

func (d *deliveryCountTransport) Ack(context.Context, *ReceivedMessage) error {
	d.acks++
	return d.ackErr
}

func TestAppDeliveryCountBoundsUndecodableRelease(t *testing.T) {
	tr := &deliveryCountTransport{InMemoryTransport: NewInMemoryTransport()}
	app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(3), WithDeadLetter(true))
	if err := tr.InMemoryTransport.Publish(context.Background(), "orders", `{"job":"urn:x","data":[1]}`); err != nil {
		t.Fatal(err)
	}
	n, err := app.Drain(context.Background(), "orders", 50)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("deliveries = %d, want 3", n)
	}
	if tr.Size("orders") != 0 || tr.Size("orders.dlq") != 1 {
		t.Errorf("orders=%d dlq=%d, want 0/1", tr.Size("orders"), tr.Size("orders.dlq"))
	}
}

func TestAppAckFailureIsReportedNotRetried(t *testing.T) {
	tr := &deliveryCountTransport{InMemoryTransport: NewInMemoryTransport(), ackErr: errors.New("delete failed")}
	var got []error
	app := NewApp(tr, WithDefaultQueue("orders"), WithDeadLetter(true),
		WithAckErrorHandler(func(_ context.Context, _ *ReceivedMessage, err error) { got = append(got, err) }))
	calls := 0
	app.Handle("urn:babel:orders:created", func(context.Context, Envelope) error { calls++; return nil })
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 10); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(got) != 1 || got[0] == nil {
		t.Errorf("handler calls=%d ack errors=%v, want 1 call and 1 reported error", calls, got)
	}
	if tr.Size("orders") != 0 || tr.Size("orders.dlq") != 0 {
		t.Errorf("ack failure must not release/re-publish/dead-letter: orders=%d dlq=%d", tr.Size("orders"), tr.Size("orders.dlq"))
	}
}

// releaseErrTransport releases by returning a fixed error, like an SQS client whose
// ChangeMessageVisibility is denied.
type releaseErrTransport struct {
	*InMemoryTransport
	releaseErr error
	releases   int
}

func (r *releaseErrTransport) Release(context.Context, *ReceivedMessage, time.Duration) error {
	r.releases++
	return r.releaseErr
}

func TestAppReleaseFailureIsReported(t *testing.T) {
	tr := &releaseErrTransport{InMemoryTransport: NewInMemoryTransport(), releaseErr: errors.New("AccessDenied")}
	var got []error
	app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(3),
		WithReleaseErrorHandler(func(_ context.Context, msg *ReceivedMessage, err error) {
			if msg == nil {
				t.Error("release error handler got nil message")
			}
			got = append(got, err)
		}))
	app.Handle("urn:babel:orders:created", func(context.Context, Envelope) error { return errors.New("boom") })
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if tr.releases != 1 || len(got) != 1 || got[0] == nil || got[0].Error() != "AccessDenied" {
		t.Errorf("releases=%d reported=%v, want 1 release and 1 reported AccessDenied", tr.releases, got)
	}
	if tr.Size("orders") != 0 {
		t.Errorf("a failed release must not re-publish: orders=%d", tr.Size("orders"))
	}
}

func TestAppReleaseUnsupportedIsNotReported(t *testing.T) {
	tr := &releaseErrTransport{InMemoryTransport: NewInMemoryTransport(), releaseErr: ErrReleaseUnsupported}
	var got []error
	app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(3),
		WithReleaseErrorHandler(func(_ context.Context, _ *ReceivedMessage, err error) { got = append(got, err) }))
	app.Handle("urn:babel:orders:created", func(context.Context, Envelope) error { return errors.New("boom") })
	if _, err := app.Publish(context.Background(), "urn:babel:orders:created", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("ErrReleaseUnsupported reported as a release error: %v", got)
	}
	if tr.Size("orders") != 1 {
		t.Errorf("fallback must re-publish the retry: orders=%d", tr.Size("orders"))
	}
}

func TestAppReleaseStrategyBoundsPoisonBodies(t *testing.T) {
	bodies := []string{`not json`, `{"job":"urn:x","data":[1]}`, `{"trace_id":"t","data":{}}`}
	for _, body := range bodies {
		// In-place release (native delivery count) and re-publish fallback.
		for _, inPlace := range []bool{true, false} {
			var tr interface {
				Transport
				Size(string) int
			}
			mem := NewInMemoryTransport()
			tr = mem
			if inPlace {
				tr = &deliveryCountTransport{InMemoryTransport: mem}
			}
			app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(3), WithDeadLetter(true),
				WithUnknownURNStrategy(StrategyRelease))
			if err := mem.Publish(context.Background(), "orders", body); err != nil {
				t.Fatal(err)
			}
			n, err := app.Drain(context.Background(), "orders", 50)
			if err != nil {
				t.Fatal(err)
			}
			if n != 3 {
				t.Errorf("%q inPlace=%v: deliveries = %d, want 3", body, inPlace, n)
			}
			if tr.Size("orders") != 0 || tr.Size("orders.dlq") != 1 {
				t.Errorf("%q inPlace=%v: orders=%d dlq=%d, want 0/1", body, inPlace, tr.Size("orders"), tr.Size("orders.dlq"))
			}
		}
	}
}

// A decodable envelope with an unregistered URN keeps the release semantics (a
// rolling deploy may register its handler later): it is released, not counted.
func TestAppReleaseStrategyStillReleasesUnknownURN(t *testing.T) {
	tr := &releaseErrTransport{InMemoryTransport: NewInMemoryTransport()}
	app := NewApp(tr, WithDefaultQueue("orders"), WithMaxAttempts(1), WithDeadLetter(true),
		WithUnknownURNStrategy(StrategyRelease))
	if _, err := app.Publish(context.Background(), "urn:babel:orders:later", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Drain(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	if tr.releases != 1 || tr.Size("orders.dlq") != 0 {
		t.Errorf("releases=%d dlq=%d, want 1 release and no dead letter", tr.releases, tr.Size("orders.dlq"))
	}
}
