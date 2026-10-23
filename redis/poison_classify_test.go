package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	goredis "github.com/redis/go-redis/v9"
)

// These tests need no Redis: they pin the pure decisions the transport makes about a body.

// MINOR 1: bodies the reserve script's cjson accepts (it is more lenient than the SDK codec)
// but the core Decode rejects are poison at reserve, so they reach the gated raw dead-letter
// route instead of a zero-envelope push + ack.
func TestIsPoisonHeadClassifiesCodecRejectedBodies(t *testing.T) {
	poison := map[string]string{
		"string meta":       `{"job":"urn:x","attempts":0,"meta":"x"}`,
		"string data":       `{"job":"urn:x","attempts":0,"data":"s"}`,
		"numeric trace_id":  `{"job":"urn:x","attempts":0,"trace_id":5}`,
		"fractional tries":  `{"job":"urn:x","attempts":1.5}`,
		"not json":          `not-json`,
		"json array":        `[1,2]`,
		"string attempts":   `{"job":"urn:x","attempts":"1"}`,
		"unterminated json": `{"job":`,
	}
	for name, body := range poison {
		t.Run("poison/"+name, func(t *testing.T) {
			if decodesForRelease(body) {
				t.Fatalf("decodesForRelease(%q) = true, want false (core Decode must reject it)", body)
			}
			if !isPoisonHead(body, body+"-incremented") {
				t.Fatalf("isPoisonHead(%q) = false, want true", body)
			}
		})
	}

	ok := map[string]string{
		"full envelope": `{"job":"urn:bq:orders:created","trace_id":"t","data":{"n":1},"meta":{"id":"i","queue":"orders","lang":"go","schema_version":1,"created_at":1},"attempts":0}`,
		"minimal":       `{"job":"urn:x","attempts":0}`,
		"no identity":   `{"attempts":0,"data":{}}`,
		"missing tries": `{"job":"urn:x"}`,
	}
	for name, body := range ok {
		t.Run("not-poison/"+name, func(t *testing.T) {
			if !decodesForRelease(body) {
				t.Fatalf("decodesForRelease(%q) = false, want true", body)
			}
			if isPoisonHead(body, body+"-incremented") {
				t.Fatalf("isPoisonHead(%q) = true, want false", body)
			}
		})
	}

	// A1 stays: the script's own {job, job} answer is poison whatever the body is.
	valid := `{"job":"urn:x","attempts":0}`
	if !isPoisonHead(valid, valid) {
		t.Fatal("job == reserved must stay poison")
	}
}

// MAJOR: in the default (legacy :processing) layout an undecodable body must not be re-queued
// verbatim (its attempts never advance and this layout has no delivery counter, so it would be
// redelivered forever). Release declines it, which sends the App to its bounded re-publish path.
// The Release returns before any Redis call, so no server is needed.
func TestLegacyReleaseOfUndecodableBodyIsUnsupported(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	tr := NewWithClient(client)

	for _, body := range []string{`not-json`, `{"job":`, `[1,2]`, `{"job":"urn:x","attempts":0,"meta":"x"}`} {
		msg := &babelqueue.ReceivedMessage{
			Queue:  "q",
			Body:   body,
			Handle: legacyHandle{raw: "tok:" + body, token: "tok"},
		}
		err := tr.Release(context.Background(), msg, time.Second)
		if !errors.Is(err, babelqueue.ErrReleaseUnsupported) {
			t.Fatalf("Release(%q) = %v, want ErrReleaseUnsupported", body, err)
		}
	}
}

// The zset layout never re-encodes an undecodable head either (same decision, other handle).
func TestZsetReleaseOfUndecodableBodyIsUnsupported(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	tr := NewWithClient(client, WithLaravelLayout(""))

	msg := &babelqueue.ReceivedMessage{Queue: "q", Body: `not-json`, Handle: zsetHandle{job: `not-json`, reserved: `not-json`}}
	if err := tr.Release(context.Background(), msg, time.Second); !errors.Is(err, babelqueue.ErrReleaseUnsupported) {
		t.Fatalf("Release = %v, want ErrReleaseUnsupported", err)
	}
}

// BenchmarkDecodesForRelease measures the per-reservation cost of classifying a body in the
// transport (a second parse next to the App's own Decode).
func BenchmarkDecodesForRelease(b *testing.B) {
	body := `{"job":"urn:bq:orders:created","trace_id":"t","data":{"order_id":42,"items":[1,2,3]},"meta":{"id":"i","queue":"orders","lang":"go","schema_version":1,"created_at":1},"attempts":0}`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !decodesForRelease(body) {
			b.Fatal("body must decode")
		}
	}
}
