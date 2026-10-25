package redis

import (
	"context"
	"errors"
	"strings"
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
		// ADR-0036 decision 4 (K-E): an identity-less body is poison, not an unknown URN.
		"no identity":            `{"attempts":0,"data":{}}`,
		"job number, no urn":     `{"job":5,"attempts":0}`,
		"job blank, no urn":      `{"job":" \t","attempts":0}`,
		"urn number, no job":     `{"urn":5,"attempts":0}`,
		"job and urn both blank": `{"job":"","urn":"  ","attempts":0}`,
		"bare null":              `null`,
	}
	for name, body := range poison {
		t.Run("poison/"+name, func(t *testing.T) {
			if decodesWithIdentity(body) {
				t.Fatalf("decodesWithIdentity(%q) = true, want false (core Decode must reject it)", body)
			}
			if !isPoisonHead(body, body+"-incremented") {
				t.Fatalf("isPoisonHead(%q) = false, want true", body)
			}
		})
	}

	ok := map[string]string{
		"full envelope":   `{"job":"urn:bq:orders:created","trace_id":"t","data":{"n":1},"meta":{"id":"i","queue":"orders","lang":"go","schema_version":1,"created_at":1},"attempts":0}`,
		"minimal":         `{"job":"urn:x","attempts":0}`,
		"missing tries":   `{"job":"urn:x"}`,
		"urn alias":       `{"urn":"urn:x","attempts":0}`,
		"blank job, urn":  `{"job":" ","urn":"urn:x","attempts":0}`,
		"job number, urn": `{"job":5,"urn":"urn:x","attempts":0}`,
	}
	for name, body := range ok {
		t.Run("not-poison/"+name, func(t *testing.T) {
			if !decodesWithIdentity(body) {
				t.Fatalf("decodesWithIdentity(%q) = false, want true", body)
			}
			if isPoisonHead(body, body+"-incremented") {
				t.Fatalf("isPoisonHead(%q) = true, want false", body)
			}
		})
	}

	// Binding §1.1 "Invalid UTF-8": a body the codec decodes (U+FFFD) but whose stored bytes are not
	// valid UTF-8 is poison in the zset layout too, whichever of job / reserved carries the byte.
	badUTF8 := "{\"job\":\"urn:x\",\"attempts\":0,\"data\":{\"n\":\"\xff\"}}"
	if !decodesWithIdentity(badUTF8) {
		t.Fatal("precondition: the core codec decodes this body (that is why the transport must classify it)")
	}
	if !isPoisonHead(badUTF8, strings.Replace(badUTF8, `"attempts":0`, `"attempts":1`, 1)) {
		t.Fatal("a decodable body with invalid UTF-8 must be poison in the zset layout")
	}
	if !isPoisonHead(`{"job":"urn:x","attempts":0}`, `{"job":"urn:x","attempts":1,"x":"`+"\xff"+`"}`) {
		t.Fatal("an invalid UTF-8 reserved member must be poison too")
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

// BenchmarkDecodesWithIdentity measures the per-reservation cost of classifying a body in the
// transport (a second parse next to the App's own Decode).
func BenchmarkDecodesWithIdentity(b *testing.B) {
	body := `{"job":"urn:bq:orders:created","trace_id":"t","data":{"order_id":42,"items":[1,2,3]},"meta":{"id":"i","queue":"orders","lang":"go","schema_version":1,"created_at":1},"attempts":0}`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !decodesWithIdentity(body) {
			b.Fatal("body must decode")
		}
	}
}
