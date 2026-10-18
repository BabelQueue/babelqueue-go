package redis

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// conformanceDir is the core's vendored copy of the canonical suite.
const conformanceDir = "../testdata/conformance"

type redisManifest struct {
	Redis struct {
		PayloadIdentity struct {
			EnvelopeFile string `json:"envelope_file"`
		} `json:"payload_identity"`
	} `json:"redis"`
	RedisLayout struct {
		Cases []struct {
			Name      string `json:"name"`
			Queue     string `json:"queue"`
			KeyPrefix string `json:"key_prefix"`
			Expect    struct {
				Ready    string `json:"ready"`
				Delayed  string `json:"delayed"`
				Reserved string `json:"reserved"`
				Notify   string `json:"notify"`
			} `json:"expect"`
		} `json:"cases"`
	} `json:"redis_layout"`
}

// skipOrFailNoRedis skips a live-broker subtest when no Redis is reachable, unless
// BQ_REQUIRE_REDIS is set (the CI conformance-results leg provides a Redis service container and
// sets it): then a missing broker fails instead of letting the case pass on the static check alone.
func skipOrFailNoRedis(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv("BQ_REQUIRE_REDIS") != "" {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}

func loadRedisManifest(t *testing.T) redisManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m redisManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	return m
}

// TestRedisConformance locks broker-bindings.md §1 payload identity: the queue element is the
// envelope JSON byte-for-byte, with no wrapping and no added fields. The static check covers
// the framing used by Publish/Pop; when a Redis broker is reachable the same bytes are also
// pushed and reserved through the live transport in both key layouts.
func TestRedisConformance(t *testing.T) {
	m := loadRedisManifest(t)

	t.Run("payload_identity", func(t *testing.T) {
		body, err := os.ReadFile(filepath.Join(conformanceDir, m.Redis.PayloadIdentity.EnvelopeFile))
		if err != nil {
			t.Fatalf("fixture: %v", err)
		}
		want := string(body)

		stored, err := frameValue(want, nil)
		if err != nil {
			t.Fatal(err)
		}
		if stored != want {
			t.Fatalf("stored element differs from the envelope bytes:\n got %q\nwant %q", stored, want)
		}
		got, headers := unframe(stored)
		if got != want || len(headers) != 0 {
			t.Fatalf("unframe changed the payload or invented headers: body=%q headers=%v", got, headers)
		}

		for name, opts := range map[string][]Option{
			"default": nil,
			"laravel": {WithLaravelLayout("")},
		} {
			t.Run("live/"+name, func(t *testing.T) {
				url := os.Getenv("REDIS_URL")
				if url == "" {
					url = "redis://localhost:6379/0"
				}
				tr, err := New(url, opts...)
				if err != nil {
					skipOrFailNoRedis(t, "bad redis url: %v", err)
				}
				defer tr.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := tr.client.Ping(ctx).Err(); err != nil {
					skipOrFailNoRedis(t, "no redis broker: %v", err)
				}
				queue := "bq-conformance-identity-" + name
				if err := tr.Publish(ctx, queue, want); err != nil {
					t.Fatal(err)
				}
				msg, err := tr.Pop(ctx, queue, time.Second)
				if err != nil || msg == nil {
					t.Fatalf("pop: msg=%v err=%v", msg, err)
				}
				if msg.Body != want {
					t.Fatalf("reserved element differs from the published bytes: %q", msg.Body)
				}
				if err := tr.Ack(ctx, msg); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

// TestRedisLayoutConformance locks the broker-bindings.md §1.1 key names of the
// Laravel-compatible layout.
func TestRedisLayoutConformance(t *testing.T) {
	m := loadRedisManifest(t)
	for _, c := range m.RedisLayout.Cases {
		t.Run(c.Name, func(t *testing.T) {
			tr := &Transport{}
			WithLaravelLayout(c.KeyPrefix)(tr)
			k := tr.Keys(c.Queue)
			got := map[string]string{"ready": k.Ready, "delayed": k.Delayed, "reserved": k.Reserved, "notify": k.Notify}
			want := map[string]string{"ready": c.Expect.Ready, "delayed": c.Expect.Delayed, "reserved": c.Expect.Reserved, "notify": c.Expect.Notify}
			for name, w := range want {
				if got[name] != w {
					t.Errorf("%s key = %q, want %q", name, got[name], w)
				}
			}
		})
	}
}
