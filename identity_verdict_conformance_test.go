package babelqueue_test

import (
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

type identityVerdictCase struct {
	Name   string `json:"name"`
	Body   string `json:"body"`
	Expect string `json:"expect"`
}

// TestConformanceIdentityVerdict runs the cross-SDK `identity_verdict` section (ADR-0036
// decision 4, ADR-0032 section 6, error-handling.md section 3): the verdict of the core decoder
// on a raw message body, for every transport outside Kafka. `ok` is a body that decodes into
// an envelope with a valid identity; `poison` is a body that does not (a decode error or no
// valid job/urn). A `poison` body is never handed to a handler; that routing is covered by
// TestConformancePulsarExhaustion (pulsar module) and the Redis consumer tests.
func TestConformanceIdentityVerdict(t *testing.T) {
	var sec struct {
		Cases []identityVerdictCase `json:"cases"`
	}
	loadManifestSection(t, "identity_verdict", &sec)
	if len(sec.Cases) == 0 {
		t.Fatal("identity_verdict section has no cases")
	}
	for _, c := range sec.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			env, err := babelqueue.Decode([]byte(c.Body))
			switch c.Expect {
			case "ok":
				if err != nil {
					t.Fatalf("decode error %v, expect ok", err)
				}
				if u := env.URN(); u == "" {
					t.Fatalf("decoded without a valid identity (URN %q), expect ok", u)
				}
			case "poison":
				if err == nil {
					t.Fatalf("body decoded (URN %q), expect poison", env.URN())
				}
			default:
				t.Fatalf("unknown expect %q", c.Expect)
			}
		})
	}
}
