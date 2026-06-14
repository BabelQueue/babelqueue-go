package machinery

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// conformanceDir is the core's vendored copy of the canonical suite. machinery is
// a Redis/AMQP-backed framework adapter (§1/§2), so the cross-SDK invariant it must
// satisfy is payload identity: the queue element — here the signature's envelope
// argument — is the canonical envelope JSON byte-for-byte, with no wrapping.
const conformanceDir = "../testdata/conformance"

type redisManifest struct {
	Redis struct {
		PayloadIdentity struct {
			EnvelopeFile string `json:"envelope_file"`
		} `json:"payload_identity"`
	} `json:"redis"`
}

func TestMachineryPayloadIdentity(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m redisManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(conformanceDir, m.Redis.PayloadIdentity.EnvelopeFile))
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}

	env, err := babelqueue.Decode(body)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	sig, err := Signature(env)
	if err != nil {
		t.Fatalf("Signature: %v", err)
	}

	// Name routes by URN.
	if sig.Name != env.URN() {
		t.Errorf("signature name = %q, want the URN %q", sig.Name, env.URN())
	}

	// Payload identity: the envelope arg is the canonical envelope, byte-for-byte.
	want, err := env.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if sig.Args[0].Value.(string) != string(want) {
		t.Errorf("envelope arg is not byte-identical\n got: %s\nwant: %s", sig.Args[0].Value, want)
	}
}
