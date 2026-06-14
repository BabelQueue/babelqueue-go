package asynq

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

// conformanceDir is the core's vendored copy of the canonical suite (synced from
// the conformance/ repo). asynq is a Redis-backed framework adapter (§1), so the
// cross-SDK invariant it must satisfy is Redis payload identity: the queue
// element — here the asynq task Payload — is the canonical envelope JSON
// byte-for-byte, with no wrapping and no added fields.
const conformanceDir = "../testdata/conformance"

type redisManifest struct {
	Redis struct {
		PayloadIdentity struct {
			EnvelopeFile string `json:"envelope_file"`
		} `json:"payload_identity"`
	} `json:"redis"`
}

func TestAsynqPayloadIdentity(t *testing.T) {
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

	task, err := Task(env)
	if err != nil {
		t.Fatalf("Task: %v", err)
	}

	// TypeName routes by URN.
	if task.Type() != env.URN() {
		t.Errorf("task type = %q, want the URN %q", task.Type(), env.URN())
	}

	// Payload identity: the task payload is the canonical envelope, byte-for-byte.
	// (We re-encode the decoded fixture to the canonical wire form — the same
	// transform every producing SDK applies — and compare.)
	want, err := env.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(task.Payload()) != string(want) {
		t.Errorf("task payload is not the byte-identical envelope\n got: %s\nwant: %s", task.Payload(), want)
	}
}
