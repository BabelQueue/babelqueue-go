package amqp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// conformanceDir is the core's vendored copy of the canonical suite (synced from the
// conformance/ repo). The RabbitMQ (AMQP 0-9-1) binding cases live under manifest.json ->
// "rabbitmq".
const conformanceDir = "../testdata/conformance"

type rabbitmqManifest struct {
	Rabbitmq struct {
		PropertyProjection struct {
			EnvelopeFile string            `json:"envelope_file"`
			Properties   map[string]string `json:"properties"`
			Headers      struct {
				SchemaVersion int    `json:"x-schema-version"`
				SourceLang    string `json:"x-source-lang"`
				Attempts      int    `json:"x-attempts"`
			} `json:"headers"`
		} `json:"property_projection"`
	} `json:"rabbitmq"`
}

// TestRabbitMQConformance locks the §2 AMQP 0-9-1 property projection against the vendored
// canonical suite: type = URN, correlation_id = trace_id, message_id = meta.id, app_id, plus the
// native-typed x- headers. The publishing() projection is a pure function — no broker, no network.
func TestRabbitMQConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceDir, "manifest.json"))
	if err != nil {
		t.Skipf("vendored conformance manifest not found: %v", err)
	}
	var m rabbitmqManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	proj := m.Rabbitmq.PropertyProjection

	body, err := os.ReadFile(filepath.Join(conformanceDir, proj.EnvelopeFile))
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}

	pub := (&Transport{}).publishing(string(body), nil)

	checks := map[string]struct{ got, want string }{
		"type":           {pub.Type, proj.Properties["type"]},
		"correlation_id": {pub.CorrelationId, proj.Properties["correlation_id"]},
		"message_id":     {pub.MessageId, proj.Properties["message_id"]},
		"app_id":         {pub.AppId, proj.Properties["app_id"]},
		"content_type":   {pub.ContentType, proj.Properties["content_type"]},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", name, c.got, c.want)
		}
	}

	if v, _ := pub.Headers["x-schema-version"].(int); v != proj.Headers.SchemaVersion {
		t.Errorf("x-schema-version = %v, want %d", pub.Headers["x-schema-version"], proj.Headers.SchemaVersion)
	}
	if v, _ := pub.Headers["x-source-lang"].(string); v != proj.Headers.SourceLang {
		t.Errorf("x-source-lang = %v, want %q", pub.Headers["x-source-lang"], proj.Headers.SourceLang)
	}
	if v, _ := pub.Headers["x-attempts"].(int); v != proj.Headers.Attempts {
		t.Errorf("x-attempts = %v, want %d", pub.Headers["x-attempts"], proj.Headers.Attempts)
	}
}
