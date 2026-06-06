package babelqueue_test

import (
	"encoding/json"
	"strings"
	"testing"

	babelqueue "github.com/babelqueue/babelqueue-go"
)

func TestMakeProducesCanonicalShape(t *testing.T) {
	env, err := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1042})
	if err != nil {
		t.Fatalf("Make: %v", err)
	}
	if env.Job != "urn:babel:orders:created" {
		t.Errorf("job = %q", env.Job)
	}
	if env.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", env.Attempts)
	}
	if env.Meta.Lang != babelqueue.SourceLang {
		t.Errorf("lang = %q, want %q", env.Meta.Lang, babelqueue.SourceLang)
	}
	if env.Meta.SchemaVersion != babelqueue.SchemaVersion {
		t.Errorf("schema_version = %d", env.Meta.SchemaVersion)
	}
	if env.Meta.Queue != "default" {
		t.Errorf("queue = %q, want default", env.Meta.Queue)
	}
	if env.TraceID == "" || env.Meta.ID == "" {
		t.Fatal("trace_id and meta.id must be minted")
	}
	if env.TraceID == env.Meta.ID {
		t.Error("trace_id and meta.id must be distinct")
	}
	if env.Meta.CreatedAt <= 0 {
		t.Error("created_at must be a positive unix-ms timestamp")
	}
}

func TestMakeEmptyURNFails(t *testing.T) {
	if _, err := babelqueue.Make("   ", nil); err != babelqueue.ErrEmptyURN {
		t.Fatalf("want ErrEmptyURN, got %v", err)
	}
}

func TestWithQueueAndTraceID(t *testing.T) {
	env, err := babelqueue.Make(
		"urn:babel:orders:created",
		map[string]any{"order_id": 1},
		babelqueue.WithQueue("orders"),
		babelqueue.WithTraceID("trace-123"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if env.Meta.Queue != "orders" {
		t.Errorf("queue = %q", env.Meta.Queue)
	}
	if env.TraceID != "trace-123" {
		t.Errorf("trace_id = %q", env.TraceID)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	env, _ := babelqueue.Make(
		"urn:babel:orders:created",
		map[string]any{"order_id": 1042},
		babelqueue.WithQueue("orders"),
	)
	body, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := babelqueue.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.URN() != env.URN() || got.TraceID != env.TraceID || got.Meta.ID != env.Meta.ID {
		t.Error("round-trip lost identity fields")
	}
	if !got.Accepts() {
		t.Error("round-tripped envelope must be accepted")
	}
	if oid, ok := got.Data["order_id"].(float64); !ok || oid != 1042 {
		t.Errorf("data.order_id = %v", got.Data["order_id"])
	}
}

func TestEncodeFieldOrderAndNoHTMLEscaping(t *testing.T) {
	env, _ := babelqueue.Make(
		"urn:babel:catalog:item.indexed",
		map[string]any{"title": "A & B <tag>"},
		babelqueue.WithTraceID("t"),
	)
	body, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	s := string(body)
	// HTML escaping must be off: with it on, encoding/json would emit the
	// unicode-escaped forms of & and <, so this literal substring would be absent.
	if !strings.Contains(s, "A & B <tag>") {
		t.Errorf("HTML characters must be emitted literally (unescaped): %s", s)
	}
	if !strings.HasPrefix(s, `{"job":`) {
		t.Errorf("job must be the first field: %s", s)
	}
	if strings.Contains(s, "dead_letter") {
		t.Errorf("a produced envelope must omit dead_letter: %s", s)
	}
}

func TestDecodeURNAlias(t *testing.T) {
	raw := []byte(`{"urn":"urn:babel:orders:created","trace_id":"t","data":{},` +
		`"meta":{"id":"i","queue":"q","lang":"go","schema_version":1,"created_at":1},"attempts":0}`)
	env, err := babelqueue.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if env.URN() != "urn:babel:orders:created" {
		t.Errorf("urn alias not resolved: %q", env.URN())
	}
	if !env.Accepts() {
		t.Error("urn-alias envelope must be accepted")
	}
}

func TestAcceptsRejects(t *testing.T) {
	base := func() babelqueue.Envelope {
		e, _ := babelqueue.Make("urn:babel:orders:created", map[string]any{"x": 1})
		return e
	}
	if !base().Accepts() {
		t.Fatal("baseline must be accepted")
	}

	noURN := base()
	noURN.Job = ""
	if noURN.Accepts() {
		t.Error("empty URN must be rejected")
	}

	badVer := base()
	badVer.Meta.SchemaVersion = 2
	if badVer.Accepts() {
		t.Error("unknown schema_version must be rejected")
	}

	blankTrace := base()
	blankTrace.TraceID = "  "
	if blankTrace.Accepts() {
		t.Error("blank trace_id must be rejected")
	}

	noData := base()
	noData.Data = nil
	if noData.Accepts() {
		t.Error("missing data must be rejected")
	}
}

func TestAnnotateAttachesDeadLetter(t *testing.T) {
	env, _ := babelqueue.Make(
		"urn:babel:orders:created",
		map[string]any{"order_id": 1},
		babelqueue.WithQueue("orders"),
	)

	dl := babelqueue.Annotate(env, "failed", "orders", 3, errExample{})
	if dl.DeadLetter == nil {
		t.Fatal("dead_letter must be attached")
	}
	if dl.DeadLetter.Reason != "failed" ||
		dl.DeadLetter.OriginalQueue != "orders" ||
		dl.DeadLetter.Attempts != 3 ||
		dl.DeadLetter.Lang != babelqueue.SourceLang {
		t.Errorf("dead_letter = %+v", dl.DeadLetter)
	}
	if dl.DeadLetter.Error == nil || *dl.DeadLetter.Error != "boom" {
		t.Errorf("error = %v", dl.DeadLetter.Error)
	}
	if env.DeadLetter != nil {
		t.Error("Annotate must not mutate the original envelope")
	}

	body, _ := dl.Encode()
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(body, &probe); err != nil {
		t.Fatal(err)
	}
	if _, ok := probe["dead_letter"]; !ok {
		t.Errorf("dead_letter not serialized: %s", body)
	}
}

type errExample struct{}

func (errExample) Error() string { return "boom" }
