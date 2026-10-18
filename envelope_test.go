package babelqueue_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

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

// ADR-0032 §5 brief: trimming must stay linear. 200k ASCII whitespace bytes on each side of
// the URN and the trace id are stripped well inside a second; a hand-rolled quadratic loop
// would take far longer and fail this.
func TestTrimLargePaddingIsLinear(t *testing.T) {
	pad := strings.Repeat(" \t\n\v\f\r", 200_000/6+1)[:200_000]
	start := time.Now()
	env, err := babelqueue.Make(pad+"urn:a:b"+pad, nil, babelqueue.WithTraceID(pad+"trace-1"+pad))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Make: %v", err)
	}
	if env.Job != "urn:a:b" || env.TraceID != "trace-1" {
		t.Errorf("job=%q trace_id=%q, want padding trimmed", env.Job, env.TraceID)
	}
	if elapsed > time.Second {
		t.Errorf("trimming 200k padding took %s, want < 1s", elapsed)
	}
}

// ADR-0032 §5: trimming is ASCII-only (space, \t, \n, \v, \f, \r). Unicode spaces
// such as U+00A0, U+2003 and U+3000 are content, not whitespace.
func TestTrimIsASCIIOnly(t *testing.T) {
	// ASCII padding is stripped from the URN and the trace id.
	env, err := babelqueue.Make(" \t\n\v\f\rurn:a:b \r\f\v\n\t ", nil, babelqueue.WithTraceID("\v trace-1 \f"))
	if err != nil {
		t.Fatalf("Make: %v", err)
	}
	if env.Job != "urn:a:b" || env.TraceID != "trace-1" {
		t.Errorf("job=%q trace_id=%q, want ASCII padding trimmed", env.Job, env.TraceID)
	}

	// Unicode spaces are preserved on both.
	for _, sp := range []string{"\u00a0", "\u2003", "\u3000"} {
		env, err := babelqueue.Make(sp+"urn:a:b"+sp, nil, babelqueue.WithTraceID(sp+"t"+sp))
		if err != nil {
			t.Fatalf("Make with %U: %v", []rune(sp)[0], err)
		}
		if env.Job != sp+"urn:a:b"+sp {
			t.Errorf("job = %q, want %U preserved", env.Job, []rune(sp)[0])
		}
		if env.TraceID != sp+"t"+sp {
			t.Errorf("trace_id = %q, want %U preserved", env.TraceID, []rune(sp)[0])
		}
	}

	// A trace id that is only a Unicode space is not blank: Accepts is true.
	env, err = babelqueue.Make("urn:a:b", map[string]any{})
	if err != nil {
		t.Fatalf("Make: %v", err)
	}
	env.TraceID = "\u00a0"
	if !env.Accepts() {
		t.Error("a U+00A0-only trace_id must be accepted (not ASCII-blank)")
	}
	env.TraceID = " \t\n"
	if env.Accepts() {
		t.Error("an ASCII-blank trace_id must be rejected")
	}
}
