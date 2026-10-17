package babelqueue

import (
	"encoding/json"
	"strings"
	"testing"
)

// Envelope keys are matched with their exact, case-sensitive spelling and a repeated key
// follows "last wins" without merging, like every other SDK core (conformance
// invalid-wrong-case-job / roundtrip-wrong-case-keys). These tests pin both rules.

func TestDecodeExactCaseAndLastWins(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		job      string
		dataKeys []string
		queue    string
		metaID   string
		attempts int
		extras   []string
		metaXtra []string
	}{
		{name: "wrong-case job only", body: `{"JOB":"urn:a","trace_id":"t","data":{},"meta":{},"attempts":0}`,
			job: "", extras: []string{"JOB"}},
		{name: "job then JOB", body: `{"job":"urn:a","JOB":"urn:b"}`,
			job: "urn:a", extras: []string{"JOB"}},
		{name: "JOB then job", body: `{"JOB":"urn:b","job":"urn:a"}`,
			job: "urn:a", extras: []string{"JOB"}},
		{name: "duplicate job last wins", body: `{"job":"urn:a","job":"urn:b"}`,
			job: "urn:b"},
		{name: "job then null falls back to the urn alias", body: `{"job":"urn:a","job":null,"urn":"urn:u"}`,
			job: "urn:u"},
		{name: "wrong-case urn alias is not an alias", body: `{"URN":"urn:u"}`,
			job: "", extras: []string{"URN"}},
		{name: "duplicate urn alias last wins", body: `{"urn":"urn:u1","urn":"urn:u2"}`,
			job: "urn:u2"},
		{name: "duplicate data is not merged", body: `{"job":"urn:a","data":{"a":1},"data":{"b":2}}`,
			job: "urn:a", dataKeys: []string{"b"}},
		{name: "data then null", body: `{"job":"urn:a","data":{"a":1},"data":null}`,
			job: "urn:a"},
		{name: "wrong-case data is an extra", body: `{"job":"urn:a","DATA":{"a":1}}`,
			job: "urn:a", extras: []string{"DATA"}},
		{name: "duplicate meta replaces the whole block", body: `{"job":"urn:a","meta":{"id":"x","queue":"q"},"meta":{"id":"y"}}`,
			job: "urn:a", metaID: "y"},
		{name: "wrong-case meta keys are meta extras", body: `{"job":"urn:a","Meta":{"id":"x"},"meta":{"ID":"z","Queue":"q"}}`,
			job: "urn:a", extras: []string{"Meta"}, metaXtra: []string{"ID", "Queue"}},
		{name: "wrong-case attempts is an extra", body: `{"job":"urn:a","Attempts":3,"attempts":1}`,
			job: "urn:a", attempts: 1, extras: []string{"Attempts"}},
		{name: "wrong-case trace_id is an extra", body: `{"job":"urn:a","TRACE_ID":"t"}`,
			job: "urn:a", extras: []string{"TRACE_ID"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env, err := Decode([]byte(c.body))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if env.Job != c.job {
				t.Errorf("job = %q, want %q", env.Job, c.job)
			}
			if c.job == "" && env.Accepts() {
				t.Error("Accepts() = true for an envelope without a job")
			}
			if len(env.Data) != len(c.dataKeys) {
				t.Errorf("data = %v, want keys %v", env.Data, c.dataKeys)
			}
			for _, k := range c.dataKeys {
				if _, ok := env.Data[k]; !ok {
					t.Errorf("data lacks %q: %v", k, env.Data)
				}
			}
			if env.Meta.ID != c.metaID {
				t.Errorf("meta.id = %q, want %q", env.Meta.ID, c.metaID)
			}
			if env.Meta.Queue != c.queue {
				t.Errorf("meta.queue = %q, want %q", env.Meta.Queue, c.queue)
			}
			if env.Attempts != c.attempts {
				t.Errorf("attempts = %d, want %d", env.Attempts, c.attempts)
			}
			for _, k := range c.extras {
				if _, ok := env.extras[k]; !ok {
					t.Errorf("extras lack %q: %v", k, env.extras)
				}
			}
			for _, k := range c.metaXtra {
				if _, ok := env.metaExtras[k]; !ok {
					t.Errorf("meta extras lack %q: %v", k, env.metaExtras)
				}
			}
		})
	}
}

func TestDecodeDuplicateDeadLetterLastWins(t *testing.T) {
	env, err := Decode([]byte(`{"job":"urn:a",` +
		`"dead_letter":{"reason":"failed","attempts":3},` +
		`"dead_letter":{"reason":"poison"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if env.DeadLetter == nil || env.DeadLetter.Reason != "poison" || env.DeadLetter.Attempts != 0 {
		t.Fatalf("dead_letter = %+v, want the last block alone (reason poison, attempts 0)", env.DeadLetter)
	}
	env, err = Decode([]byte(`{"job":"urn:a","dead_letter":{"reason":"failed"},"dead_letter":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if env.DeadLetter != nil {
		t.Fatalf("dead_letter = %+v, want nil after a trailing null", env.DeadLetter)
	}
}

func TestDecodeWrongTypeInAnyDuplicateIsPoison(t *testing.T) {
	for _, body := range []string{
		`{"job":"urn:a","job":5}`,
		`{"job":5,"job":"urn:a"}`,
		`{"job":"urn:a","attempts":1,"attempts":"x"}`,
	} {
		if _, err := Decode([]byte(body)); err == nil {
			t.Errorf("%s decoded without error, want a type error", body)
		}
	}
	// A wrongly cased key never has its type checked: it is an unknown key.
	for _, body := range []string{`{"JOB":5}`, `{"Meta":{"ID":5}}`, `{"ATTEMPTS":"x"}`} {
		if _, err := Decode([]byte(body)); err != nil {
			t.Errorf("%s: %v, want no error (unknown key)", body, err)
		}
	}
}

func TestWrongCaseKeysRoundTripAsUnknownKeys(t *testing.T) {
	raw := `{"job":"urn:a","trace_id":"t","data":{"k":1},` +
		`"meta":{"id":"i","queue":"q","lang":"go","schema_version":1,"created_at":1,"Region":"eu"},` +
		`"attempts":2,"JOB":"urn:b","Data":{"x":true}}`
	env, err := Decode([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	out, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	if string(doc["JOB"]) != `"urn:b"` || string(doc["Data"]) != `{"x":true}` {
		t.Fatalf("unknown wrong-case keys not re-emitted verbatim: %s", out)
	}
	if string(doc["job"]) != `"urn:a"` {
		t.Fatalf("canonical job changed: %s", out)
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(doc["meta"], &meta); err != nil {
		t.Fatal(err)
	}
	if string(meta["Region"]) != `"eu"` {
		t.Fatalf("meta unknown key lost: %s", doc["meta"])
	}
	// A second pass is a fixed point.
	env2, err := Decode(out)
	if err != nil {
		t.Fatal(err)
	}
	out2, err := env2.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(out2) {
		t.Fatalf("re-encode is not stable:\n%s\n%s", out, out2)
	}
}

func TestDecodeDisplacedMetaBlockDropsItsForbiddenWarnings(t *testing.T) {
	cases := []struct {
		name string
		body string
		want []string
	}{
		{"displaced block warning is gone", `{"job":"urn:a","meta":{"ts":1},"meta":{"id":"x"}}`, nil},
		{"final block warning stays", `{"job":"urn:a","meta":{"id":"x"},"meta":{"ts":1}}`, []string{"/meta/ts"}},
		{"null meta clears the earlier block", `{"job":"urn:a","meta":{"ts":1},"meta":null}`, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env, err := Decode([]byte(c.body))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			got := env.Warnings()
			if len(got) != len(c.want) {
				t.Fatalf("warnings = %v, want %v", got, c.want)
			}
			for i := range got {
				if !strings.Contains(got[i], c.want[i]) {
					t.Errorf("warnings[%d] = %q, want it to mention %q", i, got[i], c.want[i])
				}
			}
		})
	}
}

// --- ADR-0031 (c): blank job falls back to urn; job+urn drops urn with a warning ---

const blankJobTail = `"trace_id":"7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b","data":{},` +
	`"meta":{"id":"f1e2d3c4-b5a6-4789-90ab-cdef01234567","queue":"orders","lang":"php",` +
	`"schema_version":1,"created_at":1749132727000},"attempts":0}`

func TestDecodeBlankASCIIJobFallsBackToURN(t *testing.T) {
	for _, job := range []string{`""`, `" "`, `"\t"`, `"\n"`, `"\u000b"`, `"\f"`, `"\r"`, `" \t\r\n "`} {
		body := `{"job":` + job + `,"urn":"urn:acme:b",` + blankJobTail
		env, err := Decode([]byte(body))
		if err != nil {
			t.Fatalf("job %s: %v", job, err)
		}
		if env.Job != "urn:acme:b" || !env.Accepts() {
			t.Errorf("job %s: Job=%q Accepts=%v, want urn:acme:b/true", job, env.Job, env.Accepts())
		}
		if w := env.Warnings(); len(w) != 0 {
			t.Errorf("job %s: unexpected warnings %q", job, w)
		}
		out, err := env.Encode()
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]json.RawMessage
		_ = json.Unmarshal(out, &doc)
		if _, ok := doc["urn"]; ok {
			t.Errorf("job %s: urn key retained: %s", job, out)
		}
	}
}

func TestDecodeURNOnlyAndMissingJob(t *testing.T) {
	env, err := Decode([]byte(`{"urn":"urn:acme:b",` + blankJobTail))
	if err != nil || env.Job != "urn:acme:b" || !env.Accepts() || len(env.Warnings()) != 0 {
		t.Errorf("urn-only: %+v err=%v", env, err)
	}
	// A blank job with no usable urn is not accepted.
	for _, body := range []string{`{"job":" ",` + blankJobTail, `{"job":"","urn":"  ",` + blankJobTail} {
		env, err := Decode([]byte(body))
		if err == nil && env.Accepts() {
			t.Errorf("blank job without urn accepted: %s", body)
		}
	}
}

func TestDecodeNonASCIIWhitespaceJobIsNotBlank(t *testing.T) {
	// U+2003 EM SPACE is not in the closed ASCII blank set: job wins, urn is dropped.
	body := `{"job":"\u2003","urn":"urn:acme:b",` + blankJobTail
	env, err := Decode([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if env.Job != "\u2003" {
		t.Errorf("Job = %q, want the U+2003 value untouched", env.Job)
	}
	warned := false
	for _, w := range env.Warnings() {
		warned = warned || strings.Contains(w, "/urn")
	}
	if !warned {
		t.Errorf("no warning names /urn: %q", env.Warnings())
	}
}

func TestDecodeJobAndURNJobWinsURNDroppedWithWarning(t *testing.T) {
	body := `{"job":"urn:acme:a","urn":"urn:acme:other",` + blankJobTail
	env, err := Decode([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if env.Job != "urn:acme:a" {
		t.Errorf("Job = %q, want urn:acme:a", env.Job)
	}
	warned := false
	for _, w := range env.Warnings() {
		warned = warned || strings.Contains(w, "/urn")
	}
	if !warned {
		t.Errorf("no warning names /urn: %q", env.Warnings())
	}
	out, err := env.Encode()
	if err != nil {
		t.Fatalf("re-encode must succeed (urn was dropped, not retained): %v", err)
	}
	if strings.Contains(string(out), `"urn"`) {
		t.Errorf("urn re-emitted: %s", out)
	}
	// The same rule holds on the json.Unmarshal path.
	var viaUnmarshal Envelope
	if err := json.Unmarshal([]byte(body), &viaUnmarshal); err != nil {
		t.Fatal(err)
	}
	if len(viaUnmarshal.Warnings()) == 0 {
		t.Error("json.Unmarshal path lost the /urn warning")
	}
}

// --- Review round 1: the alias is taken verbatim and warned about when unused ---

func TestDecodeAliasIsTakenVerbatim(t *testing.T) {
	// GR-5: Java, .NET and Node write the alias into Job as-is; Go must not trim it.
	for _, alias := range []string{" urn:acme:b ", "\turn:acme:b", "urn:acme:b\n", "\u2003urn:acme:b"} {
		quoted, _ := json.Marshal(alias)
		env, err := Decode([]byte(`{"job":"","urn":` + string(quoted) + `,` + blankJobTail))
		if err != nil {
			t.Fatalf("alias %q: %v", alias, err)
		}
		if env.Job != alias {
			t.Errorf("alias %q: Job=%q, want the alias verbatim", alias, env.Job)
		}
		if w := env.Warnings(); len(w) != 0 {
			t.Errorf("alias %q: unexpected warnings %q", alias, w)
		}
		out, err := env.Encode()
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]json.RawMessage
		_ = json.Unmarshal(out, &doc)
		if string(doc["job"]) != string(quoted) {
			t.Errorf("alias %q: re-emitted job %s, want %s", alias, doc["job"], quoted)
		}
	}
}

func TestDecodeWhitespaceAliasBecomesBlankJob(t *testing.T) {
	// A whitespace-only string alias still replaces the blank job (verbatim) and is
	// then rejected by the consumer as a blank identity, with no spurious warning.
	env, err := Decode([]byte(`{"job":"","urn":" ",` + blankJobTail))
	if err != nil {
		t.Fatal(err)
	}
	if env.Job != " " || env.Accepts() || len(env.Warnings()) != 0 {
		t.Errorf("Job=%q Accepts=%v Warnings=%q", env.Job, env.Accepts(), env.Warnings())
	}
}

func TestDecodeNonStringAliasWarns(t *testing.T) {
	// A non-string urn is no alias: it is dropped, and (like Java/.NET) with a warning.
	for _, urn := range []string{`123`, `null`, `true`, `{"a":1}`, `["x"]`} {
		for _, job := range []string{`""`, `" "`} {
			env, err := Decode([]byte(`{"job":` + job + `,"urn":` + urn + `,` + blankJobTail))
			if err != nil {
				t.Fatalf("urn %s: %v", urn, err)
			}
			if env.Accepts() {
				t.Errorf("job %s urn %s: accepted with Job=%q", job, urn, env.Job)
			}
			w := env.Warnings()
			if len(w) != 1 || !strings.Contains(w[0], "/urn") {
				t.Errorf("job %s urn %s: warnings %q, want one naming /urn", job, urn, w)
			}
		}
	}
}
