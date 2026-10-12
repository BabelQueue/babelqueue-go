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
