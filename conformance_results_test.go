package babelqueue_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// This file is the conformance results writer (ADR-0032 section 2). It is dormant unless
// BQ_CONFORMANCE_RESULTS names an output path. When set, it runs the per-section
// conformance tests of this module and of every transport module, maps each case's
// subtest outcome onto the case ids the manifest defines, and writes a results.json that
// conforms to schema/results.schema.json.
//
// The runners themselves live next to the code they exercise (core, idempotency/,
// schema/, sqs/, kafka/, redis/, ...). This writer only orchestrates and reports; it
// never decides a case passed on its own.

// sectionRunner names the test function that executes one manifest section, and the
// directory (relative to this module root) of the Go module or package that holds it.
type sectionRunner struct {
	dir  string
	test string
}

// sectionRunners maps every manifest section this SDK can execute to its runner. A
// section that is in scope for a carried binding but absent here is reported as a
// compliance gap, never silently dropped.
var sectionRunners = map[string]sectionRunner{
	"cases":                  {".", "TestConformance"},
	"roundtrip":              {".", "TestConformanceRoundtrip"},
	"data_shape":             {".", "TestConformanceDataShape"},
	"forbidden_keys":         {".", "TestConformanceForbiddenKeys"},
	"extension_write":        {".", "TestConformanceExtensionWrite"},
	"encode_reject":          {".", "TestConformanceEncodeReject"},
	"trim":                   {".", "TestConformanceTrim"},
	"job_type":               {".", "TestConformanceJobType"},
	"idempotency":            {"idempotency", "TestIdempotencyConformance"},
	"payload_schema":         {"schema", "TestPayloadConformance"},
	"payload_schema_unicode": {"schema", "TestPayloadUnicodeConformance"},
	"sqs":                    {"sqs", "TestSqsConformance"},
	"delay":                  {"sqs", "TestSqsDelayConformance"},
	"kafka":                  {"kafka", "TestKafkaConformance"},
	"backoff":                {"kafka", "TestKafkaBackoffConformance"},
	"redis":                  {"redis", "TestRedisConformance"},
	"redis_layout":           {"redis", "TestRedisLayoutConformance"},
	"rabbitmq":               {"amqp", "TestRabbitMQConformance"},
	"asb":                    {"azureservicebus", "TestAsbConformance"},
	"pulsar":                 {"pulsar", "TestPulsarConformance"},
	"artemis":                {"artemis", "TestArtemisConformance"},
	"traceparent":            {"otel", "TestTraceparentConformance"},
}

// moduleBindings derives the results.json `bindings` from the repo's own Go modules:
// every directory holding a go.mod is a transport module (or a known non-transport
// helper). Nothing here is hand-listed per release; an unclassified module fails the
// run so a new transport cannot be forgotten.
//
// KEEP IN SYNC with MODULE_BINDINGS / NON_BINDING_MODULES in
// scripts/check-conformance-results.py: the release gate re-derives the same set
// independently, so a new module must be classified in both places (the gate fails on a
// module it does not know).
var moduleBindings = map[string]string{
	"redis":           "redis",
	"amqp":            "amqp",
	"sqs":             "sqs",
	"azureservicebus": "asb",
	"pulsar":          "pulsar",
	"kafka":           "kafka",
	"artemis":         "artemis",
	"otel":            "otel",
}

// nonTransportModules are Go modules that carry no broker binding: queue-library
// adapters and idempotency-store helpers (they reuse the core envelope unchanged).
var nonTransportModules = map[string]bool{
	"asynq":                true,
	"machinery":            true,
	"idempotency-redis":    true,
	"idempotency-postgres": true,
}

type caseResult struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

type resultsFile struct {
	SDK                string                           `json:"sdk"`
	Version            string                           `json:"version"`
	ConformanceVersion string                           `json:"conformance_version"`
	GeneratedAt        string                           `json:"generated_at"`
	Bindings           []string                         `json:"bindings"`
	Sections           map[string]map[string]caseResult `json:"sections"`
}

func TestConformanceResults(t *testing.T) {
	out := os.Getenv("BQ_CONFORMANCE_RESULTS")
	if out == "" {
		t.Skip("BQ_CONFORMANCE_RESULTS not set; results.json is not written")
	}

	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	vendor := filepath.Join(root, "testdata", "conformance")
	manifest := loadRawManifest(t, filepath.Join(vendor, "manifest.json"))

	bindings := deriveBindings(t, root)

	// Section -> scope, straight from the manifest `sections` map (not from the
	// manifest's top-level keys; `sections` and `exceptions` are not sections).
	scopes := map[string]string{}
	for name, raw := range manifest["sections"].(map[string]any) {
		scopes[name] = raw.(map[string]any)["scope"].(string)
	}
	carried := map[string]bool{"core": true}
	for _, b := range bindings {
		carried[b] = true
	}

	var inScope []string
	for name, scope := range scopes {
		if carried[scope] {
			inScope = append(inScope, name)
		}
	}
	sort.Strings(inScope)

	// Run each distinct (dir, test) once and keep every direct and nested subtest outcome.
	type key struct{ dir, test string }
	outcomes := map[key]map[string]caseResult{}
	runErr := map[key]string{}
	for _, name := range inScope {
		r, ok := sectionRunners[name]
		if !ok {
			t.Errorf("compliance gap: section %q (scope %q) is in scope but has no runner", name, scopes[name])
			continue
		}
		k := key(r)
		if _, done := outcomes[k]; done {
			continue
		}
		outcomes[k], runErr[k] = runGoTest(t, root, r.dir, r.test)
	}

	res := resultsFile{
		SDK:                "babelqueue-go",
		Version:            sdkVersion(),
		ConformanceVersion: conformanceVersion(t, vendor, manifest),
		GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
		Bindings:           bindings,
		Sections:           map[string]map[string]caseResult{},
	}

	for _, name := range inScope {
		r, ok := sectionRunners[name]
		if !ok {
			continue
		}
		k := key(r)
		sec := map[string]caseResult{}
		for _, id := range manifestCaseIDs(name, manifest) {
			sec[id] = outcomeFor(outcomes[k], r.test, id, runErr[k])
		}
		res.Sections[name] = sec
	}

	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}

	// The file is written first; only then do failures break the run.
	total, failed := 0, 0
	for name, sec := range res.Sections {
		for id, c := range sec {
			total++
			if c.Status == "fail" {
				failed++
				t.Errorf("conformance fail: %s/%s: %s", name, id, c.Reason)
			}
		}
	}
	t.Logf("wrote %s: %d sections, %d cases, %d failed, bindings %v", out, len(res.Sections), total, failed, res.Bindings)
}

func loadRawManifest(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("vendored manifest: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	return m
}

// deriveBindings lists the bindings of every transport module found in the repo.
func deriveBindings(t *testing.T, root string) []string {
	t.Helper()
	mods, err := filepath.Glob(filepath.Join(root, "*", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, mod := range mods {
		dir := filepath.Base(filepath.Dir(mod))
		if nonTransportModules[dir] {
			continue
		}
		b, ok := moduleBindings[dir]
		if !ok {
			t.Errorf("module %q is not classified as a transport or a helper; add it to moduleBindings or nonTransportModules", dir)
			continue
		}
		seen[b] = true
	}
	out := make([]string, 0, len(seen))
	for b := range seen {
		out = append(out, b)
	}
	sort.Strings(out)
	return out
}

// sdkVersion resolves the package version: Go has no in-tree version source (the module
// version is the git tag), so the release tag is used on a tag build, else "dev".
func sdkVersion() string {
	if name := os.Getenv("GITHUB_REF_NAME"); name != "" && os.Getenv("GITHUB_REF_TYPE") != "branch" {
		return name
	}
	return "dev"
}

func conformanceVersion(t *testing.T, vendor string, manifest map[string]any) string {
	t.Helper()
	if raw, err := os.ReadFile(filepath.Join(vendor, "CONFORMANCE_VERSION")); err == nil {
		if v := strings.TrimSpace(string(raw)); v != "" {
			return v
		}
	}
	if v, ok := manifest["conformance_version"].(string); ok {
		return v
	}
	t.Fatal("no conformance version found")
	return ""
}

// manifestCaseIDs lists a section's case ids by the rule in results.schema.json.
func manifestCaseIDs(section string, manifest map[string]any) []string {
	var body map[string]any
	if section == "cases" {
		body = map[string]any{"cases": manifest["cases"]}
	} else {
		body, _ = manifest[section].(map[string]any)
	}
	var ids []string
	if arr, ok := body["cases"].([]any); ok {
		for i, c := range arr {
			ids = append(ids, caseName(c, fmt.Sprintf("%s#%d", section, i)))
		}
	}
	keys := make([]string, 0, len(body))
	for k := range body {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if k == "description" || k == "schema" || k == "cases" {
			continue
		}
		sub, ok := body[k].(map[string]any)
		if !ok {
			continue
		}
		if arr, ok := sub["cases"].([]any); ok {
			for i, c := range arr {
				ids = append(ids, k+"/"+caseName(c, fmt.Sprint(i)))
			}
			continue
		}
		ids = append(ids, k)
	}
	return ids
}

func caseName(c any, fallback string) string {
	if m, ok := c.(map[string]any); ok {
		if n, ok := m["name"].(string); ok && n != "" {
			return n
		}
	}
	return fallback
}

type goTestEvent struct {
	Action string
	Test   string
	Output string
}

var adrRef = regexp.MustCompile(`ADR-[0-9]{4}`)

// runGoTest runs one test function in dir and returns every subtest outcome keyed by the
// path below the test function ("sub/subsub"). The second value is a non-empty note when
// the run itself could not complete.
func runGoTest(t *testing.T, root, dir, test string) (map[string]caseResult, string) {
	t.Helper()
	cmd := exec.Command("go", "test", "-json", "-count=1", "-run", "^"+test+"$", ".")
	cmd.Dir = filepath.Join(root, dir)
	env := []string{}
	for _, e := range os.Environ() {
		// The child must not re-enter this writer.
		if !strings.HasPrefix(e, "BQ_CONFORMANCE_RESULTS=") {
			env = append(env, e)
		}
	}
	if os.Getenv("CGO_ENABLED") == "" {
		env = append(env, "CGO_ENABLED=0")
	}
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	runErr := cmd.Run()

	outcomes := map[string]caseResult{}
	output := map[string][]string{}
	var buildNote []string
	sc := bufio.NewScanner(&stdout)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var ev goTestEvent
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			buildNote = append(buildNote, strings.TrimSpace(sc.Text()))
			continue
		}
		if ev.Test == "" {
			if ev.Action == "output" {
				buildNote = append(buildNote, strings.TrimSpace(ev.Output))
			}
			continue
		}
		switch ev.Action {
		case "output":
			output[ev.Test] = append(output[ev.Test], strings.TrimSpace(ev.Output))
		case "pass", "fail", "skip":
			if ev.Test == test {
				continue
			}
			path := strings.TrimPrefix(ev.Test, test+"/")
			c := caseResult{Status: ev.Action}
			switch ev.Action {
			case "fail":
				c.Reason = lastLines(output[ev.Test], 4)
			case "skip":
				c.Reason = lastLines(output[ev.Test], 2)
			}
			outcomes[path] = c
		}
	}
	note := ""
	if runErr != nil && len(outcomes) == 0 {
		note = fmt.Sprintf("go test in %s exited with %v: %s %s", dir, runErr,
			lastLines(buildNote, 4), strings.TrimSpace(stderr.String()))
	}
	return outcomes, note
}

func lastLines(lines []string, n int) string {
	var keep []string
	for _, l := range lines {
		if l != "" && !strings.HasPrefix(l, "=== ") && !strings.HasPrefix(l, "--- ") {
			keep = append(keep, l)
		}
	}
	if len(keep) > n {
		keep = keep[len(keep)-n:]
	}
	return strings.Join(keep, " | ")
}

// outcomeFor resolves one manifest case id against the subtests the runner produced. A
// case with no subtest is a fail (never a silent pass), and a Go skip is only accepted as
// a skip when it cites an authorising ADR; otherwise it is an unexplained gap and fails.
func outcomeFor(outcomes map[string]caseResult, test, id, runNote string) caseResult {
	got, ok := outcomes[strings.ReplaceAll(id, " ", "_")]
	if !ok {
		reason := fmt.Sprintf("%s produced no subtest for this case", test)
		if runNote != "" {
			reason = runNote
		}
		return caseResult{Status: "fail", Reason: reason}
	}
	switch got.Status {
	case "skip":
		if adrRef.MatchString(got.Reason) {
			return got
		}
		return caseResult{Status: "fail", Reason: "case skipped without an ADR-cited reason: " + got.Reason}
	case "fail":
		if got.Reason == "" {
			got.Reason = "subtest failed"
		}
	}
	return got
}
