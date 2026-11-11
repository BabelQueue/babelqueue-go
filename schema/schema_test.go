package schema

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func parse(t *testing.T, src string) *Schema {
	t.Helper()
	s, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return s
}

func TestValidate_ObjectRequiredTypesAndAdditional(t *testing.T) {
	s := parse(t, `{
		"type":"object",
		"required":["order_id"],
		"properties":{"order_id":{"type":"integer"},"note":{"type":"string","minLength":1}},
		"additionalProperties":false
	}`)
	if errs := s.Validate(map[string]any{"order_id": 7.0}); len(errs) != 0 {
		t.Fatalf("expected valid, got %v", errs)
	}
	if errs := s.Validate(map[string]any{}); len(errs) == 0 {
		t.Fatal("expected a missing-required violation")
	}
	if errs := s.Validate(map[string]any{"order_id": "x"}); len(errs) == 0 {
		t.Fatal("expected an integer-type violation")
	}
	if errs := s.Validate(map[string]any{"order_id": 7.0, "extra": 1.0}); len(errs) == 0 {
		t.Fatal("expected an additionalProperties violation")
	}
	if errs := s.Validate(map[string]any{"order_id": 7.0, "note": ""}); len(errs) == 0 {
		t.Fatal("expected a minLength violation")
	}
}

func TestValidate_EnumConstMinimumArray(t *testing.T) {
	s := parse(t, `{
		"type":"object",
		"properties":{
			"status":{"enum":["new","paid"]},
			"qty":{"type":"integer","minimum":1},
			"tags":{"type":"array","items":{"type":"string"}}
		}
	}`)
	if errs := s.Validate(map[string]any{"status": "paid", "qty": 2.0, "tags": []any{"a", "b"}}); len(errs) != 0 {
		t.Fatalf("expected valid, got %v", errs)
	}
	if errs := s.Validate(map[string]any{"status": "cancelled"}); len(errs) == 0 {
		t.Fatal("expected an enum violation")
	}
	if errs := s.Validate(map[string]any{"qty": 0.0}); len(errs) == 0 {
		t.Fatal("expected a minimum violation")
	}
	if errs := s.Validate(map[string]any{"tags": []any{"a", 1.0}}); len(errs) == 0 {
		t.Fatal("expected an array-item type violation")
	}
}

func TestValidate_Const(t *testing.T) {
	s := parse(t, `{"const":"v1"}`)
	if errs := s.Validate("v1"); len(errs) != 0 {
		t.Fatalf("matching const should validate: %v", errs)
	}
	if errs := s.Validate("v2"); len(errs) == 0 {
		t.Fatal("mismatched const should fail")
	}
}

func TestValidate_ScalarTypes(t *testing.T) {
	cases := []struct {
		src   string
		value any
		valid bool
	}{
		{`{"type":"boolean"}`, true, true},
		{`{"type":"boolean"}`, "x", false},
		{`{"type":"null"}`, nil, true},
		{`{"type":"null"}`, 1.0, false},
		{`{"type":"number","minimum":0.5}`, 0.6, true},
		{`{"type":"number","minimum":0.5}`, 0.4, false},
		{`{"type":"number"}`, "x", false},
		{`{"type":"string"}`, 5.0, false},
		{`{"type":"integer"}`, 1.0, true},
		{`{"type":"integer"}`, 1.5, false},
		{`{"type":"object"}`, "x", false},
		{`{"type":"array"}`, "x", false},
	}
	for _, c := range cases {
		errs := parse(t, c.src).Validate(c.value)
		if c.valid && len(errs) != 0 {
			t.Errorf("%s with %v: expected valid, got %v", c.src, c.value, errs)
		}
		if !c.valid && len(errs) == 0 {
			t.Errorf("%s with %v: expected a violation, got none", c.src, c.value)
		}
	}
}

func TestParse_Invalid(t *testing.T) {
	if _, err := Parse([]byte("not json")); err == nil {
		t.Fatal("invalid JSON should error")
	}
}

func TestGDPRSensitive_ParseAndValidationNeutral(t *testing.T) {
	s := parse(t, `{
		"type":"object",
		"properties":{
			"email":{"type":"string","x-gdpr-sensitive":"email"},
			"phone":{"type":"string","x-gdpr-sensitive":true},
			"locale":{"type":"string"},
			"opt_in":{"type":"boolean","x-gdpr-sensitive":false},
			"empty":{"type":"string","x-gdpr-sensitive":""}
		}
	}`)
	if !s.Properties["email"].GDPRSensitive || s.Properties["email"].GDPRCategory != "email" {
		t.Fatal(`x-gdpr-sensitive:"email" must mark sensitive with category "email"`)
	}
	if !s.Properties["phone"].GDPRSensitive || s.Properties["phone"].GDPRCategory != "" {
		t.Fatal("x-gdpr-sensitive:true must mark sensitive with no category")
	}
	if s.Properties["locale"].GDPRSensitive {
		t.Fatal("unmarked property must not be sensitive")
	}
	if s.Properties["opt_in"].GDPRSensitive {
		t.Fatal("x-gdpr-sensitive:false must not mark sensitive")
	}
	if s.Properties["empty"].GDPRSensitive {
		t.Fatal(`x-gdpr-sensitive:"" must not mark sensitive`)
	}
	// The keyword must not change validation (GR-1): a valid value stays valid.
	if errs := s.Validate(map[string]any{"email": "a@b.com", "phone": "123"}); len(errs) != 0 {
		t.Fatalf("x-gdpr-sensitive must not change validation, got %v", errs)
	}
}

func TestSensitivePaths_NestedAndArrays(t *testing.T) {
	s := parse(t, `{
		"type":"object",
		"properties":{
			"email":{"type":"string","x-gdpr-sensitive":"email"},
			"profile":{"type":"object","properties":{
				"full_name":{"type":"string","x-gdpr-sensitive":true}
			}},
			"addresses":{"type":"array","items":{"type":"object","properties":{
				"line":{"type":"string","x-gdpr-sensitive":true},
				"city":{"type":"string"}
			}}}
		}
	}`)
	got := s.SensitivePaths()
	want := []SensitivePath{
		{Path: "addresses[].line"},
		{Path: "email", Category: "email"},
		{Path: "profile.full_name"},
	}
	if len(got) != len(want) {
		t.Fatalf("SensitivePaths() = %v, want %v", got, want)
	}
	for i := range want { // SensitivePaths is sorted
		if got[i] != want[i] {
			t.Fatalf("SensitivePaths() = %v, want %v", got, want)
		}
	}
}

func TestSensitivePaths_RootAndNilSafe(t *testing.T) {
	root := parse(t, `{"type":"string","x-gdpr-sensitive":true}`)
	if paths := root.SensitivePaths(); len(paths) != 1 || paths[0].Path != "" {
		t.Fatalf("root mark should be path %q, got %v", "", root.SensitivePaths())
	}
	var nilSchema *Schema
	if paths := nilSchema.SensitivePaths(); len(paths) != 0 {
		t.Fatalf("nil schema should yield no paths, got %v", paths)
	}
}

func TestValidate_UnionTypes(t *testing.T) {
	s := parse(t, `{"type":["string","null"]}`)
	if s.Type != "" || !reflect.DeepEqual(s.Types, []string{"string", "null"}) {
		t.Fatalf("union not parsed: Type=%q Types=%v", s.Type, s.Types)
	}
	for _, v := range []any{"abc", nil} {
		if errs := s.Validate(v); len(errs) != 0 {
			t.Errorf("%v should match the union, got %v", v, errs)
		}
	}
	for _, v := range []any{5.0, false, map[string]any{}} {
		if errs := s.Validate(v); len(errs) == 0 {
			t.Errorf("%v should not match [string,null]", v)
		}
	}

	i := parse(t, `{"type":["integer","null"],"minimum":0}`)
	for _, v := range []any{1.0, 100.0, int64(3), json.Number("1e2"), nil} {
		if errs := i.Validate(v); len(errs) != 0 {
			t.Errorf("%v should be an integer or null, got %v", v, errs)
		}
	}
	if errs := i.Validate(1.5); len(errs) == 0 {
		t.Error("1.5 is not an integer")
	}
	if errs := i.Validate(-1.0); len(errs) == 0 {
		t.Error("minimum must still apply inside a union")
	}
	if errs := i.Validate(true); len(errs) == 0 {
		t.Error("a boolean is never an integer")
	}
}

func TestValidate_UnionKeepsPerTypeConstraints(t *testing.T) {
	s := parse(t, `{"type":["string","null"],"minLength":2,"maxLength":3}`)
	if errs := s.Validate("a"); len(errs) == 0 {
		t.Error("minLength must apply to the string branch")
	}
	if errs := s.Validate("abcd"); len(errs) == 0 {
		t.Error("maxLength must apply to the string branch")
	}
	if errs := s.Validate("abc"); len(errs) != 0 {
		t.Errorf("abc is within bounds: %v", errs)
	}
	if errs := s.Validate(nil); len(errs) != 0 {
		t.Errorf("null skips the string-only bounds: %v", errs)
	}
	obj := parse(t, `{"type":["object","null"],"required":["a"]}`)
	if errs := obj.Validate(map[string]any{}); len(errs) == 0 {
		t.Error("object branch must enforce required")
	}
}

func TestValidate_NonFiniteLengthBoundsAreIgnored(t *testing.T) {
	// NaN / Inf cannot come from JSON; a hand-built map can carry them, and they must not
	// change a verdict (int(NaN) is implementation-defined and would otherwise reject "a").
	for _, keyword := range []string{"minLength", "maxLength"} {
		for _, bound := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
			s := fromMap(map[string]any{"type": "string", keyword: bound})
			for _, v := range []string{"", "a", "abc"} {
				if errs := s.Validate(v); len(errs) != 0 {
					t.Errorf("%s=%v must be ignored, %q gave %v", keyword, bound, v, errs)
				}
			}
		}
	}
}

func TestValidate_FractionalLengthBoundsTruncateTowardZero(t *testing.T) {
	min := parse(t, `{"type":"string","minLength":2.5}`)
	if errs := min.Validate("ab"); len(errs) != 0 {
		t.Errorf("minLength 2.5 behaves as 2: %v", errs)
	}
	if errs := min.Validate("a"); len(errs) == 0 {
		t.Error("a is below minLength 2")
	}
	max := parse(t, `{"type":"string","maxLength":-0.5}`)
	if errs := max.Validate(""); len(errs) != 0 {
		t.Errorf("maxLength -0.5 behaves as 0: %v", errs)
	}
	if errs := max.Validate("a"); len(errs) == 0 {
		t.Error("a exceeds maxLength 0")
	}
}

func TestValidate_MaxLengthCountsCodePoints(t *testing.T) {
	s := parse(t, `{"type":"string","maxLength":3}`)
	for _, v := range []string{"abc", "\u011f\u00fc\u015f", "\U0001F600\U0001F600\U0001F600"} {
		if errs := s.Validate(v); len(errs) != 0 {
			t.Errorf("%q has 3 code points: %v", v, errs)
		}
	}
	if errs := s.Validate("\U0001F600\U0001F600\U0001F600\U0001F600"); len(errs) == 0 {
		t.Error("4 code points must exceed maxLength 3")
	}
	if errs := parse(t, `{"type":"string","maxLength":1}`).Validate("e\u0301"); len(errs) == 0 {
		t.Error("decomposed e is 2 code points")
	}
}

func TestValidate_NumericEqualityAcrossNumberTypes(t *testing.T) {
	c := parse(t, `{"const":1}`)
	for _, v := range []any{1.0, 1, int8(1), int64(1), uint(1), uint64(1), float32(1), json.Number("1"), json.Number("1.0"), json.Number("1e0")} {
		if errs := c.Validate(v); len(errs) != 0 {
			t.Errorf("const 1 should accept %T(%v): %v", v, v, errs)
		}
	}
	for _, v := range []any{true, "1", nil, 1.5, json.Number("1.5"), math.NaN(), math.Inf(1)} {
		if errs := c.Validate(v); len(errs) == 0 {
			t.Errorf("const 1 must reject %T(%v)", v, v)
		}
	}
	if errs := parse(t, `{"const":2.5}`).Validate(json.Number("2.50")); len(errs) != 0 {
		t.Errorf("2.5 equals 2.50: %v", errs)
	}
	if errs := parse(t, `{"const":true}`).Validate(1); len(errs) == 0 {
		t.Error("true is not 1")
	}
	if errs := parse(t, `{"const":null}`).Validate(0); len(errs) == 0 {
		t.Error("null equals only null")
	}

	e := parse(t, `{"enum":[1,2.5,"x",null,{"n":[1]}]}`)
	for _, v := range []any{1, 2.50, json.Number("1e0"), "x", nil, map[string]any{"n": []any{1}}, map[string]any{"n": []any{int64(1)}}} {
		if errs := e.Validate(v); len(errs) != 0 {
			t.Errorf("enum should accept %T(%v): %v", v, v, errs)
		}
	}
	for _, v := range []any{3, "1", true, map[string]any{"n": []any{true}}, map[string]any{"n": []any{1, 2}}} {
		if errs := e.Validate(v); len(errs) == 0 {
			t.Errorf("enum must reject %T(%v)", v, v)
		}
	}
}

func TestValidate_ConstObjectKeyOrderAndArrayOrder(t *testing.T) {
	c := parse(t, `{"const":{"a":1,"b":[2,{"c":3}]}}`)
	if errs := c.Validate(map[string]any{"b": []any{2.0, map[string]any{"c": 3}}, "a": int64(1)}); len(errs) != 0 {
		t.Errorf("key order and number types are irrelevant: %v", errs)
	}
	if errs := parse(t, `{"const":{"a":[1,2]}}`).Validate(map[string]any{"a": []any{2.0, 1.0}}); len(errs) == 0 {
		t.Error("array order matters")
	}
	if errs := parse(t, `{"const":{"a":[1]}}`).Validate(map[string]any{"a": []any{1.0}, "b": 1.0}); len(errs) == 0 {
		t.Error("an extra key is not equal")
	}
}

func TestMinimum_AcceptsMixedNumberTypes(t *testing.T) {
	s := parse(t, `{"type":"integer","minimum":2}`)
	if errs := s.Validate(int64(3)); len(errs) != 0 {
		t.Errorf("int64(3) >= 2: %v", errs)
	}
	if errs := s.Validate(json.Number("1")); len(errs) == 0 {
		t.Error("json.Number(1) < 2")
	}
}

func TestParse_WarnsOncePerUnsupportedKeyword(t *testing.T) {
	s := parse(t, `{
		"type":"object","minProperties":1,
		"properties":{
			"email":{"type":"string","pattern":"^.+@.+$","format":"email"},
			"a/b":{"type":"string","pattern":"x"},
			"m~n":{"type":"string","format":"x"},
			"list":{"type":"array","items":{"type":"integer","multipleOf":2}}
		},
		"additionalProperties":{"type":"string","pattern":"^x"},
		"allOf":[{"type":"integer","pattern":"x"}],
		"definitions":{"s":{"type":"string","format":"x"}}
	}`)
	got := map[Warning]bool{}
	for _, w := range s.Warnings() {
		if got[w] {
			t.Errorf("duplicate warning %v", w)
		}
		got[w] = true
	}
	want := map[Warning]bool{
		{"", "minProperties"}:                    true,
		{"", "allOf"}:                            true,
		{"", "definitions"}:                      true,
		{"/properties/email", "pattern"}:         true,
		{"/properties/email", "format"}:          true,
		{"/properties/a~1b", "pattern"}:          true,
		{"/properties/m~0n", "format"}:           true,
		{"/properties/list/items", "multipleOf"}: true,
		{"/additionalProperties", "pattern"}:     true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("warnings = %v, want %v", s.Warnings(), want)
	}
	// An unsupported keyword never changes the verdict.
	if errs := parse(t, `{"type":"string","pattern":"^a$"}`).Validate("zzz"); len(errs) != 0 {
		t.Errorf("pattern must be evaluated as absent: %v", errs)
	}
}

func TestParse_AnnotationKeywordsNeverWarn(t *testing.T) {
	s := parse(t, `{
		"$schema":"http://json-schema.org/draft-07/schema#","$id":"urn:x","title":"T","description":"d",
		"examples":[1],"default":{},"$comment":"c","x-owner":"billing",
		"type":"object",
		"properties":{
			"title":{"type":"string"},"pattern":{"type":"string"},"x-gdpr-sensitive":{"type":"string"},
			"email":{"type":"string","x-gdpr-sensitive":true,"x-custom":1,"default":"a"}
		}
	}`)
	if w := s.Warnings(); len(w) != 0 {
		t.Errorf("annotations and property names must not warn, got %v", w)
	}
	var nilSchema *Schema
	if nilSchema.Warnings() != nil {
		t.Error("nil schema has no warnings")
	}
}

func TestParse_SupportedSubsetProducesNoWarnings(t *testing.T) {
	s := parse(t, `{"type":"object","required":["a"],"properties":{"a":{"type":["number","null"],"minimum":0},
		"n":{"type":"string","minLength":1,"maxLength":5},"k":{"enum":["a"]},"v":{"const":1},
		"t":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}`)
	if w := s.Warnings(); len(w) != 0 {
		t.Errorf("unexpected warnings %v", w)
	}
}

func TestValidate_NonFiniteFloatsNeverPanicOrMatch(t *testing.T) {
	nonFinite := []any{
		math.NaN(), math.Inf(1), math.Inf(-1),
		float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)),
	}
	schemas := []string{`{"const":1}`, `{"enum":[1,2.5]}`, `{"type":"number"}`, `{"type":"integer"}`, `{"type":["integer","null"],"minimum":0}`}
	for _, src := range schemas {
		s := parse(t, src)
		for _, v := range nonFinite {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("%s with %T(%v) panicked: %v", src, v, v, r)
					}
				}()
				if errs := s.Validate(v); len(errs) == 0 {
					t.Errorf("%s must reject non-finite %T(%v)", src, v, v)
				}
			}()
		}
	}
	// A non-finite value is never equal to anything, including itself.
	if jsonEqual(math.NaN(), math.NaN()) || jsonEqual(float32(math.Inf(1)), float32(math.Inf(1))) {
		t.Error("non-finite values are not JSON numbers and never compare equal")
	}
}
