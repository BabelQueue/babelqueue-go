package schema

import "testing"

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
