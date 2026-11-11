// Package schema adds optional, dependency-free per-URN payload validation to a babelqueue
// producer or consumer (ADR-0024). A [Provider] supplies a JSON Schema for a message URN —
// typically read from a babelqueue-registry registry.json — and the message's data block is
// validated against it. It is opt-in: a URN with no registered schema is never validated.
//
// The validator is an intentionally small subset of JSON Schema (draft-07) — enough to
// describe a message's data, mirroring the subset babelqueue-registry and php-sdk's envelope
// validator enforce. It is NOT a full draft-07 implementation: a keyword outside the subset
// never changes a verdict (it is evaluated as if absent), but parsing a schema reports one
// [Warning] per unsupported keyword occurrence (see [Schema.Warnings]) and logs it through
// log/slog, so a silently-unenforced constraint is never a surprise.
// It lives in the core module (stdlib only, zero dependencies) so it ships with every SDK.
package schema

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"
)

// Schema is a parsed (subset) JSON Schema node.
type Schema struct {
	Type                 string             // object|string|integer|number|boolean|array|null; "" = unconstrained or a union (see Types)
	Types                []string           // "type" given as an array (union): a value is valid when it matches ANY listed type
	Required             []string           // object: required property names
	Properties           map[string]*Schema // object: per-property schemas
	AdditionalProperties *bool              // object: nil = open (draft-07 default true)
	Items                *Schema            // array: item schema
	Enum                 []any              // allowed values (nil = any)
	Const                any                // fixed value when HasConst
	HasConst             bool
	MinLength            *int     // string (counted in Unicode code points)
	MaxLength            *int     // string (counted in Unicode code points)
	Minimum              *float64 // integer|number

	// GDPRSensitive marks a property as carrying personal/sensitive data
	// ("x-gdpr-sensitive": true). It is a JSON-Schema extension keyword (ADR-0030):
	// validation ignores it entirely (it never makes a value valid or invalid), so a registry
	// can declare sensitivity without changing how data is validated — adding it is never a
	// breaking change (GR-1). GDPRCategory carries the optional free-form category form
	// ("x-gdpr-sensitive": "email") for documentation; it is "" when the keyword was the
	// boolean true. The gdpr subpackage consumes these marks to encrypt/decrypt the matching
	// values at runtime; this mirrors babelqueue-registry's internal/schema model.
	GDPRSensitive bool
	GDPRCategory  string

	// warnings are the registration warnings Parse produced for the whole document (set on the
	// root schema only); read them through Warnings.
	warnings []Warning
}

// Warning reports one unsupported keyword found when a schema was parsed. The schema is
// evaluated as if the keyword were absent, so a Warning never changes a verdict; it tells the
// operator that a constraint they wrote is not enforced by this validator.
type Warning struct {
	// Pointer is the RFC 6901 JSON Pointer, within the schema document, of the schema object
	// carrying the keyword: "" for the root, then "/properties/<name>", "/items",
	// "/additionalProperties" (property names escaped "~" -> "~0", "/" -> "~1").
	Pointer string
	// Keyword is the unsupported keyword, e.g. "pattern".
	Keyword string
}

// String renders the warning for logs.
func (w Warning) String() string {
	return fmt.Sprintf("schema: unsupported keyword %q at %q is ignored", w.Keyword, w.Pointer)
}

// supportedKeywords is the enforced draft-07 subset; annotationKeywords never warn.
var (
	supportedKeywords = map[string]bool{
		"type": true, "required": true, "properties": true, "additionalProperties": true,
		"items": true, "enum": true, "const": true, "minLength": true, "maxLength": true, "minimum": true,
	}
	annotationKeywords = map[string]bool{
		"$schema": true, "$id": true, "title": true, "description": true,
		"examples": true, "default": true, "$comment": true,
	}
)

// Parse decodes a (subset) JSON Schema document. Every unsupported keyword it finds is
// reported once per occurrence through [Schema.Warnings] and logged with slog.Warn; the
// annotation keywords ($schema, $id, title, description, examples, default, $comment and
// every "x-*" keyword) never warn.
func Parse(raw []byte) (*Schema, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("schema: parse: %w", err)
	}
	s := fromMap(m)
	collectWarnings(m, "", &s.warnings)
	for _, w := range s.warnings {
		slog.Warn("babelqueue: "+w.String(), "pointer", w.Pointer, "keyword", w.Keyword)
	}
	return s, nil
}

// Warnings returns the unsupported-keyword warnings Parse produced for the document this
// schema was parsed from, in a deterministic order. It is empty for a nil schema, for a schema
// that is not the parsed root, and for a document that stays inside the supported subset.
func (s *Schema) Warnings() []Warning {
	if s == nil {
		return nil
	}
	return append([]Warning(nil), s.warnings...)
}

// collectWarnings walks the schema document the way the conformance suite pins it: only into
// the values of properties, items and additionalProperties (each when a JSON object), and never
// into the value of an unsupported keyword.
func collectWarnings(m map[string]any, pointer string, out *[]Warning) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if supportedKeywords[k] || annotationKeywords[k] || strings.HasPrefix(k, "x-") {
			continue
		}
		*out = append(*out, Warning{Pointer: pointer, Keyword: k})
	}
	if props, ok := m["properties"].(map[string]any); ok {
		names := make([]string, 0, len(props))
		for name := range props {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if pm, ok := props[name].(map[string]any); ok {
				collectWarnings(pm, pointer+"/properties/"+escapePointer(name), out)
			}
		}
	}
	if items, ok := m["items"].(map[string]any); ok {
		collectWarnings(items, pointer+"/items", out)
	}
	if ap, ok := m["additionalProperties"].(map[string]any); ok {
		collectWarnings(ap, pointer+"/additionalProperties", out)
	}
}

func escapePointer(name string) string {
	return strings.ReplaceAll(strings.ReplaceAll(name, "~", "~0"), "/", "~1")
}

func fromMap(m map[string]any) *Schema {
	s := &Schema{}
	switch t := m["type"].(type) {
	case string:
		s.Type = t
	case []any:
		for _, e := range t {
			if name, ok := e.(string); ok {
				s.Types = append(s.Types, name)
			}
		}
	}
	if req, ok := m["required"].([]any); ok {
		for _, r := range req {
			if name, ok := r.(string); ok {
				s.Required = append(s.Required, name)
			}
		}
	}
	if props, ok := m["properties"].(map[string]any); ok {
		s.Properties = make(map[string]*Schema, len(props))
		for name, ps := range props {
			if pm, ok := ps.(map[string]any); ok {
				s.Properties[name] = fromMap(pm)
			}
		}
	}
	if ap, ok := m["additionalProperties"].(bool); ok {
		s.AdditionalProperties = &ap
	}
	if items, ok := m["items"].(map[string]any); ok {
		s.Items = fromMap(items)
	}
	if enum, ok := m["enum"].([]any); ok {
		s.Enum = enum
	}
	if c, ok := m["const"]; ok {
		s.Const = c
		s.HasConst = true
	}
	if ml, ok := m["minLength"].(float64); ok {
		v := int(ml)
		s.MinLength = &v
	}
	if ml, ok := m["maxLength"].(float64); ok {
		v := int(ml)
		s.MaxLength = &v
	}
	if min, ok := m["minimum"].(float64); ok {
		v := min
		s.Minimum = &v
	}
	// x-gdpr-sensitive (ADR-0030): a recognised-but-validation-ignored extension keyword.
	// Accept either the boolean true or a non-empty string category (e.g. "email"); any other
	// shape — false, "", a number — leaves the property unmarked. Mirrors the registry's parser.
	switch g := m["x-gdpr-sensitive"].(type) {
	case bool:
		s.GDPRSensitive = g
	case string:
		if g != "" {
			s.GDPRSensitive = true
			s.GDPRCategory = g
		}
	}
	return s
}

// SensitivePath is one property marked x-gdpr-sensitive, located by its dotted path from the
// schema root (array elements use the "field[]" segment the validator/compat use). Category is
// the optional "x-gdpr-sensitive": "<category>" string, or "" when it was the boolean true.
type SensitivePath struct {
	Path     string
	Category string
}

// SensitivePaths walks the schema and returns every property marked x-gdpr-sensitive, in sorted
// path order. It descends into nested objects (dotted paths) and array item schemas ("field[]").
// A mark on the root schema itself (path "") is reported as a path of "". It is the value-level
// counterpart to the registry's inventory: the gdpr subpackage uses these paths to locate the
// leaves it must encrypt on produce and decrypt on consume.
func (s *Schema) SensitivePaths() []SensitivePath {
	var out []SensitivePath
	s.collectSensitive("", &out)
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func (s *Schema) collectSensitive(path string, out *[]SensitivePath) {
	if s == nil {
		return
	}
	if s.GDPRSensitive {
		*out = append(*out, SensitivePath{Path: path, Category: s.GDPRCategory})
	}
	for name, sub := range s.Properties {
		sub.collectSensitive(join(path, name), out)
	}
	if s.Items != nil {
		s.Items.collectSensitive(path+"[]", out)
	}
}

// Validate checks value against the schema and returns a sorted list of human-readable
// violations. An empty result means the value is valid.
func (s *Schema) Validate(value any) []string {
	var errs []string
	s.validate("", value, &errs)
	sort.Strings(errs)
	return errs
}

func (s *Schema) validate(path string, value any, errs *[]string) {
	if s == nil {
		return
	}
	at := func(msg string) string {
		if path == "" {
			return msg
		}
		return path + ": " + msg
	}

	if s.HasConst && !jsonEqual(value, s.Const) {
		*errs = append(*errs, at(fmt.Sprintf("must equal const %v", s.Const)))
	}
	if s.Enum != nil && !inEnum(value, s.Enum) {
		*errs = append(*errs, at("value is not one of the allowed enum values"))
	}

	types := s.knownTypes()
	if len(types) == 0 {
		return
	}
	matched := ""
	for _, t := range types {
		if matchesType(t, value) {
			matched = t
			break
		}
	}
	if matched == "" {
		if len(types) == 1 {
			*errs = append(*errs, at(typeMessages[types[0]]))
		} else {
			*errs = append(*errs, at("must be one of the types "+strings.Join(types, ", ")))
		}
		return
	}

	switch matched {
	case "object":
		s.validateObject(path, value, errs, at)
	case "array":
		s.validateArray(path, value, errs, at)
	case "string":
		// minLength/maxLength count Unicode code points (not UTF-8 bytes, UTF-16 units or
		// grapheme clusters) — the cross-SDK rule.
		str := value.(string)
		n := utf8.RuneCountInString(str)
		if s.MinLength != nil && n < *s.MinLength {
			*errs = append(*errs, at(fmt.Sprintf("must be at least %d characters", *s.MinLength)))
		}
		if s.MaxLength != nil && n > *s.MaxLength {
			*errs = append(*errs, at(fmt.Sprintf("must be at most %d characters", *s.MaxLength)))
		}
	case "integer", "number":
		s.checkMinimum(value, errs, at)
	}
}

var typeMessages = map[string]string{
	"object":  "must be an object",
	"array":   "must be an array",
	"string":  "must be a string",
	"integer": "must be an integer",
	"number":  "must be a number",
	"boolean": "must be a boolean",
	"null":    "must be null",
}

// knownTypes returns the schema's type names (the "type" string, or every entry of a "type"
// array) restricted to the seven JSON Schema types; an unknown name constrains nothing.
func (s *Schema) knownTypes() []string {
	var names []string
	if s.Type != "" {
		names = []string{s.Type}
	} else {
		names = s.Types
	}
	var out []string
	for _, n := range names {
		if _, ok := typeMessages[n]; ok {
			out = append(out, n)
		}
	}
	return out
}

func matchesType(t string, value any) bool {
	switch t {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "integer":
		return isInteger(value)
	case "number":
		return isNumber(value)
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	}
	return false
}

func (s *Schema) validateObject(path string, value any, errs *[]string, at func(string) string) {
	obj, ok := value.(map[string]any)
	if !ok {
		*errs = append(*errs, at("must be an object"))
		return
	}
	for _, req := range s.Required {
		if _, present := obj[req]; !present {
			*errs = append(*errs, at(fmt.Sprintf("missing required property %q", req)))
		}
	}
	for key, v := range obj {
		if sub, ok := s.Properties[key]; ok {
			sub.validate(join(path, key), v, errs)
			continue
		}
		if s.AdditionalProperties != nil && !*s.AdditionalProperties {
			*errs = append(*errs, at(fmt.Sprintf("additional property %q is not allowed", key)))
		}
	}
}

func (s *Schema) validateArray(path string, value any, errs *[]string, at func(string) string) {
	arr, ok := value.([]any)
	if !ok {
		*errs = append(*errs, at("must be an array"))
		return
	}
	if s.Items == nil {
		return
	}
	for i, item := range arr {
		s.Items.validate(fmt.Sprintf("%s[%d]", path, i), item, errs)
	}
}

func (s *Schema) checkMinimum(value any, errs *[]string, at func(string) string) {
	if s.Minimum == nil {
		return
	}
	if f, ok := toFloat(value); ok && f < *s.Minimum {
		*errs = append(*errs, at(fmt.Sprintf("must be >= %v", *s.Minimum)))
	}
}

func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func inEnum(value any, enum []any) bool {
	for _, e := range enum {
		if jsonEqual(value, e) {
			return true
		}
	}
	return false
}

// jsonEqual compares two JSON values the way const/enum require: numbers by mathematical value
// (1, 1.0 and 1e0 are equal, whatever Go number type carries them: int*, uint*, float32/64 or
// json.Number), recursively inside arrays (element by element, in order) and objects (same key
// set, key order irrelevant). A boolean never equals a number, a string never equals a number,
// and nil equals only nil.
func jsonEqual(a, b any) bool {
	if ra, ok := toRat(a); ok {
		rb, ok := toRat(b)
		return ok && ra.Cmp(rb) == 0
	}
	switch av := a.(type) {
	case float32, float64:
		return false // NaN and the infinities are not JSON numbers and equal nothing
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !jsonEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, x := range av {
			y, present := bv[k]
			if !present || !jsonEqual(x, y) {
				return false
			}
		}
		return true
	}
	if _, ok := toRat(b); ok {
		return false
	}
	return reflect.DeepEqual(a, b)
}

// toRat converts any Go number representation to an exact rational so mixed number types
// compare by mathematical value. NaN and the infinities are not JSON numbers and report false.
func toRat(value any) (*big.Rat, bool) {
	switch n := value.(type) {
	case int:
		return new(big.Rat).SetInt64(int64(n)), true
	case int8:
		return new(big.Rat).SetInt64(int64(n)), true
	case int16:
		return new(big.Rat).SetInt64(int64(n)), true
	case int32:
		return new(big.Rat).SetInt64(int64(n)), true
	case int64:
		return new(big.Rat).SetInt64(n), true
	case uint:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(uint64(n))), true
	case uint8:
		return new(big.Rat).SetInt64(int64(n)), true
	case uint16:
		return new(big.Rat).SetInt64(int64(n)), true
	case uint32:
		return new(big.Rat).SetInt64(int64(n)), true
	case uint64:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(n)), true
	case float32:
		r := new(big.Rat).SetFloat64(float64(n)) // nil for NaN/Inf
		return r, r != nil
	case float64:
		r := new(big.Rat).SetFloat64(n) // nil for NaN/Inf
		return r, r != nil
	case json.Number:
		r, ok := new(big.Rat).SetString(string(n))
		return r, ok
	}
	return nil, false
}

func toFloat(value any) (float64, bool) {
	r, ok := toRat(value)
	if !ok || r == nil {
		return 0, false
	}
	f, _ := r.Float64()
	return f, true
}

func isNumber(value any) bool {
	r, ok := toRat(value)
	return ok && r != nil
}

// isInteger reports whether value is a JSON number whose mathematical value is integral, so
// 1.0 and 1e2 are integers and 1.5 is not (draft-07). A boolean or string is never an integer.
func isInteger(value any) bool {
	r, ok := toRat(value)
	return ok && r != nil && r.IsInt()
}
