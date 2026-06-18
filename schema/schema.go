// Package schema adds optional, dependency-free per-URN payload validation to a babelqueue
// producer or consumer (ADR-0024). A [Provider] supplies a JSON Schema for a message URN —
// typically read from a babelqueue-registry registry.json — and the message's data block is
// validated against it. It is opt-in: a URN with no registered schema is never validated.
//
// The validator is an intentionally small subset of JSON Schema (draft-07) — enough to
// describe a message's data, mirroring the subset babelqueue-registry and php-sdk's envelope
// validator enforce. It is NOT a full draft-07 implementation; unknown keywords are ignored.
// It lives in the core module (stdlib only, zero dependencies) so it ships with every SDK.
package schema

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
)

// Schema is a parsed (subset) JSON Schema node.
type Schema struct {
	Type                 string             // object|string|integer|number|boolean|array|null; "" = unconstrained
	Required             []string           // object: required property names
	Properties           map[string]*Schema // object: per-property schemas
	AdditionalProperties *bool              // object: nil = open (draft-07 default true)
	Items                *Schema            // array: item schema
	Enum                 []any              // allowed values (nil = any)
	Const                any                // fixed value when HasConst
	HasConst             bool
	MinLength            *int     // string
	Minimum              *float64 // integer|number
}

// Parse decodes a (subset) JSON Schema document.
func Parse(raw []byte) (*Schema, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("schema: parse: %w", err)
	}
	return fromMap(m), nil
}

func fromMap(m map[string]any) *Schema {
	s := &Schema{}
	if t, ok := m["type"].(string); ok {
		s.Type = t
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
	if min, ok := m["minimum"].(float64); ok {
		v := min
		s.Minimum = &v
	}
	return s
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

	if s.HasConst && !reflect.DeepEqual(value, s.Const) {
		*errs = append(*errs, at(fmt.Sprintf("must equal const %v", s.Const)))
	}
	if s.Enum != nil && !inEnum(value, s.Enum) {
		*errs = append(*errs, at("value is not one of the allowed enum values"))
	}

	switch s.Type {
	case "object":
		s.validateObject(path, value, errs, at)
	case "array":
		s.validateArray(path, value, errs, at)
	case "string":
		str, ok := value.(string)
		if !ok {
			*errs = append(*errs, at("must be a string"))
			return
		}
		if s.MinLength != nil && len(str) < *s.MinLength {
			*errs = append(*errs, at(fmt.Sprintf("must be at least %d characters", *s.MinLength)))
		}
	case "integer":
		if !isInteger(value) {
			*errs = append(*errs, at("must be an integer"))
			return
		}
		s.checkMinimum(value, errs, at)
	case "number":
		if !isNumber(value) {
			*errs = append(*errs, at("must be a number"))
			return
		}
		s.checkMinimum(value, errs, at)
	case "boolean":
		if _, ok := value.(bool); !ok {
			*errs = append(*errs, at("must be a boolean"))
		}
	case "null":
		if value != nil {
			*errs = append(*errs, at("must be null"))
		}
	}
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
		if reflect.DeepEqual(value, e) {
			return true
		}
	}
	return false
}

func toFloat(value any) (float64, bool) {
	switch n := value.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

func isNumber(value any) bool {
	_, ok := toFloat(value)
	return ok
}

// isInteger reports whether value is a JSON number with no fractional part. JSON decodes
// numbers to float64, so "1" arrives as 1.0 — an integer for our purposes.
func isInteger(value any) bool {
	f, ok := toFloat(value)
	return ok && math.Trunc(f) == f
}
