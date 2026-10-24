package babelqueue

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

// This file builds the DLQ record of a poison body (ADR-0035, error-handling.md section 6). A body
// that is a strict RFC 8259 JSON object without a top-level "dead_letter" key is spliced at byte
// level: every original byte is kept and only the dead_letter member is inserted, so no value is
// ever re-encoded (a 1e400 or a 64-bit integer stays as written, a repeated member name is not
// collapsed). Anything else is wrapped as {"raw": <string>, "raw_encoding"?: "base64",
// "dead_letter": ...}: "raw" is the body as plain text when it is valid UTF-8 and otherwise the
// standard base64 of the exact bytes with "raw_encoding": "base64". U+FFFD substitution never
// happens.

// buildPoisonRecord returns the DLQ record for body, with deadLetter (the JSON of the dead_letter
// block) as a top-level member.
func buildPoisonRecord(body string, deadLetter []byte) []byte {
	var buf bytes.Buffer
	if isSpliceableObject(body) {
		trimmed := strings.Trim(body, " \t\r\n")
		inner := strings.Trim(trimmed[1:len(trimmed)-1], " \t\r\n")
		if inner == "" {
			buf.WriteString(`{"dead_letter":`)
		} else {
			buf.WriteString(trimmed[:len(trimmed)-1])
			buf.WriteString(`,"dead_letter":`)
		}
		buf.Write(deadLetter)
		buf.WriteByte('}')
		return buf.Bytes()
	}

	buf.WriteString(`{"raw":`)
	if utf8.ValidString(body) {
		buf.Write(marshalJSONString(body))
	} else {
		buf.Write(marshalJSONString(base64.StdEncoding.EncodeToString([]byte(body))))
		buf.WriteString(`,"raw_encoding":"base64"`)
	}
	buf.WriteString(`,"dead_letter":`)
	buf.Write(deadLetter)
	buf.WriteByte('}')
	return buf.Bytes()
}

// marshalJSONString encodes s (valid UTF-8) as a JSON string without HTML escaping.
func marshalJSONString(s string) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s) // a string always encodes
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// isSpliceableObject reports whether body is a single strict RFC 8259 JSON object that is valid
// UTF-8, carries no unpaired surrogate escape and has no top-level "dead_letter" member
// (ADR-0035 section 1).
func isSpliceableObject(body string) bool {
	if !utf8.ValidString(body) || !json.Valid([]byte(body)) {
		return false // also rejects NaN / Infinity / a BOM / trailing data / control characters
	}
	i := 0
	for i < len(body) && isJSONSpace(body[i]) {
		i++
	}
	if i >= len(body) || body[i] != '{' {
		return false
	}

	depth := 0
	expectKey := false
	for i < len(body) {
		switch c := body[i]; c {
		case '{', '[':
			depth++
			expectKey = c == '{' && depth == 1
			i++
		case '}', ']':
			depth--
			i++
		case ',':
			expectKey = depth == 1
			i++
		case '"':
			end, ok := scanJSONString(body, i)
			if !ok {
				return false
			}
			if depth == 1 && expectKey {
				var key string
				if err := json.Unmarshal([]byte(body[i:end]), &key); err != nil || key == "dead_letter" {
					return false
				}
				expectKey = false
			}
			i = end
		default:
			i++
		}
	}
	return true
}

func isJSONSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

// scanJSONString scans the (already syntax-validated) string literal starting at s[start] and
// returns the index just past its closing quote. ok is false when it holds an unpaired surrogate
// escape.
func scanJSONString(s string, start int) (end int, ok bool) {
	i := start + 1
	for i < len(s) {
		switch s[i] {
		case '"':
			return i + 1, true
		case '\\':
			if s[i+1] != 'u' {
				i += 2
				continue
			}
			cp, good := parseHex4(s, i+2)
			if !good {
				return 0, false
			}
			i += 6
			switch {
			case cp >= 0xD800 && cp <= 0xDBFF:
				if i+1 >= len(s) || s[i] != '\\' || s[i+1] != 'u' {
					return 0, false
				}
				lo, good := parseHex4(s, i+2)
				if !good || lo < 0xDC00 || lo > 0xDFFF {
					return 0, false
				}
				i += 6
			case cp >= 0xDC00 && cp <= 0xDFFF:
				return 0, false
			}
		default:
			i++
		}
	}
	return 0, false
}

func parseHex4(s string, at int) (rune, bool) {
	if at+4 > len(s) {
		return 0, false
	}
	var cp rune
	for _, c := range []byte(s[at : at+4]) {
		switch {
		case c >= '0' && c <= '9':
			cp = cp<<4 | rune(c-'0')
		case c >= 'a' && c <= 'f':
			cp = cp<<4 | rune(c-'a'+10)
		case c >= 'A' && c <= 'F':
			cp = cp<<4 | rune(c-'A'+10)
		default:
			return 0, false
		}
	}
	return cp, true
}
