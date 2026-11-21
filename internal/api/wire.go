package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"slices"
	"strconv"
	"strings"
)

// Object names are compared after JSON unescaping. The stdlib struct decoder
// folds case and merges duplicate members, while a map decoder may overwrite
// them. Reject ambiguous inputs before using a canonical document as identity.
type objectShape uint8

const (
	freeForm objectShape = iota
	notificationObject
)

var itemFields = [...]string{"channel", "recipient", "content", "priority", "scheduledAt", "template", "variables"}

func (s objectShape) acceptsSpelling(key string) bool {
	var fields []string
	switch s {
	case notificationObject:
		fields = itemFields[:]
	default:
		return true
	}
	for _, field := range fields {
		if strings.EqualFold(field, key) {
			return field == key
		}
	}
	// Unknown properties retain their normal schema-validation behavior.
	return true
}

// jsonMembers yields slices into a syntax-checked document. It neither copies
// array elements nor decodes unused child values. readBody checks json.Valid
// before this point, so delimiters and escape sequences are well formed.
func jsonMembers(raw []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		object := raw[0] == '{'
		remaining := bytes.TrimSpace(raw[1 : len(raw)-1])
		for len(remaining) > 0 {
			var key []byte
			if object {
				end := quotedEnd(remaining)
				key = remaining[:end]
				remaining = bytes.TrimSpace(remaining[end:])
				remaining = bytes.TrimSpace(remaining[1:]) // colon
			}
			end := valueEnd(remaining)
			if !yield(key, bytes.TrimSpace(remaining[:end])) {
				return
			}
			if end == len(remaining) {
				return
			}
			remaining = bytes.TrimSpace(remaining[end+1:]) // comma
		}
	}
}

func memberCount(raw []byte) int {
	count := 0
	for range jsonMembers(raw) {
		count++
	}
	return count
}

func quotedEnd(raw []byte) int {
	for i := 1; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			return i + 1
		case '\\':
			i++
		}
	}
	return len(raw)
}

// Return the next comma at the current nesting level, or the end of the slice.
func valueEnd(raw []byte) int {
	depth := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			i += quotedEnd(raw[i:]) - 1
		case '[', '{':
			depth++
		case ']', '}':
			depth--
		case ',':
			if depth == 0 {
				return i
			}
		}
	}
	return len(raw)
}

func jsonString(raw []byte) string {
	if !bytes.ContainsRune(raw, '\\') {
		return string(raw[1 : len(raw)-1])
	}
	var s string
	_ = json.Unmarshal(raw, &s) // A validated string token.
	return s
}

// Match encoding/json's original string representation, including HTML escapes.
func appendQuoted(dst []byte, raw []byte) []byte {
	if !bytes.ContainsAny(raw, "\\<>&\u2028\u2029") {
		return append(dst, raw...)
	}
	encoded, _ := json.Marshal(jsonString(raw))
	return append(dst, encoded...)
}

type member struct {
	name       string
	key, value []byte
}

func appendCanonical(dst, raw []byte, shape objectShape, depth int) ([]byte, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting exceeds maximum depth")
	}
	switch raw[0] {
	case '{':
		var fields []member
		// Wide variables maps otherwise grow and copy several backing arrays.
		if len(raw) > 4096 {
			fields = make([]member, 0, memberCount(raw))
		}
		for key, value := range jsonMembers(raw) {
			name := jsonString(key)
			if !shape.acceptsSpelling(name) {
				return nil, errors.New("JSON property names are case-sensitive")
			}
			fields = append(fields, member{name, key, value})
		}
		slices.SortFunc(fields, func(a, b member) int { return strings.Compare(a.name, b.name) })
		dst = append(dst, '{')
		for i, field := range fields {
			if i > 0 {
				if field.name == fields[i-1].name {
					return nil, errors.New("duplicate JSON object member")
				}
				dst = append(dst, ',')
			}
			dst = appendQuoted(dst, field.key)
			dst = append(dst, ':')
			var err error
			dst, err = appendCanonical(dst, field.value, freeForm, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil
	case '[':
		dst = append(dst, '[')
		first := true
		for _, value := range jsonMembers(raw) {
			if !first {
				dst = append(dst, ',')
			}
			first = false
			var err error
			dst, err = appendCanonical(dst, value, freeForm, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	case '"':
		return appendQuoted(dst, raw), nil
	default:
		return append(dst, raw...), nil
	}
}

// json.Valid accepts unpaired surrogate escapes and json.Unmarshal replaces
// them with U+FFFD. That would collapse distinct request bodies onto one key.
// readBody has already validated JSON syntax and the request's raw UTF-8.
func validSurrogatePairs(raw []byte) bool {
	for {
		i := bytes.IndexByte(raw, '\\')
		if i < 0 {
			return true
		}
		raw = raw[i+1:]
		if raw[0] != 'u' {
			raw = raw[1:]
			continue
		}
		n, _ := strconv.ParseUint(string(raw[1:5]), 16, 16)
		raw = raw[5:]
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if len(raw) < 6 || raw[0] != '\\' || raw[1] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(raw[2:6]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			raw = raw[6:]
		}
	}
}

// Match persisted request fingerprints while releasing each item's canonical
// representation before the next. A previously accepted key remains replayable.
func requestItems(path string, raw []byte) ([][]byte, []byte, error) {
	raw = bytes.TrimSpace(raw)
	if !validSurrogatePairs(raw) {
		return nil, nil, errors.New("invalid Unicode surrogate pair")
	}
	batch := path == "/v1/batches"
	items := [][]byte{raw}
	if batch {
		const maxItems = 1000
		items = nil
		if raw[0] != '{' {
			return nil, nil, errors.New("provide 1 to 1000 notifications")
		}
		for key, array := range jsonMembers(raw) {
			if jsonString(key) != "notifications" || items != nil || array[0] != '[' {
				return nil, nil, errors.New("provide a single notifications array")
			}
			items = make([][]byte, 0)
			for _, item := range jsonMembers(array) {
				if len(items) == maxItems {
					return nil, nil, errors.New("provide 1 to 1000 notifications")
				}
				items = append(items, item)
			}
		}
		if len(items) == 0 {
			return nil, nil, errors.New("provide 1 to 1000 notifications")
		}
	}
	h := sha256.New()
	_, _ = io.WriteString(h, path+"\n")
	depth := 0
	if batch {
		_, _ = io.WriteString(h, `{"notifications":[`)
		depth = 2
	}
	var buffer []byte
	for i, item := range items {
		data, err := appendCanonical(buffer[:0], item, notificationObject, depth)
		if err != nil {
			if batch {
				err = fmt.Errorf("notifications[%d]: %w", i, err)
			}
			return nil, nil, err
		}
		if i > 0 {
			_, _ = io.WriteString(h, ",")
		}
		_, _ = h.Write(data)
		buffer = data
	}
	if batch {
		_, _ = io.WriteString(h, "]}")
	}
	_, _ = io.WriteString(h, "\n")
	return items, h.Sum(nil), nil
}
