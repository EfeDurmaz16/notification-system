package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
)

// Object names are compared after JSON unescaping. The stdlib struct decoder
// folds case and merges duplicate members, while a map decoder may overwrite
// them. Reject ambiguous inputs before using a canonical document as identity.
type objectShape uint8

const (
	freeForm objectShape = iota
	batchEnvelope
	itemArray
	notificationObject
)

var itemFields = [...]string{"channel", "recipient", "content", "priority", "scheduledAt", "template", "variables"}

func (s objectShape) acceptsSpelling(key string) bool {
	var fields []string
	switch s {
	case batchEnvelope:
		fields = []string{"notifications"}
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

func canonicalValue(d *json.Decoder, shape objectShape, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting exceeds maximum depth")
	}
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch tok {
	case json.Delim('{'):
		obj := make(map[string]any)
		for d.More() {
			tok, err := d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := tok.(string)
			if !ok {
				return nil, errors.New("expected an object name")
			}
			if _, dup := obj[key]; dup {
				return nil, errors.New("duplicate JSON object member")
			}
			if !shape.acceptsSpelling(key) {
				return nil, errors.New("JSON property names are case-sensitive")
			}
			child := freeForm
			if shape == batchEnvelope && key == "notifications" {
				child = itemArray
			}
			obj[key], err = canonicalValue(d, child, depth+1)
			if err != nil {
				return nil, err
			}
		}
		_, err = d.Token()
		return obj, err
	case json.Delim('['):
		arr := []any{}
		child := freeForm
		if shape == itemArray {
			child = notificationObject
		}
		for d.More() {
			v, err := canonicalValue(d, child, depth+1)
			if err != nil {
				return nil, err
			}
			arr = append(arr, v)
		}
		_, err = d.Token()
		return arr, err
	default:
		return tok, nil
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

func fingerprint(path string, raw []byte) ([]byte, error) {
	if !validSurrogatePairs(raw) {
		return nil, errors.New("invalid Unicode surrogate pair")
	}
	shape := notificationObject
	if path == "/v1/batches" {
		shape = batchEnvelope
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	value, err := canonicalValue(d, shape, 0)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	_, _ = io.WriteString(h, path+"\n") // hash.Hash writes always succeed.
	if err := json.NewEncoder(h).Encode(value); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}
