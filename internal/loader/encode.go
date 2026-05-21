package loader

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

type Header struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func EncodeValue(input any) ([]byte, error) {
	if input == nil {
		return nil, nil
	}

	switch value := input.(type) {
	case string:
		return []byte(value), nil
	case bool, float64:
		return json.Marshal(value)
	case []any:
		return json.Marshal(value)
	}

	raw, ok := input.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unsupported value type %T", input)
	}

	typeField, _ := raw["type"].(string)
	if typeField == "" {
		return json.Marshal(raw)
	}

	data := raw["data"]
	switch typeField {
	case "null":
		return nil, nil
	case "string":
		text, ok := data.(string)
		if !ok {
			return nil, fmt.Errorf("type=string expects string data")
		}
		return []byte(text), nil
	case "json":
		return json.Marshal(data)
	case "base64":
		text, ok := data.(string)
		if !ok {
			return nil, fmt.Errorf("type=base64 expects string data")
		}
		decoded, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return nil, fmt.Errorf("decode base64: %w", err)
		}
		return decoded, nil
	case "hex":
		text, ok := data.(string)
		if !ok {
			return nil, fmt.Errorf("type=hex expects string data")
		}
		decoded, err := hex.DecodeString(text)
		if err != nil {
			return nil, fmt.Errorf("decode hex: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported typed value %q", typeField)
	}
}

func EncodeHeaders(raw json.RawMessage) ([]Header, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}

	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err == nil {
		keys := make([]string, 0, len(asMap))
		for key := range asMap {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		headers := make([]Header, 0, len(keys))
		for _, key := range keys {
			value, err := EncodeValue(asMap[key])
			if err != nil {
				return nil, fmt.Errorf("header %q: %w", key, err)
			}
			headers = append(headers, Header{Key: key, Value: value})
		}
		return headers, nil
	}

	var asList []Header
	if err := json.Unmarshal(raw, &asList); err == nil {
		headers := make([]Header, 0, len(asList))
		for _, entry := range asList {
			value, err := EncodeValue(entry.Value)
			if err != nil {
				return nil, fmt.Errorf("header %q: %w", entry.Key, err)
			}
			headers = append(headers, Header{Key: entry.Key, Value: value})
		}
		return headers, nil
	}

	return nil, fmt.Errorf("headers must be an object or an array of {key,value}")
}
