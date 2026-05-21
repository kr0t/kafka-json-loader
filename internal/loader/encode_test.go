package loader

import (
	"encoding/json"
	"testing"
)

func TestEncodeValueTypedBase64(t *testing.T) {
	input := map[string]any{
		"type": "base64",
		"data": "SGVsbG8=",
	}

	got, err := EncodeValue(input)
	if err != nil {
		t.Fatalf("EncodeValue returned error: %v", err)
	}

	if string(got) != "Hello" {
		t.Fatalf("unexpected payload: %q", string(got))
	}
}

func TestEncodeValueJSONMap(t *testing.T) {
	input := map[string]any{
		"hello": "world",
	}

	got, err := EncodeValue(input)
	if err != nil {
		t.Fatalf("EncodeValue returned error: %v", err)
	}

	if string(got) != "{\"hello\":\"world\"}" {
		t.Fatalf("unexpected payload: %q", string(got))
	}
}

func TestEncodeHeadersObject(t *testing.T) {
	raw := json.RawMessage(`{"trace-id":"abc","binary":{"type":"hex","data":"0a0b"}}`)

	headers, err := EncodeHeaders(raw)
	if err != nil {
		t.Fatalf("EncodeHeaders returned error: %v", err)
	}

	if len(headers) != 2 {
		t.Fatalf("unexpected header count: %d", len(headers))
	}

	if headers[0].Key != "binary" || string(headers[0].Value) != "\x0a\x0b" {
		t.Fatalf("unexpected first header: %#v", headers[0])
	}

	if headers[1].Key != "trace-id" || string(headers[1].Value) != "abc" {
		t.Fatalf("unexpected second header: %#v", headers[1])
	}
}
