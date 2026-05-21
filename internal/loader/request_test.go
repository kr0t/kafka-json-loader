package loader

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRequestRejectsIncompleteClientCertificatePair(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	config := `{
		"brokers": ["localhost:9092"],
		"topic": "demo",
		"ssl": {
			"enabled": true,
			"certFile": "client.pem"
		},
		"messages": [
			{"value": "hello"}
		]
	}`

	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	_, err := LoadRequest(configPath)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func TestValidateRequestRejectsEmptyMessages(t *testing.T) {
	_, err := validateRequest(&Request{
		Brokers: []string{"localhost:9092"},
		Topic:   "demo",
	})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}
