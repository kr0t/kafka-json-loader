package loader

import "testing"

func TestBuildTLSConfigDisabledReturnsNil(t *testing.T) {
	cfg, err := buildTLSConfig(&SSLConfig{Enabled: false})
	if err != nil {
		t.Fatalf("buildTLSConfig returned error: %v", err)
	}

	if cfg != nil {
		t.Fatalf("expected nil tls config, got %#v", cfg)
	}
}
