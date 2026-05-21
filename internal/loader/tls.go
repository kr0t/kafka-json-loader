package loader

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func buildTLSConfig(cfg *SSLConfig) (*tls.Config, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		ServerName:         cfg.ServerName,
	}

	if cfg.CAFile != "" {
		caBytes, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ssl.caFile %q: %w", cfg.CAFile, err)
		}

		rootCAs := x509.NewCertPool()
		if !rootCAs.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("parse ssl.caFile %q: no certificates found", cfg.CAFile)
		}
		tlsConfig.RootCAs = rootCAs
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	return tlsConfig, nil
}
