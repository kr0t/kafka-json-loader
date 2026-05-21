package loader

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Request struct {
	Brokers            []string       `json:"brokers"`
	Topic              string         `json:"topic"`
	ClientID           string         `json:"clientId"`
	RequiredAcks       string         `json:"requiredAcks"`
	Compression        string         `json:"compression"`
	BatchTimeoutMillis int            `json:"batchTimeoutMs"`
	WriteTimeoutMillis int            `json:"writeTimeoutMs"`
	ReadTimeoutMillis  int            `json:"readTimeoutMs"`
	SSL                *SSLConfig     `json:"ssl"`
	Messages           []MessageInput `json:"messages"`
}

type MessageInput struct {
	Key     any           `json:"key"`
	Value   any           `json:"value"`
	Headers json.RawMessage `json:"headers"`
	Time    *time.Time    `json:"time"`
}

type SSLConfig struct {
	Enabled            bool   `json:"enabled"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify"`
	ServerName         string `json:"serverName"`
	CAFile             string `json:"caFile"`
	CertFile           string `json:"certFile"`
	KeyFile            string `json:"keyFile"`
}

func LoadRequest(path string) (*Request, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var request Request
	if err := json.Unmarshal(content, &request); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}

	if len(request.Brokers) == 0 {
		return nil, fmt.Errorf("brokers must contain at least one Kafka broker")
	}

	if strings.TrimSpace(request.Topic) == "" {
		return nil, fmt.Errorf("topic is required")
	}

	if len(request.Messages) == 0 {
		return nil, fmt.Errorf("messages must contain at least one message")
	}

	if request.BatchTimeoutMillis <= 0 {
		request.BatchTimeoutMillis = 1000
	}

	if request.WriteTimeoutMillis <= 0 {
		request.WriteTimeoutMillis = 10000
	}

	if request.ReadTimeoutMillis <= 0 {
		request.ReadTimeoutMillis = 10000
	}

	if request.RequiredAcks == "" {
		request.RequiredAcks = "all"
	}

	if request.SSL != nil && request.SSL.Enabled {
		if (request.SSL.CertFile == "") != (request.SSL.KeyFile == "") {
			return nil, fmt.Errorf("ssl.certFile and ssl.keyFile must be provided together")
		}
	}

	return &request, nil
}
