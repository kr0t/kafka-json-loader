package loader

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func Send(request *Request) (int, error) {
	tlsConfig, err := buildTLSConfig(request.SSL)
	if err != nil {
		return 0, err
	}

	messages := make([]kafka.Message, 0, len(request.Messages))
	for index, item := range request.Messages {
		key, err := EncodeValue(item.Key)
		if err != nil {
			return 0, fmt.Errorf("message %d key: %w", index, err)
		}

		value, err := EncodeValue(item.Value)
		if err != nil {
			return 0, fmt.Errorf("message %d value: %w", index, err)
		}

		headers, err := EncodeHeaders(item.Headers)
		if err != nil {
			return 0, fmt.Errorf("message %d headers: %w", index, err)
		}

		message := kafka.Message{
			Key:   key,
			Value: value,
			Time:  time.Now(),
		}
		if item.Time != nil {
			message.Time = *item.Time
		}

		if len(headers) > 0 {
			message.Headers = make([]kafka.Header, 0, len(headers))
			for _, header := range headers {
				message.Headers = append(message.Headers, kafka.Header{
					Key:   header.Key,
					Value: header.Value,
				})
			}
		}

		messages = append(messages, message)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(request.Brokers...),
		Topic:        request.Topic,
		BatchTimeout: time.Duration(request.BatchTimeoutMillis) * time.Millisecond,
		ReadTimeout:  time.Duration(request.ReadTimeoutMillis) * time.Millisecond,
		WriteTimeout: time.Duration(request.WriteTimeoutMillis) * time.Millisecond,
		RequiredAcks: parseRequiredAcks(request.RequiredAcks),
		Compression:  parseCompression(request.Compression),
		Balancer:     &kafka.LeastBytes{},
		Transport: &kafka.Transport{
			ClientID: request.ClientID,
			TLS:      tlsConfig,
		},
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(request.WriteTimeoutMillis)*time.Millisecond)
	defer cancel()

	if err := writer.WriteMessages(ctx, messages...); err != nil {
		return 0, err
	}

	return len(messages), nil
}

func parseRequiredAcks(value string) kafka.RequiredAcks {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "none", "0":
		return kafka.RequireNone
	case "one", "1":
		return kafka.RequireOne
	default:
		return kafka.RequireAll
	}
}

func parseCompression(value string) kafka.Compression {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return 0
	}
}
