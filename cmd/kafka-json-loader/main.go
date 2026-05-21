package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"kafka-json-loader/internal/loader"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var configPath string
	var brokers string
	var topic string
	var clientID string
	var requiredAcks string
	var compression string
	var count int
	var keyPrefix string
	var eventType string
	var source string
	var continuous bool
	var rate int
	var interval time.Duration
	var duration time.Duration
	var sslEnabled bool
	var insecureSkipVerify bool
	var serverName string
	var caFile string
	var certFile string
	var keyFile string

	flag.StringVar(&configPath, "config", "", "Path to the JSON payload file.")
	flag.StringVar(&configPath, "c", "", "Path to the JSON payload file.")
	flag.StringVar(&brokers, "brokers", "", "Comma-separated Kafka brokers.")
	flag.StringVar(&topic, "topic", "", "Kafka topic.")
	flag.StringVar(&clientID, "client-id", "windows-loader", "Kafka client id.")
	flag.StringVar(&requiredAcks, "acks", "all", "Kafka required acks: none, one, all.")
	flag.StringVar(&compression, "compression", "none", "Compression: none, gzip, snappy, lz4, zstd.")
	flag.IntVar(&count, "count", 1, "How many messages to generate.")
	flag.StringVar(&keyPrefix, "key-prefix", "msg", "Prefix for generated message keys.")
	flag.StringVar(&eventType, "event-type", "generated", "Event type inside generated payload.")
	flag.StringVar(&source, "source", "kafka-json-loader", "Source value for generated payload and headers.")
	flag.BoolVar(&continuous, "continuous", false, "Send generated messages continuously until stopped.")
	flag.IntVar(&rate, "rate", 0, "Generated messages per second in continuous mode.")
	flag.DurationVar(&interval, "interval", 0, "Pause between generated messages in continuous mode, for example 200ms or 1s.")
	flag.DurationVar(&duration, "duration", 0, "How long continuous mode should run before stopping.")
	flag.BoolVar(&sslEnabled, "ssl", false, "Enable SSL/TLS.")
	flag.BoolVar(&insecureSkipVerify, "ssl-insecure-skip-verify", false, "Disable server certificate verification.")
	flag.StringVar(&serverName, "ssl-server-name", "", "TLS server name.")
	flag.StringVar(&caFile, "ssl-ca-file", "", "Path to CA PEM file.")
	flag.StringVar(&certFile, "ssl-cert-file", "", "Path to client certificate PEM file.")
	flag.StringVar(&keyFile, "ssl-key-file", "", "Path to client private key PEM file.")
	flag.Parse()

	var request *loader.Request
	var err error

	if configPath != "" {
		if continuous {
			return fmt.Errorf("-continuous cannot be used together with -config")
		}
		request, err = loader.LoadRequest(configPath)
	} else {
		options := loader.GeneratorOptions{
			Brokers:            splitAndTrim(brokers),
			Topic:              topic,
			ClientID:           clientID,
			RequiredAcks:       requiredAcks,
			Compression:        compression,
			Count:              count,
			KeyPrefix:          keyPrefix,
			EventType:          eventType,
			Source:             source,
			BatchTimeoutMillis: 1000,
			WriteTimeoutMillis: 10000,
			ReadTimeoutMillis:  10000,
			SSL: &loader.SSLConfig{
				Enabled:            sslEnabled,
				InsecureSkipVerify: insecureSkipVerify,
				ServerName:         serverName,
				CAFile:             caFile,
				CertFile:           certFile,
				KeyFile:            keyFile,
			},
		}

		if continuous {
			return runContinuous(options, rate, interval, duration)
		}

		request, err = loader.BuildGeneratedRequest(options)
	}
	if err != nil {
		return err
	}

	sentCount, err := loader.Send(request)
	if err != nil {
		return err
	}

	fmt.Printf("sent %d message(s) to topic %q\n", sentCount, request.Topic)
	return nil
}

func runContinuous(options loader.GeneratorOptions, rate int, interval, duration time.Duration) error {
	resolvedInterval, err := resolveInterval(rate, interval)
	if err != nil {
		return err
	}

	startedAt := time.Now()
	sequence := 1
	sentTotal := 0

	for {
		if duration > 0 && time.Since(startedAt) >= duration {
			fmt.Printf("continuous mode finished after %s, sent %d message(s) to topic %q\n", duration, sentTotal, options.Topic)
			return nil
		}

		request, err := loader.BuildGeneratedRequest(loader.GeneratorOptions{
			Brokers:            options.Brokers,
			Topic:              options.Topic,
			ClientID:           options.ClientID,
			RequiredAcks:       options.RequiredAcks,
			Compression:        options.Compression,
			Count:              1,
			StartSequence:      sequence,
			KeyPrefix:          options.KeyPrefix,
			EventType:          options.EventType,
			Source:             options.Source,
			BatchTimeoutMillis: options.BatchTimeoutMillis,
			WriteTimeoutMillis: options.WriteTimeoutMillis,
			ReadTimeoutMillis:  options.ReadTimeoutMillis,
			SSL:                options.SSL,
		})
		if err != nil {
			return err
		}

		sent, err := loader.Send(request)
		if err != nil {
			return err
		}

		sentTotal += sent
		sequence += sent

		if resolvedInterval > 0 {
			time.Sleep(resolvedInterval)
		}
	}
}

func resolveInterval(rate int, interval time.Duration) (time.Duration, error) {
	if rate > 0 && interval > 0 {
		return 0, fmt.Errorf("use either -rate or -interval, not both")
	}
	if rate <= 0 && interval <= 0 {
		return 0, fmt.Errorf("continuous mode requires -rate or -interval")
	}
	if rate > 0 {
		return time.Second / time.Duration(rate), nil
	}
	if interval <= 0 {
		return 0, fmt.Errorf("interval must be greater than zero")
	}
	return interval, nil
}

func splitAndTrim(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
