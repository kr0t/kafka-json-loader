package loader

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

type GeneratorOptions struct {
	Brokers            []string
	Topic              string
	ClientID           string
	RequiredAcks       string
	Compression        string
	Count              int
	KeyPrefix          string
	EventType          string
	Source             string
	BatchTimeoutMillis int
	WriteTimeoutMillis int
	ReadTimeoutMillis  int
	SSL                *SSLConfig
}

type generatedValue struct {
	ID         string         `json:"id"`
	Sequence   int            `json:"sequence"`
	EventType  string         `json:"eventType"`
	Source     string         `json:"source"`
	GeneratedAt string        `json:"generatedAt"`
	Customer   generatedParty `json:"customer"`
	Order      generatedOrder `json:"order"`
	Flags      generatedFlags `json:"flags"`
}

type generatedParty struct {
	ID    int      `json:"id"`
	Name  string   `json:"name"`
	Email string   `json:"email"`
	Tags  []string `json:"tags"`
}

type generatedOrder struct {
	Number   string             `json:"number"`
	Currency string             `json:"currency"`
	Amount   float64            `json:"amount"`
	Items    []generatedOrderItem `json:"items"`
}

type generatedOrderItem struct {
	SKU   string `json:"sku"`
	Qty   int    `json:"qty"`
	Price float64 `json:"price"`
}

type generatedFlags struct {
	Priority bool `json:"priority"`
	TestData bool `json:"testData"`
}

func BuildGeneratedRequest(options GeneratorOptions) (*Request, error) {
	if options.Count <= 0 {
		options.Count = 1
	}
	if strings.TrimSpace(options.KeyPrefix) == "" {
		options.KeyPrefix = "msg"
	}
	if strings.TrimSpace(options.EventType) == "" {
		options.EventType = "generated"
	}
	if strings.TrimSpace(options.Source) == "" {
		options.Source = "kafka-json-loader"
	}
	if options.BatchTimeoutMillis <= 0 {
		options.BatchTimeoutMillis = 1000
	}
	if options.WriteTimeoutMillis <= 0 {
		options.WriteTimeoutMillis = 10000
	}
	if options.ReadTimeoutMillis <= 0 {
		options.ReadTimeoutMillis = 10000
	}

	request := &Request{
		Brokers:            options.Brokers,
		Topic:              options.Topic,
		ClientID:           options.ClientID,
		RequiredAcks:       options.RequiredAcks,
		Compression:        options.Compression,
		BatchTimeoutMillis: options.BatchTimeoutMillis,
		WriteTimeoutMillis: options.WriteTimeoutMillis,
		ReadTimeoutMillis:  options.ReadTimeoutMillis,
		SSL:                options.SSL,
		Messages:           make([]MessageInput, 0, options.Count),
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	now := time.Now().UTC()

	for i := 0; i < options.Count; i++ {
		generatedAt := now.Add(time.Duration(i) * time.Millisecond)
		messageID := fmt.Sprintf("%s-%d-%06d", options.KeyPrefix, generatedAt.Unix(), i+1)

		value := generatedValue{
			ID:          messageID,
			Sequence:    i + 1,
			EventType:   options.EventType,
			Source:      options.Source,
			GeneratedAt: generatedAt.Format(time.RFC3339Nano),
			Customer: generatedParty{
				ID:    1000 + i + 1,
				Name:  fmt.Sprintf("Customer %d", i+1),
				Email: fmt.Sprintf("customer%d@example.local", i+1),
				Tags:  []string{"generated", "windows", "kafka"},
			},
			Order: generatedOrder{
				Number:   fmt.Sprintf("ORD-%06d", i+1),
				Currency: "RUB",
				Amount:   generatedAmount(seededRand),
				Items: []generatedOrderItem{
					{SKU: fmt.Sprintf("SKU-%03d", (i%10)+1), Qty: 1 + i%3, Price: 499.90},
					{SKU: fmt.Sprintf("SKU-%03d", ((i+1)%10)+1), Qty: 1, Price: 999.50},
				},
			},
			Flags: generatedFlags{
				Priority: i%2 == 0,
				TestData: true,
			},
		}

		headerMap := map[string]any{
			"content-type": "application/json",
			"generator":    "kafka-json-loader",
			"source":       options.Source,
			"host":         hostname,
			"sequence":     i + 1,
			"event-type":   options.EventType,
		}

		headers, err := json.Marshal(headerMap)
		if err != nil {
			return nil, fmt.Errorf("marshal generated headers: %w", err)
		}

		request.Messages = append(request.Messages, MessageInput{
			Key:     messageID,
			Value:   value,
			Headers: headers,
			Time:    &generatedAt,
		})
	}

	return validateRequest(request)
}

func generatedAmount(seededRand *rand.Rand) float64 {
	value := 1000 + seededRand.Intn(90000)
	return float64(value) / 100
}
