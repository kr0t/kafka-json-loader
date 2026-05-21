package loader

import "testing"

func TestBuildGeneratedRequest(t *testing.T) {
	request, err := BuildGeneratedRequest(GeneratorOptions{
		Brokers:   []string{"localhost:9092"},
		Topic:     "demo.events",
		Count:     2,
		KeyPrefix: "auto",
		EventType: "order.created",
		Source:    "test-suite",
	})
	if err != nil {
		t.Fatalf("BuildGeneratedRequest returned error: %v", err)
	}

	if len(request.Messages) != 2 {
		t.Fatalf("unexpected message count: %d", len(request.Messages))
	}

	if request.Messages[0].Key == nil {
		t.Fatal("expected generated key")
	}

	if len(request.Messages[0].Headers) == 0 {
		t.Fatal("expected generated headers")
	}
}

func TestBuildGeneratedRequestStartSequence(t *testing.T) {
	request, err := BuildGeneratedRequest(GeneratorOptions{
		Brokers:       []string{"localhost:9092"},
		Topic:         "demo.events",
		Count:         2,
		StartSequence: 10,
	})
	if err != nil {
		t.Fatalf("BuildGeneratedRequest returned error: %v", err)
	}

	firstKey, ok := request.Messages[0].Key.(string)
	if !ok {
		t.Fatalf("unexpected key type: %T", request.Messages[0].Key)
	}
	if firstKey == "" || request.Messages[0].Time == nil {
		t.Fatal("expected generated key and time")
	}
}

func TestBuildGeneratedRequestRejectsMissingBrokers(t *testing.T) {
	_, err := BuildGeneratedRequest(GeneratorOptions{
		Topic: "demo.events",
	})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}
