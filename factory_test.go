package protobus

import (
	"testing"
)

func TestMessageFactoryBuildRequest(t *testing.T) {
	factory := NewMessageFactory()
	factory.Init()

	data := map[string]interface{}{
		"a": float64(5),
		"b": float64(3),
	}

	bytes, err := factory.BuildRequest("calculator.MathService.Add", data, "test-actor")
	if err != nil {
		t.Fatalf("BuildRequest failed: %v", err)
	}

	// Decode and verify
	decoded, err := factory.DecodeRequest(bytes)
	if err != nil {
		t.Fatalf("DecodeRequest failed: %v", err)
	}

	if decoded.Method != "calculator.MathService.Add" {
		t.Errorf("Expected method calculator.MathService.Add, got %s", decoded.Method)
	}

	if decoded.Actor != "test-actor" {
		t.Errorf("Expected actor test-actor, got %s", decoded.Actor)
	}

	if decoded.Data["a"] != float64(5) {
		t.Errorf("Expected a=5, got %v", decoded.Data["a"])
	}
}

func TestMessageFactoryBuildResponse(t *testing.T) {
	factory := NewMessageFactory()
	factory.Init()

	result := map[string]interface{}{
		"result": float64(8),
	}

	bytes, err := factory.BuildResponse("calculator.MathService.Add", result, nil)
	if err != nil {
		t.Fatalf("BuildResponse failed: %v", err)
	}

	decoded, err := factory.DecodeResponse(bytes)
	if err != nil {
		t.Fatalf("DecodeResponse failed: %v", err)
	}

	if decoded.Method != "calculator.MathService.Add" {
		t.Errorf("Expected method calculator.MathService.Add, got %s", decoded.Method)
	}

	if decoded.Error != nil {
		t.Errorf("Expected no error, got %v", decoded.Error)
	}
}

func TestMessageFactoryBuildResponseWithError(t *testing.T) {
	factory := NewMessageFactory()
	factory.Init()

	err := NewHandledError("validation failed", "VALIDATION_ERROR")

	bytes, buildErr := factory.BuildResponse("calculator.MathService.Add", nil, err)
	if buildErr != nil {
		t.Fatalf("BuildResponse failed: %v", buildErr)
	}

	decoded, decodeErr := factory.DecodeResponse(bytes)
	if decodeErr != nil {
		t.Fatalf("DecodeResponse failed: %v", decodeErr)
	}

	if decoded.Error == nil {
		t.Fatal("Expected error in response")
	}

	if decoded.Error.Code != "VALIDATION_ERROR" {
		t.Errorf("Expected code VALIDATION_ERROR, got %s", decoded.Error.Code)
	}
}

func TestMessageFactoryBuildEvent(t *testing.T) {
	factory := NewMessageFactory()
	factory.Init()

	data := map[string]interface{}{
		"message": "hello",
	}

	bytes, err := factory.BuildEvent("test.event", data, "custom.topic")
	if err != nil {
		t.Fatalf("BuildEvent failed: %v", err)
	}

	decoded, err := factory.DecodeEvent(bytes)
	if err != nil {
		t.Fatalf("DecodeEvent failed: %v", err)
	}

	if decoded.Type != "test.event" {
		t.Errorf("Expected type test.event, got %s", decoded.Type)
	}

	if decoded.Topic != "custom.topic" {
		t.Errorf("Expected topic custom.topic, got %s", decoded.Topic)
	}
}

func TestMessageFactoryNotInitialized(t *testing.T) {
	factory := NewMessageFactory()
	// Don't call Init()

	_, err := factory.BuildRequest("test", nil, "")
	if err != ErrNotInitialized {
		t.Errorf("Expected ErrNotInitialized, got %v", err)
	}
}
