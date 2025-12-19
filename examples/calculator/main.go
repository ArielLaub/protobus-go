// Example: Calculator service demonstrating protobus-go usage
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	protobus "github.com/ArielLaub/protobus-go"
)

// CalculatorService implements calculator.MathService
type CalculatorService struct {
	*protobus.RunnableService
}

// NewCalculatorService creates a new calculator service.
func NewCalculatorService(ctx *protobus.Context) *CalculatorService {
	s := &CalculatorService{
		RunnableService: protobus.NewRunnableService(ctx, "calculator.MathService", nil),
	}
	// Auto-discover and register all methods via reflection
	s.RegisterHandlers(s)
	return s
}

// Add handles the Add RPC call.
func (s *CalculatorService) Add(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	a := data["a"].(float64)
	b := data["b"].(float64)
	return map[string]interface{}{"result": a + b}, nil
}

// Subtract handles the Subtract RPC call.
func (s *CalculatorService) Subtract(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	a := data["a"].(float64)
	b := data["b"].(float64)
	return map[string]interface{}{"result": a - b}, nil
}

// Multiply handles the Multiply RPC call.
func (s *CalculatorService) Multiply(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	a := data["a"].(float64)
	b := data["b"].(float64)
	return map[string]interface{}{"result": a * b}, nil
}

// Divide handles the Divide RPC call.
func (s *CalculatorService) Divide(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	a := data["a"].(float64)
	b := data["b"].(float64)
	if b == 0 {
		return nil, protobus.NewHandledError("division by zero", "DIVISION_BY_ZERO")
	}
	return map[string]interface{}{"result": a / b}, nil
}

func runService(ctx *protobus.Context) {
	service := NewCalculatorService(ctx)
	ctx.Factory().Parse("", service.ServiceName())

	if err := service.Init(); err != nil {
		log.Fatalf("Failed to init service: %v", err)
	}

	log.Println("Calculator service running...")
	service.Run(context.Background())
}

func runClient(ctx *protobus.Context) {
	proxy := protobus.NewServiceProxy(ctx, "calculator.MathService")
	if err := proxy.Init(); err != nil {
		log.Fatalf("Failed to init proxy: %v", err)
	}
	defer proxy.Close()

	// Make some RPC calls
	operations := []struct {
		method string
		a, b   float64
	}{
		{"Add", 10, 5},
		{"Subtract", 10, 5},
		{"Multiply", 10, 5},
		{"Divide", 10, 5},
	}

	for _, op := range operations {
		var result map[string]interface{}
		err := proxy.Call(
			context.Background(),
			op.method,
			map[string]interface{}{"a": op.a, "b": op.b},
			&result,
		)
		if err != nil {
			log.Printf("%s(%.0f, %.0f) error: %v", op.method, op.a, op.b, err)
		} else {
			log.Printf("%s(%.0f, %.0f) = %v", op.method, op.a, op.b, result["result"])
		}
	}

	// Test error handling
	var result map[string]interface{}
	err := proxy.Call(
		context.Background(),
		"Divide",
		map[string]interface{}{"a": 10.0, "b": 0.0},
		&result,
	)
	if err != nil {
		log.Printf("Divide(10, 0) correctly returned error: %v", err)
	}
}

func main() {
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	ctx := protobus.NewContext(nil)
	if err := ctx.Init(rabbitURL); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer ctx.Close()

	mode := "service"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	switch mode {
	case "service":
		runService(ctx)
	case "client":
		runClient(ctx)
	case "both":
		// Run service in background, then client
		go runService(ctx)
		time.Sleep(2 * time.Second) // Wait for service to start
		runClient(ctx)
	default:
		fmt.Println("Usage: calculator [service|client|both]")
		os.Exit(1)
	}
}
