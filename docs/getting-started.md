# Getting Started

This guide walks you through creating your first protobus-go microservice.

## Prerequisites

- Go 1.21 or higher
- RabbitMQ 3.8 or higher
- Docker (optional, for running RabbitMQ)

## Installation

```bash
go get github.com/ArielLaub/protobus-go
```

## Start RabbitMQ

Using Docker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Or use the included docker-compose:

```bash
docker-compose up -d
```

## Create Your First Service

### 1. Create a Calculator Service

```go
package main

import (
    "context"
    "log"

    "github.com/ArielLaub/protobus-go"
)

// CalculatorService handles math operations
type CalculatorService struct {
    *protobus.RunnableService
}

func NewCalculatorService(ctx *protobus.Context) *CalculatorService {
    s := &CalculatorService{
        RunnableService: protobus.NewRunnableService(ctx, "calculator.MathService", nil),
    }
    s.RegisterHandlers(s) // Auto-discover methods via reflection
    return s
}

// Add handles the Add RPC call
func (s *CalculatorService) Add(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
    a := data["a"].(float64)
    b := data["b"].(float64)
    return map[string]interface{}{"result": a + b}, nil
}

// Subtract handles the Subtract RPC call
func (s *CalculatorService) Subtract(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
    a := data["a"].(float64)
    b := data["b"].(float64)
    return map[string]interface{}{"result": a - b}, nil
}

func main() {
    ctx := protobus.NewContext(nil)
    if err := ctx.Init("amqp://guest:guest@localhost:5672/"); err != nil {
        log.Fatal(err)
    }
    defer ctx.Close()

    service := NewCalculatorService(ctx)
    ctx.Factory().Parse("", service.ServiceName())

    if err := service.Init(); err != nil {
        log.Fatal(err)
    }

    log.Println("Calculator service running...")
    service.Run(context.Background()) // Blocks until SIGINT/SIGTERM
}
```

### 2. Create a Client

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ArielLaub/protobus-go"
)

func main() {
    ctx := protobus.NewContext(nil)
    if err := ctx.Init("amqp://guest:guest@localhost:5672/"); err != nil {
        log.Fatal(err)
    }
    defer ctx.Close()

    // Create proxy
    proxy := protobus.NewServiceProxy(ctx, "calculator.MathService")
    if err := proxy.Init(); err != nil {
        log.Fatal(err)
    }
    defer proxy.Close()

    // Make RPC calls
    var result map[string]interface{}

    err := proxy.Call(context.Background(), "Add", map[string]interface{}{"a": 5, "b": 3}, &result)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("5 + 3 = %v\n", result["result"]) // Output: 5 + 3 = 8

    err = proxy.Call(context.Background(), "Subtract", map[string]interface{}{"a": 10, "b": 4}, &result)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("10 - 4 = %v\n", result["result"]) // Output: 10 - 4 = 6
}
```

### 3. Run the Example

```bash
# Terminal 1: Start the service
go run examples/calculator/main.go service

# Terminal 2: Run the client
go run examples/calculator/main.go client
```

## Handler Signature

All RPC handlers must follow this signature:

```go
func (s *Service) MethodName(
    ctx context.Context,
    data map[string]interface{},
    actor string,
    correlationID string,
) (map[string]interface{}, error)
```

Parameters:
- `ctx` - Context for cancellation/timeout
- `data` - Request payload as a map
- `actor` - Optional actor/user identifier
- `correlationID` - Unique request identifier for tracing

Returns:
- Response data as a map
- Error (use `HandledError` for expected errors)

## Error Handling

```go
// Return expected errors without triggering retries
if input < 0 {
    return nil, protobus.NewHandledError("input must be positive", "INVALID_INPUT")
}

// Unexpected errors will trigger retry logic
return nil, fmt.Errorf("database connection failed: %w", err)
```

## Next Steps

- [Architecture](architecture.md) - Understand the system design
- [API Reference](api/context.md) - Detailed API documentation
- [CLI Tools](cli.md) - Generate services and clients
