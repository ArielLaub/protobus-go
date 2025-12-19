# Protobus Go

A lightweight, scalable microservices message bus for Go. Leverages RabbitMQ for message routing and load balancing, combined with Protocol Buffers for efficient, type-safe serialization.

> **Note:** This is the official Go port of [Protobus](https://github.com/ArielLaub/protobus), originally written in TypeScript. Also available in [Python](https://github.com/ArielLaub/protobus-py).

## Why Protobus?

Unlike transport-agnostic frameworks that abstract away the message broker, Protobus **embraces RabbitMQ's native capabilities** directly. We leverage topic exchanges, routing keys, competing consumers, dead-letter queues, and message persistence - rather than re-implementing routing logic at the application level.

### RabbitMQ-Native Approach

**Message Routing**: By delegating routing to RabbitMQ's Erlang runtime instead of your Go process, Protobus eliminates the double-processing that transport-agnostic frameworks impose.

**Binary Serialization**: Protocol Buffers provide 3-10x smaller payloads than JSON, with compile-time type safety and built-in backward compatibility.

### Polyglot Advantage

Since Protobus uses standard Protobuf schemas and AMQP protocol, services written in **Go**, **TypeScript**, and **Python** can communicate seamlessly on the same message bus.

## Features

- **RPC Communication**: Request-response pattern with `ServiceProxy.Call()`
- **Event System**: Publish-subscribe with topic-based routing and wildcards
- **Auto-Reconnection**: Exponential backoff with jitter for resilient connections
- **Message Retry**: Automatic retry with dead-letter queue (DLQ) support
- **Custom Types**: Extensible type system (BigInt, Timestamp built-in)
- **Reflection-based Handlers**: Auto-discover service methods
- **CLI Tools**: Generate service stubs and typed clients from .proto files
- **Lifecycle Management**: RunnableService with graceful shutdown handling

## Requirements

- Go 1.21 or higher
- RabbitMQ 3.8 or higher

## Installation

```bash
go get github.com/ArielLaub/protobus-go
```

## Quick Start

### 1. Start RabbitMQ

```bash
docker-compose up -d
```

### 2. Create a Service

```go
package main

import (
    "context"
    "log"

    "github.com/ArielLaub/protobus-go"
)

// CalculatorService implements calculator.MathService
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

// Multiply handles the Multiply RPC call
func (s *CalculatorService) Multiply(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
    a := data["a"].(float64)
    b := data["b"].(float64)
    return map[string]interface{}{"result": a * b}, nil
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

    // Run until SIGINT/SIGTERM
    service.Run(context.Background())
}
```

### 3. Create a Client

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
}
```

## CLI Tools

Generate service stubs and typed clients:

```bash
# Install CLI
go install github.com/ArielLaub/protobus-go/cmd/protobus@latest

# Generate from proto files
protobus generate

# Generate service stub and client
protobus generate:service calculator.MathService

# Show setup instructions
protobus init
```

Generated client with interface:

```go
// MathServiceClient interface (for mocking)
type MathServiceClient interface {
    Add(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error)
    Multiply(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error)
}

// Usage
client, _ := NewMathServiceClient(ctx)
result, _ := client.Add(context.Background(), map[string]interface{}{"a": 5, "b": 3})
```

## API Overview

| Component | Description |
|-----------|-------------|
| `Context` | Manages connection and provides access to factory |
| `Connection` | RabbitMQ connection with auto-reconnection |
| `MessageFactory` | Encodes/decodes messages with custom type support |
| `BaseService` | Base service with reflection-based handler registration |
| `RunnableService` | Service with lifecycle management (signals, cleanup) |
| `ServiceProxy` | RPC client with `Call()` method |
| `ServiceCluster` | Manages multiple services |

## Error Handling

```go
// HandledError - expected errors that shouldn't trigger retries
err := protobus.NewHandledError("validation failed", "VALIDATION_ERROR")

// Check if error is handled
if protobus.IsHandledError(err) {
    // Don't retry
}
```

## Examples

### Calculator Service

A basic calculator demonstrating RPC calls:

```bash
# Terminal 1: Start service
go run examples/calculator/main.go service

# Terminal 2: Run client
go run examples/calculator/main.go client
```

### Combat Game

A battle royale game demonstrating multiple services with different AI strategies:

```bash
go run examples/combat/...
```

Features 6 player strategies:
- **Vindicator** - Shoots back at whoever attacked them
- **Bully Hunter** - Targets the weakest player
- **Giant Slayer** - Targets the strongest player
- **Equalizer** - Targets players with similar health
- **Wildcard** - Random target selection
- **Terminator** - Focuses on one target until eliminated

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Step-by-step guide to your first service |
| [Architecture](docs/architecture.md) | System design and component overview |
| [Configuration](docs/configuration.md) | Environment and connection settings |

### API Reference

| Component | Description |
|-----------|-------------|
| [Context](docs/api/context.md) | Connection and factory management |
| [BaseService](docs/api/base-service.md) | Foundation for implementing services |
| [RunnableService](docs/api/runnable-service.md) | Service with lifecycle management |
| [ServiceProxy](docs/api/service-proxy.md) | Client for calling remote services |

### Advanced Topics

| Topic | Description |
|-------|-------------|
| [Error Handling](docs/advanced/error-handling.md) | HandledError, retries, and DLQ |

## Cross-Language Compatibility

Services written in Go, TypeScript, and Python can communicate seamlessly:

```
┌─────────────┐     RabbitMQ      ┌─────────────┐
│  Go Service │ ◄──────────────► │  TS Service │
└─────────────┘                   └─────────────┘
       ▲                                 ▲
       │         ┌─────────────┐         │
       └────────►│  Py Client  │◄────────┘
                 └─────────────┘
```

## License

MIT License - Copyright (c) Remarkable Games Ltd.
