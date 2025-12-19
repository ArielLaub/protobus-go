# Architecture

This document describes the system architecture of protobus-go.

## Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        Application                          │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ BaseService  │  │RunnableService│  │  ServiceProxy    │  │
│  │              │  │  (lifecycle)  │  │    (client)      │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘  │
├─────────┴─────────────────┴───────────────────┴────────────┤
│                        Context                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ Connection   │  │MessageFactory│  │  BaseListener    │  │
│  │ (reconnect)  │  │  (encoding)  │  │  (consuming)     │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                      RabbitMQ                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   Exchange   │  │    Queues    │  │   Dead Letter    │  │
│  │   (topic)    │  │ (per service)│  │      Queue       │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### Context

The `Context` is the entry point for all protobus operations:

```go
ctx := protobus.NewContext(nil)
ctx.Init("amqp://localhost:5672")
defer ctx.Close()
```

It manages:
- RabbitMQ connection (with auto-reconnection)
- Message factory (encoding/decoding)
- Exchange declarations

### Connection

Handles the RabbitMQ connection with:
- Automatic reconnection with exponential backoff
- Connection pooling
- Channel management

```go
// Connection options
opts := &protobus.ConnectionOptions{
    MaxReconnectAttempts: 10,
    ReconnectDelayMs:     1000,
    MaxReconnectDelayMs:  30000,
}
```

### MessageFactory

Encodes and decodes messages:
- JSON serialization (compatible with other protobus implementations)
- Custom type support (BigInt, Timestamp)
- Preprocessing cache for performance

```go
factory := ctx.Factory()
factory.RegisterType(protobus.BigIntType{})
factory.RegisterType(protobus.TimestampType{})
```

### BaseService

Foundation for all services:
- Handler registration
- Message routing
- Request/response correlation

```go
type MyService struct {
    *protobus.BaseService
}

func NewMyService(ctx *protobus.Context) *MyService {
    s := &MyService{
        BaseService: protobus.NewBaseService(ctx, "my.Service", "", nil),
    }
    s.RegisterHandlers(s) // Auto-discover via reflection
    return s
}
```

### RunnableService

Extends BaseService with lifecycle management:
- SIGINT/SIGTERM handling
- Graceful shutdown
- Blocking Run() method

```go
service := protobus.NewRunnableService(ctx, "my.Service", nil)
service.OnStop(func() {
    // Cleanup logic
})
service.Run(context.Background()) // Blocks until signal
```

### ServiceProxy

RPC client for calling remote services:

```go
proxy := protobus.NewServiceProxy(ctx, "calculator.MathService")
proxy.Init()

var result map[string]interface{}
proxy.Call(ctx, "Add", map[string]interface{}{"a": 1, "b": 2}, &result)
```

### BaseListener

Handles message consumption:
- Queue binding
- Message acknowledgment
- Retry logic with DLQ support

## Message Flow

### RPC Request/Response

```
1. Client calls proxy.Call("Add", {a: 1, b: 2})
2. Proxy builds request message
3. Proxy publishes to exchange with routing key: REQUEST.Service.Add
4. RabbitMQ routes to service queue
5. Service listener receives message
6. Service calls handler method
7. Handler returns result
8. Service publishes response to reply queue
9. Proxy receives response
10. Proxy returns result to caller
```

### Competing Consumers

Multiple service instances can consume from the same queue:

```
                    ┌─────────────┐
                    │   Queue     │
                    │ (Service.A) │
                    └──────┬──────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
      ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
      │Instance 1 │  │Instance 2 │  │Instance 3 │
      └───────────┘  └───────────┘  └───────────┘
```

RabbitMQ distributes messages round-robin among instances.

## Retry and Dead Letter Queue

```
Message arrives
      │
      ▼
  Handler OK? ─────Yes────► Acknowledge
      │
      No
      │
      ▼
  Retry count < max? ────Yes────► Requeue with delay
      │
      No
      │
      ▼
  Send to DLQ
```

Configuration:
```go
opts := &protobus.RetryOptions{
    MaxRetries:   3,
    RetryDelayMs: 1000,
}
```

## Exchange Topology

Protobus uses topic exchanges for flexible routing:

```
Exchange: protobus.bus (topic)
├── REQUEST.calculator.MathService.Add → calculator.MathService queue
├── REQUEST.calculator.MathService.Subtract → calculator.MathService queue
└── REQUEST.user.AuthService.* → user.AuthService queue

Exchange: protobus.events (topic)
├── user.created → subscriber queues
├── order.* → subscriber queues
└── #.error → error handler queue
```
