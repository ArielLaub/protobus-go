# Context

The `Context` is the central component that manages the RabbitMQ connection and provides access to all protobus functionality.

## Creating a Context

```go
import "github.com/ArielLaub/protobus-go"

// Basic context
ctx := protobus.NewContext(nil)

// With options
ctx := protobus.NewContext(&protobus.ContextOptions{
    ConnectionOptions: &protobus.ConnectionOptions{
        MaxReconnectAttempts: 10,
        ReconnectDelayMs:     1000,
    },
})
```

## Initialization

```go
err := ctx.Init("amqp://guest:guest@localhost:5672/")
if err != nil {
    log.Fatal(err)
}
defer ctx.Close()
```

The `Init` method:
1. Initializes the message factory
2. Establishes RabbitMQ connection
3. Declares required exchanges

## Methods

### Init(url string) error

Initializes the context and connects to RabbitMQ.

```go
err := ctx.Init("amqp://localhost:5672/")
```

### Close() error

Closes the connection and releases resources.

```go
defer ctx.Close()
```

### Connection() *Connection

Returns the underlying connection for advanced operations.

```go
conn := ctx.Connection()
isConnected := conn.IsConnected()
```

### Factory() *MessageFactory

Returns the message factory for encoding/decoding.

```go
factory := ctx.Factory()
factory.RegisterType(myCustomType)
```

### IsConnected() bool

Returns whether the context is connected to RabbitMQ.

```go
if ctx.IsConnected() {
    // Safe to proceed
}
```

### PublishEvent(ctx context.Context, eventType string, data map[string]interface{}, topic string) error

Publishes an event to the events exchange.

```go
err := ctx.PublishEvent(
    context.Background(),
    "user.created",
    map[string]interface{}{"userId": "123"},
    "",
)
```

### ExchangeName() string

Returns the main bus exchange name (default: "protobus.bus").

### EventsExchangeName() string

Returns the events exchange name (default: "protobus.events").

## Connection Options

```go
type ConnectionOptions struct {
    // Maximum reconnection attempts (0 = unlimited)
    MaxReconnectAttempts int

    // Initial delay between reconnection attempts (ms)
    ReconnectDelayMs int

    // Maximum delay between reconnection attempts (ms)
    MaxReconnectDelayMs int
}
```

## Example: Full Lifecycle

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/ArielLaub/protobus-go"
)

func main() {
    // Create context with custom options
    ctx := protobus.NewContext(&protobus.ContextOptions{
        ConnectionOptions: &protobus.ConnectionOptions{
            MaxReconnectAttempts: 10,
            ReconnectDelayMs:     1000,
            MaxReconnectDelayMs:  30000,
        },
    })

    // Initialize
    if err := ctx.Init(os.Getenv("RABBITMQ_URL")); err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer ctx.Close()

    // Use the context...
    log.Println("Connected:", ctx.IsConnected())

    // Wait for shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    log.Println("Shutting down...")
}
```
