# Configuration

Protobus-go can be configured through code or environment variables.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection URL |
| `PROTOBUS_EXCHANGE` | `protobus.bus` | Main message exchange name |
| `PROTOBUS_EVENTS_EXCHANGE` | `protobus.events` | Events exchange name |
| `PROTOBUS_RPC_TIMEOUT` | `30000` | Default RPC timeout in milliseconds |
| `PROTOBUS_MSG_TIMEOUT` | `60000` | Message processing timeout in ms |

## Code Configuration

### Context Options

```go
ctx := protobus.NewContext(&protobus.ContextOptions{
    ConnectionOptions: &protobus.ConnectionOptions{
        MaxReconnectAttempts: 10,    // 0 = unlimited
        ReconnectDelayMs:     1000,  // Initial delay
        MaxReconnectDelayMs:  30000, // Maximum delay (with backoff)
    },
})
```

### Service Options

```go
service := protobus.NewRunnableService(ctx, "my.Service", &protobus.ServiceOptions{
    MaxConcurrent: 10,  // Max parallel message processing
    RetryOptions: &protobus.RetryOptions{
        MaxRetries:   3,
        RetryDelayMs: 1000,
    },
})
```

### Global Configuration

Access the global config singleton:

```go
config := protobus.GetConfig()
```

## Connection URL Format

```
amqp://username:password@hostname:port/vhost
```

Examples:
```
amqp://guest:guest@localhost:5672/
amqp://user:pass@rabbitmq.example.com:5672/production
amqps://user:pass@rabbitmq.example.com:5671/  # TLS
```

## Retry Configuration

Configure automatic retry behavior for failed messages:

```go
retryOpts := &protobus.RetryOptions{
    MaxRetries:   3,     // Number of retry attempts
    RetryDelayMs: 1000,  // Delay between retries
}
```

Messages that fail with:
- **HandledError**: No retry, error sent to client
- **Other errors**: Retry up to MaxRetries, then send to DLQ

## Concurrency Configuration

Control parallel message processing:

```go
serviceOpts := &protobus.ServiceOptions{
    MaxConcurrent: 10,  // Process up to 10 messages simultaneously
}
```

When `MaxConcurrent > 0`:
- Messages are manually acknowledged after processing
- RabbitMQ prefetch is set to the specified limit
- Provides backpressure control

When `MaxConcurrent = 0`:
- Messages are auto-acknowledged on receive
- Unlimited parallel processing
- Higher throughput, less control

## Exchange Names

Configure exchange names for message routing:

```go
config := protobus.GetConfig()
config.BusExchangeName = "myapp.bus"
config.EventsExchangeName = "myapp.events"
```

## Timeouts

### RPC Timeout

Maximum time to wait for an RPC response:

```go
config := protobus.GetConfig()
config.DefaultRPCTimeout = 30 * time.Second
```

Or use context timeout per-call:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
proxy.Call(ctx, "Method", data, &result)
```

### Message Processing Timeout

Maximum time for a handler to process a message:

```go
config := protobus.GetConfig()
config.MessageProcessingTimeout = 60 * time.Second
```

## Example: Full Configuration

```go
package main

import (
    "log"
    "os"
    "time"

    "github.com/ArielLaub/protobus-go"
)

func main() {
    // Set global config
    config := protobus.GetConfig()
    config.BusExchangeName = "myapp.bus"
    config.EventsExchangeName = "myapp.events"
    config.DefaultRPCTimeout = 10 * time.Second
    config.MessageProcessingTimeout = 30 * time.Second

    // Create context with options
    ctx := protobus.NewContext(&protobus.ContextOptions{
        ConnectionOptions: &protobus.ConnectionOptions{
            MaxReconnectAttempts: 10,
            ReconnectDelayMs:     1000,
            MaxReconnectDelayMs:  30000,
        },
    })

    // Get URL from environment
    url := os.Getenv("RABBITMQ_URL")
    if url == "" {
        url = "amqp://guest:guest@localhost:5672/"
    }

    if err := ctx.Init(url); err != nil {
        log.Fatal(err)
    }
    defer ctx.Close()

    // Create service with options
    service := protobus.NewRunnableService(ctx, "my.Service", &protobus.ServiceOptions{
        MaxConcurrent: 10,
        RetryOptions: &protobus.RetryOptions{
            MaxRetries:   3,
            RetryDelayMs: 1000,
        },
    })

    // ...
}
```
