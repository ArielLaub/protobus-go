# RunnableService

`RunnableService` extends `BaseService` with lifecycle management, including signal handling and graceful shutdown.

## Creating a RunnableService

```go
import "github.com/ArielLaub/protobus-go"

service := protobus.NewRunnableService(ctx, "my.Service", nil)
```

Or embed in your own struct:

```go
type MyService struct {
    *protobus.RunnableService
}

func NewMyService(ctx *protobus.Context) *MyService {
    s := &MyService{
        RunnableService: protobus.NewRunnableService(ctx, "my.Service", nil),
    }
    s.RegisterHandlers(s)
    return s
}
```

## Lifecycle Management

### Running the Service

```go
service.Run(context.Background())
```

The `Run` method:
1. Sets up SIGINT/SIGTERM handlers
2. Blocks until a signal is received
3. Calls cleanup hooks
4. Stops the service gracefully

### Cleanup Hooks

Register cleanup functions to run on shutdown:

```go
service.OnStop(func() {
    log.Println("Closing database connection...")
    db.Close()
})

service.OnStop(func() {
    log.Println("Flushing cache...")
    cache.Flush()
})
```

Hooks are called in reverse order (LIFO).

## Methods

### NewRunnableService(ctx *Context, serviceName string, options *ServiceOptions) *RunnableService

Creates a new RunnableService.

```go
service := protobus.NewRunnableService(ctx, "my.Service", &protobus.ServiceOptions{
    MaxConcurrent: 10,
})
```

### Run(ctx context.Context)

Starts the service and blocks until shutdown signal.

```go
service.Run(context.Background())
```

### Stop()

Manually triggers shutdown.

```go
service.Stop()
```

### OnStop(fn func())

Registers a cleanup hook.

```go
service.OnStop(func() {
    // Cleanup logic
})
```

### IsRunning() bool

Returns whether the service is currently running.

## Example: Service with Cleanup

```go
package main

import (
    "context"
    "log"

    "github.com/ArielLaub/protobus-go"
)

type OrderService struct {
    *protobus.RunnableService
    pendingOrders []string
}

func NewOrderService(ctx *protobus.Context) *OrderService {
    s := &OrderService{
        RunnableService: protobus.NewRunnableService(ctx, "order.OrderService", nil),
        pendingOrders:   make([]string, 0),
    }
    s.RegisterHandlers(s)
    return s
}

func (s *OrderService) CreateOrder(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
    orderID := data["orderId"].(string)
    s.pendingOrders = append(s.pendingOrders, orderID)
    return map[string]interface{}{"success": true, "orderId": orderID}, nil
}

func main() {
    ctx := protobus.NewContext(nil)
    if err := ctx.Init("amqp://localhost:5672/"); err != nil {
        log.Fatal(err)
    }
    defer ctx.Close()

    service := NewOrderService(ctx)
    ctx.Factory().Parse("", service.ServiceName())

    // Register cleanup hooks
    service.OnStop(func() {
        log.Printf("Processing %d pending orders before shutdown...", len(service.pendingOrders))
        for _, orderID := range service.pendingOrders {
            log.Printf("Saving order %s to database", orderID)
        }
    })

    if err := service.Init(); err != nil {
        log.Fatal(err)
    }

    log.Println("OrderService running... Press Ctrl+C to stop")
    service.Run(context.Background()) // Blocks until SIGINT/SIGTERM

    log.Println("OrderService stopped gracefully")
}
```

## Signal Handling

RunnableService handles these signals:
- `SIGINT` (Ctrl+C)
- `SIGTERM` (kill command)

When a signal is received:
1. `Run()` unblocks
2. Cleanup hooks are called
3. Service is stopped
4. Control returns to caller
