# ServiceProxy

The `ServiceProxy` provides a client interface for making RPC calls to remote services.

## Creating a Proxy

```go
import "github.com/ArielLaub/protobus-go"

proxy := protobus.NewServiceProxy(ctx, "calculator.MathService")
if err := proxy.Init(); err != nil {
    log.Fatal(err)
}
defer proxy.Close()
```

## Making RPC Calls

```go
var result map[string]interface{}

err := proxy.Call(
    context.Background(),
    "Add",
    map[string]interface{}{"a": 5, "b": 3},
    &result,
)

if err != nil {
    log.Printf("Error: %v", err)
} else {
    fmt.Printf("Result: %v\n", result["result"])
}
```

## Methods

### NewServiceProxy(ctx *Context, serviceName string) *ServiceProxy

Creates a new service proxy for the specified service.

```go
proxy := protobus.NewServiceProxy(ctx, "user.AuthService")
```

### Init() error

Initializes the proxy, creating the reply queue and starting the consumer.

```go
if err := proxy.Init(); err != nil {
    log.Fatal(err)
}
```

### Call(ctx context.Context, method string, data map[string]interface{}, result interface{}) error

Makes an RPC call to the service.

Parameters:
- `ctx` - Context for cancellation/timeout
- `method` - Method name to call
- `data` - Request payload
- `result` - Pointer to receive the response (usually `*map[string]interface{}`)

```go
var result map[string]interface{}
err := proxy.Call(ctx, "GetUser", map[string]interface{}{"id": "123"}, &result)
```

### Close() error

Closes the proxy and releases resources.

```go
defer proxy.Close()
```

### ServiceName() string

Returns the service name this proxy targets.

```go
name := proxy.ServiceName() // "calculator.MathService"
```

## Timeout Handling

Use context with timeout for RPC calls:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

err := proxy.Call(ctx, "SlowOperation", data, &result)
if err != nil {
    if errors.Is(err, context.DeadlineExceeded) {
        log.Println("Request timed out")
    }
}
```

## Error Handling

```go
err := proxy.Call(ctx, "ValidateInput", data, &result)
if err != nil {
    // Check if it's a handled error (expected business error)
    if protobus.IsHandledError(err) {
        he, _ := protobus.GetHandledError(err)
        log.Printf("Validation failed: %s (code: %s)", he.Message, he.Code)
    } else {
        // Unexpected error
        log.Printf("System error: %v", err)
    }
}
```

## Example: Complete Client

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/ArielLaub/protobus-go"
)

func main() {
    // Create and initialize context
    ctx := protobus.NewContext(nil)
    if err := ctx.Init("amqp://localhost:5672/"); err != nil {
        log.Fatal(err)
    }
    defer ctx.Close()

    // Create proxy
    proxy := protobus.NewServiceProxy(ctx, "calculator.MathService")
    if err := proxy.Init(); err != nil {
        log.Fatal(err)
    }
    defer proxy.Close()

    // Make multiple calls
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
        callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

        var result map[string]interface{}
        err := proxy.Call(
            callCtx,
            op.method,
            map[string]interface{}{"a": op.a, "b": op.b},
            &result,
        )
        cancel()

        if err != nil {
            log.Printf("%s(%.0f, %.0f) error: %v", op.method, op.a, op.b, err)
        } else {
            fmt.Printf("%s(%.0f, %.0f) = %v\n", op.method, op.a, op.b, result["result"])
        }
    }
}
```

## Concurrent Calls

ServiceProxy is safe for concurrent use:

```go
var wg sync.WaitGroup

for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(n int) {
        defer wg.Done()

        var result map[string]interface{}
        err := proxy.Call(ctx, "Process", map[string]interface{}{"n": n}, &result)
        if err != nil {
            log.Printf("Error: %v", err)
        }
    }(i)
}

wg.Wait()
```
