# BaseService

`BaseService` is the foundation for all protobus services. It handles handler registration, message routing, and request/response correlation.

## Creating a Service

```go
import "github.com/ArielLaub/protobus-go"

type MyService struct {
    *protobus.BaseService
}

func NewMyService(ctx *protobus.Context) *MyService {
    s := &MyService{
        BaseService: protobus.NewBaseService(ctx, "my.ServiceName", "", nil),
    }
    s.RegisterHandlers(s) // Auto-discover methods via reflection
    return s
}
```

## Handler Registration

### Automatic (Reflection-based)

Use `RegisterHandlers` to auto-discover methods with the correct signature:

```go
s.RegisterHandlers(s)
```

Methods must have this signature:
```go
func (s *MyService) MethodName(
    ctx context.Context,
    data map[string]interface{},
    actor string,
    correlationID string,
) (map[string]interface{}, error)
```

### Manual Registration

Register handlers explicitly using `Handle`:

```go
s.Handle("add", func(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
    a := data["a"].(float64)
    b := data["b"].(float64)
    return map[string]interface{}{"result": a + b}, nil
})
```

## Methods

### NewBaseService(ctx *Context, serviceName, protoFileName string, options *ServiceOptions) *BaseService

Creates a new BaseService.

Parameters:
- `ctx` - The protobus Context
- `serviceName` - Unique service identifier (e.g., "calculator.MathService")
- `protoFileName` - Path to proto file (optional, for future protobuf support)
- `options` - Service configuration options

```go
service := protobus.NewBaseService(ctx, "calculator.MathService", "", &protobus.ServiceOptions{
    MaxConcurrent: 10,
})
```

### ServiceName() string

Returns the service name.

### Handle(method string, handler MethodHandler)

Registers a method handler.

```go
s.Handle("add", addHandler)
```

### RegisterHandlers(service interface{})

Auto-discovers and registers handlers via reflection.

```go
s.RegisterHandlers(s)
```

### Init() error

Initializes the service, starting message consumption.

```go
if err := s.Init(); err != nil {
    log.Fatal(err)
}
```

### Stop() error

Stops the service.

```go
s.Stop()
```

### Context() *Context

Returns the service's Context.

### PublishEvent(ctx context.Context, eventType string, data map[string]interface{}, topic string) error

Publishes an event.

```go
s.PublishEvent(ctx, "user.created", map[string]interface{}{"userId": "123"}, "")
```

### SubscribeEvent(pattern string) error

Subscribes to events matching a pattern.

```go
s.SubscribeEvent("user.*")
```

## ServiceOptions

```go
type ServiceOptions struct {
    // Maximum concurrent message processing (0 = unlimited)
    MaxConcurrent int

    // Retry configuration
    RetryOptions *RetryOptions
}
```

## Example: Complete Service

```go
package main

import (
    "context"
    "log"

    "github.com/ArielLaub/protobus-go"
)

// UserService handles user operations
type UserService struct {
    *protobus.BaseService
    users map[string]map[string]interface{}
}

func NewUserService(ctx *protobus.Context) *UserService {
    s := &UserService{
        BaseService: protobus.NewBaseService(ctx, "user.UserService", "", &protobus.ServiceOptions{
            MaxConcurrent: 10,
        }),
        users: make(map[string]map[string]interface{}),
    }
    s.RegisterHandlers(s)
    return s
}

func (s *UserService) GetUser(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
    id := data["id"].(string)

    user, exists := s.users[id]
    if !exists {
        return nil, protobus.NewHandledError("user not found", "USER_NOT_FOUND")
    }

    return user, nil
}

func (s *UserService) CreateUser(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
    id := data["id"].(string)
    name := data["name"].(string)

    s.users[id] = map[string]interface{}{
        "id":   id,
        "name": name,
    }

    // Publish event
    s.PublishEvent(ctx, "user.created", map[string]interface{}{"userId": id}, "")

    return map[string]interface{}{"success": true}, nil
}

func main() {
    ctx := protobus.NewContext(nil)
    if err := ctx.Init("amqp://localhost:5672/"); err != nil {
        log.Fatal(err)
    }
    defer ctx.Close()

    service := NewUserService(ctx)
    ctx.Factory().Parse("", service.ServiceName())

    if err := service.Init(); err != nil {
        log.Fatal(err)
    }

    log.Println("UserService running...")
    select {} // Block forever
}
```
