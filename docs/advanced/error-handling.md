# Error Handling

Protobus-go provides structured error handling with retry logic and dead-letter queue support.

## HandledError

Use `HandledError` for expected business errors that should NOT trigger retries:

```go
import "github.com/ArielLaub/protobus-go"

func (s *Service) ValidateInput(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
    input := data["value"].(float64)

    if input < 0 {
        return nil, protobus.NewHandledError("input must be positive", "INVALID_INPUT")
    }

    if input > 1000 {
        return nil, protobus.NewHandledError("input exceeds maximum", "INPUT_TOO_LARGE")
    }

    return map[string]interface{}{"valid": true}, nil
}
```

### Creating HandledError

```go
err := protobus.NewHandledError("error message", "ERROR_CODE")
```

Parameters:
- `message` - Human-readable error message
- `code` - Machine-readable error code

### Checking for HandledError

```go
if protobus.IsHandledError(err) {
    // This is an expected error, don't retry
    he, _ := protobus.GetHandledError(err)
    log.Printf("Handled error: %s (code: %s)", he.Message, he.Code)
}
```

## Error Types

### HandledError

Expected business errors that are sent to the client without retrying.

```go
// Service side
return nil, protobus.NewHandledError("user not found", "USER_NOT_FOUND")

// Client side
err := proxy.Call(ctx, "GetUser", data, &result)
if protobus.IsHandledError(err) {
    he, _ := protobus.GetHandledError(err)
    switch he.Code {
    case "USER_NOT_FOUND":
        // Handle missing user
    case "PERMISSION_DENIED":
        // Handle authorization error
    }
}
```

### TimeoutError

Returned when an RPC call times out.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

err := proxy.Call(ctx, "SlowMethod", data, &result)
if te, ok := err.(*protobus.TimeoutError); ok {
    log.Printf("Timeout: %s after %s", te.Operation, te.Duration)
}
```

### Standard Errors

Any non-HandledError will trigger the retry mechanism:

```go
func (s *Service) ProcessOrder(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
    // This error WILL trigger retries
    if err := s.db.Save(data); err != nil {
        return nil, fmt.Errorf("database error: %w", err)
    }

    // This error will NOT trigger retries
    if data["amount"].(float64) <= 0 {
        return nil, protobus.NewHandledError("invalid amount", "INVALID_AMOUNT")
    }

    return map[string]interface{}{"success": true}, nil
}
```

## Retry Configuration

Configure retry behavior with `RetryOptions`:

```go
opts := &protobus.RetryOptions{
    MaxRetries:   3,        // Maximum retry attempts
    RetryDelayMs: 1000,     // Delay between retries (ms)
}

service := protobus.NewBaseService(ctx, "my.Service", "", &protobus.ServiceOptions{
    RetryOptions: opts,
})
```

### Default Configuration

```go
func DefaultRetryOptions() *RetryOptions {
    return &RetryOptions{
        MaxRetries:   3,
        RetryDelayMs: 1000,
    }
}
```

## Dead Letter Queue (DLQ)

Messages that exhaust all retries are sent to a Dead Letter Queue:

```
Queue: REQUEST.my.Service.Method.DLQ
```

DLQ messages include headers:
- `x-death-reason` - Error message
- `x-death-time` - Timestamp of final failure
- `x-retry-count` - Number of attempts made
- `x-first-failure-time` - Timestamp of first failure

### Processing DLQ Messages

You can create a separate consumer to process DLQ messages:

```go
func processDLQ(ctx *protobus.Context) {
    // Create a listener for the DLQ
    // Process failed messages, alert, or store for later analysis
}
```

## Error Flow

```
Handler returns error
        │
        ▼
    Is HandledError? ───Yes───► Send error response to client
        │                       (no retry)
        No
        │
        ▼
    Retry count < max? ───Yes───► Requeue with delay
        │                         (increment retry count)
        No
        │
        ▼
    Send to DLQ ───► Send error response to client
```

## Best Practices

1. **Use HandledError for validation errors**
   ```go
   if email == "" {
       return nil, protobus.NewHandledError("email required", "MISSING_EMAIL")
   }
   ```

2. **Let infrastructure errors trigger retries**
   ```go
   result, err := db.Query(query)
   if err != nil {
       return nil, err // Will retry
   }
   ```

3. **Use meaningful error codes**
   ```go
   // Good
   protobus.NewHandledError("user not found", "USER_NOT_FOUND")

   // Bad
   protobus.NewHandledError("not found", "ERROR")
   ```

4. **Handle errors on the client side**
   ```go
   err := proxy.Call(ctx, "Method", data, &result)
   if err != nil {
       if protobus.IsHandledError(err) {
           he, _ := protobus.GetHandledError(err)
           // Handle business error
       } else {
           // Handle system error
       }
   }
   ```
