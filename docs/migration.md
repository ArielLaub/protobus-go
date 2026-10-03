# Migrating from protobus-go v1

protobus-go v2 is a rewrite, not an upgrade. v1 (1.4, on the `main` branch)
was a Go library that spoke its own dialect of protobus: JSON envelopes,
`map[string]interface{}` payloads, its own exchange names. v2 is a
wire-compatible port of TypeScript protobus 2.4 and protobus-py 2.0, with
generated, typed Go code. Expect to rewrite the code that touches protobus;
the concepts carry over, the API does not.

## The things that bite

Three changes compile or run without complaint and still go wrong:

1. **`NewHandledError` swapped its arguments.** v1 was
   `NewHandledError(message, code)`; v2 is `NewHandledError(code, message)`.
   Both are strings, so v1 code compiles against v2 and silently sends the
   message as the code and the code as the message. Search for every call and
   swap the arguments.
2. **v1 and v2 do not interoperate on their defaults.** They use different
   exchanges, environment variables and encodings (below), so a v1 client
   cannot reach a v2 service, or the reverse, even on the same broker. Migrate
   a service and all of its callers together, or run the two side by side
   during the move.
3. **Generated code needs `google.golang.org/protobuf`.** v2 generates
   protobuf message types, which import it. Run `go mod tidy` after the first
   `protobus generate`, or the build fails with
   `no required module provides package google.golang.org/protobuf/...`.

## Module and toolchain

| | v1 | v2 |
|---|---|---|
| Module path | `github.com/ArielLaub/protobus-go` | `github.com/ArielLaub/protobus-go/v2` |
| Go | 1.21+ (README) | 1.25+ |
| Install the CLI | `go install github.com/ArielLaub/protobus-go/cmd/protobus@latest` | `go install github.com/ArielLaub/protobus-go/v2/cmd/protobus@latest` |

The `/v2` suffix is Go's rule for a new major version: it is a different
import path, so v1 and v2 can even be imported side by side by one program
while you migrate.

## Wire and topology

| | v1 | v2 (and TypeScript, Python) |
|---|---|---|
| Envelopes and payloads | JSON (`encoding/json`) | protobuf `RequestContainer`, `ResponseContainer`, `EventContainer`; protobuf payloads |
| RPC exchange | `protobus` (`PROTOBUS_EXCHANGE`) | `proto.bus` (`BUS_EXCHANGE_NAME`) |
| Events exchange | `protobus.events` (`PROTOBUS_EVENTS_EXCHANGE`) | `proto.bus.events` (`EVENTS_EXCHANGE_NAME`) |
| Replies | default exchange, straight to the caller's queue | `proto.bus.callback` direct exchange (`CALLBACKS_EXCHANGE_NAME`) |
| Stream cancellation | none | `proto.bus.cancel` fanout exchange (`CANCEL_EXCHANGE_NAME`) |
| Method name in the routing key | as passed to `Call`; handlers registered under their Go name with a lower-cased first letter | the rpc name exactly as the `.proto` declares it |
| Retry | per-message `expiration`, republished under `<key>.retry` | `<Service>.Retry` queue with a TTL, through `<Service>.Retry.Exchange` |
| Dead-letter queue | `<routing key>.DLQ`, one per method | `<Service>.DLQ`, one per service |
| Publisher confirms, `mandatory` requests | no | yes |

The service queue keeps its name (`<Service>`) and its `REQUEST.<Service>.*`
binding, now on `proto.bus`. Queues and exchanges v1 created stay on the
broker until you delete them; drain any v1 `….DLQ` queues first.

See [Compatibility](compatibility.md) for the full v2 topology.

## Configuration

v1 read five variables into a global `Config` (`GetConfig`, `SetConfig`). v2
reads the variables every protobus port reads into a `Config` value per `Bus`
([Configuration](configuration.md)):

| v1 | Default | v2 | Default |
|---|---|---|---|
| `PROTOBUS_EXCHANGE` | `protobus` | `BUS_EXCHANGE_NAME` | `proto.bus` |
| `PROTOBUS_EVENTS_EXCHANGE` | `protobus.events` | `EVENTS_EXCHANGE_NAME` | `proto.bus.events` |
| `PROTOBUS_MESSAGE_TIMEOUT` (ms) | 30 s | `MESSAGE_PROCESSING_TIMEOUT` (ms) | 10 min |
| `PROTOBUS_RPC_TIMEOUT` (ms) | 30 s | `RPC_CALL_TIMEOUT_MS` | 10 min |
| `PROTOBUS_STREAM_IDLE_TIMEOUT` (ms) | 60 s | `STREAM_IDLE_TIMEOUT_MS` | 60 s |
| none | | `CALLBACKS_EXCHANGE_NAME`, `CANCEL_EXCHANGE_NAME`, `DEFAULT_PREFETCH`, `PUBLISH_CONFIRM_TIMEOUT_MS`, `AMQP_HEARTBEAT_SECONDS`, `CONNECTION_READY_TIMEOUT_MS`, `MAX_OUTSTANDING_CONFIRMS`, `STREAM_MAX_BUFFERED_*`, `PROTOBUS_EXPOSE_INTERNAL_ERRORS`, `SHUTDOWN_DRAIN_TIMEOUT_MS`, `LOG_LEVEL` | |

The `PROTOBUS_*` variables are not read by v2: rename them. Note the timeout
defaults grew from 30 seconds to 10 minutes, matching the other ports; set
`RPC_CALL_TIMEOUT_MS` if your callers relied on failing fast.

## Concepts, old and new

| v1 | v2 |
|---|---|
| `NewContext(opts)` + `ctx.Init(url)` | `protobus.Dial(ctx, url, opts...)` returns a `*Bus` |
| `Context.Close()` | `bus.Close()` (immediate) or `bus.Shutdown(ctx)` (graceful) |
| `ContextOptions{ConnectionOptions}`: `MaxReconnectAttempts`, `InitialReconnectDelayMs`, `MaxReconnectDelayMs`, `ReconnectBackoffMultiplier`, `JitterPercent` | `Config.Reconnect`: `MaxRetries`, `InitialDelay`, `MaxDelay`, `Multiplier` (`time.Duration`s); jitter is fixed at up to 30% |
| `Connection.OnEvent(listener)` with `ConnectionEventReconnecting`, `…Reconnected`, `…Disconnected`, `…Error` | `WithConnectionObserver(f)` with `EventDisconnected`, `EventReconnecting`, `EventReconnected`, `EventGaveUp`; `bus.Done()`, `bus.Err()` |
| `IsConnected()`, `IsReconnecting()` | publishes wait for the connection themselves (up to `ConnectionReadyTimeout`) |
| `MessageFactory.Parse(protoSource, service)`, `map[string]interface{}` payloads | generated message structs (`protobus generate`); `protoload` for schemas known only at runtime |
| `BaseService`, `RunnableService`, `RegisterHandlers(s)` (reflection over Go methods) | the generated `XServer` interface; `RegisterXServer(bus, impl, opts...)` returns a `*Service` |
| `MethodHandler(ctx, data, actor, correlationID)` | `Method(ctx, *Request) (*Response, error)`; actor, correlation id and more in `protobus.CallInfoFromContext(ctx)` |
| `Handle(method, handler)` | a hand-written `ServiceDesc` with `bus.Register`, or `bus.RegisterDynamic` |
| `HandleStream` + `StreamingHandler(…, send)` | a `returns (stream T)` rpc: `Method(ctx, *Request, protobus.ServerStream[*T]) error` |
| `ServiceOptions{MaxConcurrent, RetryOptions}` | `WithMaxConcurrent(n)`, `WithRetry(RetryPolicy{...})`, `WithEarlyAck()`, and more |
| `RetryOptions{MaxRetries, RetryDelayMs, MessageTTLMs}` | `RetryPolicy{MaxRetries, Delay, MessageTTL}` |
| `NewServiceProxy(ctx, name)` + `Init()` + `Call(ctx, method, data, &result)` | `NewXClient(bus)` and typed methods; `protobus.NewClient(bus, name)` with `Invoke` or `Call` for the untyped route |
| `OpenStream(...)` + `Recv()` until `io.EOF` | `for msg, err := range client.Method(ctx, in)` |
| `PublishEvent(ctx, eventType, data, topic)` | `bus.PublishEvent(ctx, msg, protobus.WithTopic(topic))`; the type is the message's |
| `SubscribeEvent(pattern)` + an `onEvent` handler | `protobus.Subscribe(ctx, svc.Events(), func(ctx, *Event, protobus.EventInfo) error, protobus.WithTopic(pattern))`, or a standalone `bus.NewEventListener(queue)` |
| `ServiceCluster` | `protobus.Run(ctx, bus, svc1, svc2, ...)` |
| `RunnableService.Run(ctx)`, `SetCleanup(fn)` | `protobus.Run(ctx, bus, svc)`; release resources after it returns |
| `Logger` interface, `SetLogger`, `LogLevel` | `log/slog`: `WithLogger(*slog.Logger)`, `LOG_LEVEL` |
| `RegisterCustomType`, `BigIntType`, `TimestampType` | `pbtypes.Bigint`, `pbtypes.Timestamp`; application custom types with `-custom-type` or `protoload.WithCustomType` ([Code generation](codegen.md)) |
| `HandledError{Message, Code}`, `NewHandledError(message, code)` | `HandledError{Code, Message}`, `NewHandledError(code, message)` |
| `IsHandledError(err)`, `GetHandledError(err)` | `protobus.AsHandled(err)` |
| a remote error | `*protobus.RemoteError{Method, Code, Message}`; `ErrorCode`, `IsCode` |
| `ErrTimeout`, `ErrNotConnected`, `ErrPublishFailed`, … | `ErrRPCTimeout`, `ErrNotReady`, `ErrDisconnected`, `*PublishError`, … ([Errors](errors.md)) |

`HandledError.Error()` also changed: v1 returned `"<message> (code: <code>)"`,
v2 returns the message alone.

## The CLI

| | v1 | v2 |
|---|---|---|
| `protobus generate` | told you to run `protoc` | compiles every schema itself and writes message types and bindings |
| `protobus generate:service NAME` | `NAME` was `package.ServiceName`; wrote a stub and a map-based client | `NAME` is the `.proto` file name (`Calculator` for `Calculator.proto`); writes a runnable `main.go` skeleton |
| `protobus init` | setup text | setup text for v2 |

See [Code generation](codegen.md).

## A v1 service, rewritten

The v1 README's calculator registered `Add` and `Multiply` by reflection and
read `data["a"].(float64)`. In v2 the schema is the contract:

```proto
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc divide(DivideRequest) returns (DivideResponse);
}
message AddRequest { int32 a = 1; int32 b = 2; }
message AddResponse { int32 result = 1; }
message DivideRequest { double dividend = 1; double divisor = 2; }
message DivideResponse { double quotient = 1; }
```

`protobus generate` (then `go mod tidy`) produces `gen/calculator`, and the
service becomes:

```go
package main

import (
	"context"
	"log"
	"os"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/calculator/gen/calculator"
)

type server struct {
	calculator.UnimplementedServiceServer
}

func (server) Add(ctx context.Context, in *calculator.AddRequest) (*calculator.AddResponse, error) {
	if ci, ok := protobus.CallInfoFromContext(ctx); ok {
		log.Printf("add for %q (correlation %s)", ci.Actor, ci.CorrelationID)
	}
	return &calculator.AddResponse{Result: in.A + in.B}, nil
}

func (server) Divide(_ context.Context, in *calculator.DivideRequest) (*calculator.DivideResponse, error) {
	if in.Divisor == 0 {
		// v1: protobus.NewHandledError("cannot divide by zero", "DIVISION_BY_ZERO")
		return nil, protobus.NewHandledError("DIVISION_BY_ZERO", "cannot divide by zero")
	}
	return &calculator.DivideResponse{Quotient: in.Dividend / in.Divisor}, nil
}

func main() {
	ctx := context.Background()
	bus, err := protobus.Dial(ctx, os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatal(err)
	}
	svc, err := calculator.RegisterServiceServer(bus, server{})
	if err != nil {
		log.Fatal(err)
	}
	if err := protobus.Run(ctx, bus, svc); err != nil {
		log.Fatal(err)
	}
}
```

and the client, replacing `ServiceProxy.Call` with a map:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/calculator/gen/calculator"
)

func main() {
	ctx := context.Background()
	bus, err := protobus.Dial(ctx, os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()

	client := calculator.NewServiceClient(bus)
	sum, err := client.Add(ctx, &calculator.AddRequest{A: 5, B: 3}, protobus.WithActor("migration-guide"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("5 + 3 =", sum.Result)
}
```

The same service can now be called from TypeScript and Python, and replicas in
those languages can share its queue. See [Getting started](getting-started.md),
[Services](services.md), [Clients](clients.md), [Events](events.md) and
[Streaming](streaming.md) for the rest of the v2 API.
