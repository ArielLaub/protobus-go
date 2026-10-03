# Getting started

This tutorial builds a small calculator service from an empty directory: a
`.proto` schema, the generated Go code, a service, a client, an event, and a
unit test that needs no broker. It ends where the [examples/calculator](../examples/calculator)
example begins.

It assumes you can read Go but explains the protobus-specific parts, and the
few Go idioms that matter here, as they come up.

## Prerequisites

- **Go 1.25 or newer** (`go version`).
- **A RabbitMQ 3.8+ broker.** The quickest is Docker:

  ```bash
  docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management-alpine
  export AMQP_URL=amqp://guest:guest@localhost:5672/
  ```

  The management UI is then at <http://localhost:15672> (guest / guest). It is
  worth keeping open: you will see the queues protobus declares appear.

You do not need `protoc`. The `protobus` CLI compiles schemas itself.

## 1. Create the project

```bash
mkdir calc && cd calc
go mod init example.com/app
go get github.com/ArielLaub/protobus-go/v2
go install github.com/ArielLaub/protobus-go/v2/cmd/protobus@latest
mkdir proto
```

`go install` puts the `protobus` binary in `$(go env GOPATH)/bin`; make sure
that directory is on your `PATH`. `protobus init` prints these steps if you
need them again.

The layout this tutorial ends with:

```
calc/
├── go.mod
├── proto/Calculator.proto           the contract, shared with other languages
├── gen/calculator/                  generated; never edit
├── services/calculator/main.go      the service
├── services/calculator/main_test.go its tests
└── cmd/client/main.go               a caller
```

## 2. Write the schema

```protobuf
// proto/Calculator.proto
syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc divide(DivideRequest) returns (DivideResponse);
}

message AddRequest {
  int32 a = 1;
  int32 b = 2;
}

message AddResponse {
  int32 result = 1;
}

message DivideRequest {
  double dividend = 1;
  double divisor = 2;
}

message DivideResponse {
  double quotient = 1;
}

// Published after every successful calculation.
message Calculated {
  string operation = 1;
  timestamp at = 2;
}
```

A few things to notice:

- **The service's name on the bus is `<package>.<Service>`**, here
  `Calculator.Service`. That is the name of its RabbitMQ queue, and the name a
  TypeScript or Python caller uses for it.
- **Method names are kept exactly as written.** The wire carries `add`, not
  `Add`; only the Go method is capitalised.
- **`timestamp` is a protobus built-in type**, as is `bigint`. Schemas use
  them like scalars, without an import. In Go they become `*pbtypes.Timestamp`
  and `*pbtypes.Bigint`.
- **No `go_package` option.** The same file is used verbatim by the other
  ports, which do not need one; the CLI derives the Go package from the proto
  package.

## 3. Generate the Go code

```bash
protobus generate
go mod tidy
```

`protobus generate` compiles every `.proto` under `./proto` and writes into
`./gen`. Each proto package becomes one Go package, lower-cased, so
`package Calculator` becomes the import path `example.com/app/gen/calculator`:

```
gen/calculator/Calculator.pb.go            message structs (standard protoc-gen-go output)
gen/calculator/Calculator_protobus.pb.go   protobus bindings
```

`go mod tidy` adds `google.golang.org/protobuf`, which the generated code
imports, to `go.mod`.

The bindings follow the shape gRPC's Go bindings made familiar. For
`service Service` you get:

| Generated | What it is |
|---|---|
| `ServiceServer` | an **interface** with one method per rpc: what you implement |
| `UnimplementedServiceServer` | a struct whose methods all answer "not implemented"; embed it in your implementation |
| `RegisterServiceServer(bus, impl, opts...)` | puts an implementation on a bus, returning a `*protobus.Service` |
| `ServiceClient` / `NewServiceClient(bus, opts...)` | a typed client |
| `Service_ServiceName` | the constant `"Calculator.Service"` |

A Go interface is satisfied implicitly: any type with the right methods *is* a
`ServiceServer`, with no `implements` declaration. `RegisterServiceServer`
takes a `ServiceServer`, so a missing or mistyped method is a compile error.

Rerun `protobus generate` whenever the schema changes. To make that
`go generate ./...`, put this line in any Go file of the module:

```go
//go:generate protobus generate
```

## 4. Implement the service

```go
// services/calculator/main.go

// Command calculator serves Calculator.Service.
package main

import (
	"context"
	"log"
	"os"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"

	"example.com/app/gen/calculator"
)

type server struct {
	calculator.UnimplementedServiceServer
	bus *protobus.Bus // to publish events
}

func (s *server) Add(ctx context.Context, in *calculator.AddRequest) (*calculator.AddResponse, error) {
	s.announce(ctx, "add")
	return &calculator.AddResponse{Result: in.A + in.B}, nil
}

func (s *server) Divide(ctx context.Context, in *calculator.DivideRequest) (*calculator.DivideResponse, error) {
	if in.Divisor == 0 {
		return nil, protobus.NewHandledError("DIVISION_BY_ZERO", "cannot divide by zero")
	}
	s.announce(ctx, "divide")
	return &calculator.DivideResponse{Quotient: in.Dividend / in.Divisor}, nil
}

// announce publishes a Calculated event. A failed publish is logged, not
// returned: the calculation itself succeeded.
func (s *server) announce(ctx context.Context, op string) {
	ev := &calculator.Calculated{Operation: op, At: pbtypes.NewTimestamp(time.Now())}
	if err := s.bus.PublishEvent(ctx, ev); err != nil {
		log.Printf("publishing %s event: %v", op, err)
	}
}

func main() {
	ctx := context.Background()
	bus, err := protobus.Dial(ctx, os.Getenv("AMQP_URL"), protobus.WithConnectionName("calculator"))
	if err != nil {
		log.Fatal(err)
	}

	svc, err := calculator.RegisterServiceServer(bus, &server{bus: bus}, protobus.WithMaxConcurrent(8))
	if err != nil {
		log.Fatal(err)
	}

	err = protobus.Subscribe(ctx, svc.Events(),
		func(ctx context.Context, ev *calculator.Calculated, info protobus.EventInfo) error {
			log.Printf("event %s: %s at %s", info.Type, ev.Operation, ev.At.AsTime().Format(time.RFC3339))
			return nil
		})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Calculator.Service is up; Ctrl-C to stop")
	if err := protobus.Run(ctx, bus, svc); err != nil {
		log.Fatal(err)
	}
}
```

`protobus generate:service Calculator` writes a skeleton of this file, with a
stub for every method, to `services/calculator/main.go`. It never overwrites an
existing file.

Walking through it:

**The handler signature.** Each rpc becomes a method taking a
`context.Context` and the request, and returning the response and an `error`.
Go has no exceptions: a function reports failure by returning a non-nil
`error` as its last result, and the caller checks it. Return `nil, err` to
fail, `resp, nil` to succeed.

**`context.Context`** is Go's standard way to carry cancellation, deadlines and
request-scoped values down a call chain. protobus cancels a handler's context
when its processing timeout expires (`Config.ProcessingTimeout`, 10 minutes by
default) or the bus is closed. Pass `ctx` on to anything slow you call, a
database query or an HTTP request, and it stops too. From the context you can
also read who called and how:

```go
if info, ok := protobus.CallInfoFromContext(ctx); ok {
	slog.InfoContext(ctx, "add", "actor", info.Actor, "attempt", info.Attempt)
}
```

**Embedding `UnimplementedServiceServer`.** Listing a type without a field
name inside a struct *embeds* it: its methods become the outer struct's
methods unless the outer struct defines its own. Your `server` therefore
always satisfies `ServiceServer`, even after someone adds an rpc to the schema
and regenerates; the new method answers callers with `PROTOCOL_ERROR` until
you implement it.

**Two kinds of error.** `protobus.NewHandledError(code, message)` is an
*answer*: the caller receives it at once, with your code and message, and it is
never retried. Use it for validation failures and business rules. Any other
error, and a panic, is treated as an infrastructure failure: protobus parks the
request on the `Calculator.Service.Retry` queue and redelivers it, three times
5 seconds apart by default, then moves it to `Calculator.Service.DLQ` and only
then tells the caller. See [Errors](errors.md).

**Dial, Register, Run.** `protobus.Dial` connects to the broker and returns
the process's `*protobus.Bus`; one per process is enough, and it is safe to
share. `RegisterServiceServer` only prepares the service; nothing touches the
broker until it starts. `protobus.Run` starts it, serves until the process
receives SIGINT or SIGTERM, then shuts down gracefully: it stops taking new
requests, gives in-flight ones up to `Config.ShutdownDrainTimeout` (30 seconds)
to finish, and closes the connection.

**Concurrency.** `WithMaxConcurrent(8)` lets the service handle eight requests
at once, each on its own goroutine (Go's lightweight threads). The default is
one at a time. With more than one, your handlers run in parallel, so any state
they share needs a mutex or similar. Run several copies of the process and they
compete for the same queue; RabbitMQ balances the load between them.

Start it:

```bash
go run ./services/calculator
```

```
2026/10/03 10:00:00 Calculator.Service is up; Ctrl-C to stop
```

In the management UI you will now find the queues `Calculator.Service`,
`Calculator.Service.Retry`, `Calculator.Service.DLQ` and
`Calculator.Service.Events`. They are durable: they outlive the process, and
requests sent while no replica is running wait in them.

## 5. Call it

```go
// cmd/client/main.go

// Command client calls Calculator.Service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"

	"example.com/app/gen/calculator"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bus, err := protobus.Dial(ctx, os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()

	calc := calculator.NewServiceClient(bus)

	sum, err := calc.Add(ctx, &calculator.AddRequest{A: 20, B: 22})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("20 + 22 =", sum.Result)

	q, err := calc.Divide(ctx, &calculator.DivideRequest{Dividend: 1, Divisor: 4},
		protobus.WithActor("getting-started"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("1 / 4 =", q.Quotient)

	_, err = calc.Divide(ctx, &calculator.DivideRequest{Dividend: 1, Divisor: 0})
	var remote *protobus.RemoteError
	if errors.As(err, &remote) && remote.Code == "DIVISION_BY_ZERO" {
		fmt.Printf("1 / 0 -> %s (%s)\n", remote.Message, remote.Code)
	} else if err != nil {
		log.Fatal(err)
	}
}
```

```bash
go run ./cmd/client
```

```
20 + 22 = 42
1 / 4 = 0.25
1 / 0 -> cannot divide by zero (DIVISION_BY_ZERO)
```

and in the service's terminal, one line per successful calculation:

```
2026/10/03 10:00:05 event Calculator.Calculated: add at 2026-10-03T07:00:05Z
2026/10/03 10:00:05 event Calculator.Calculated: divide at 2026-10-03T07:00:05Z
```

Notes on the client:

- **`context.WithTimeout`** gives every call made with `ctx` a 10-second
  deadline. `defer cancel()` releases it when `main` returns; `defer` runs a
  call when the surrounding function exits. A call with no deadline is bounded
  by `Config.RPCTimeout` (10 minutes), or by `protobus.WithTimeout(d)` per
  call.
- **Call options** follow the request: `WithActor` records who the call is
  made for (a claim for tracing; nothing verifies it, see
  [Security](security.md)), `WithPriority`, `WithTimeout` and `NoReply` are the
  others. See [Clients](clients.md).
- **`defer bus.Close()`** closes the connection on exit. A long-running
  process would use `bus.Shutdown(ctx)` instead, which lets work in flight
  finish.

### Reading errors

A `HandledError` from the service arrives as a `*protobus.RemoteError`
carrying the same `Code` and `Message`. `errors.As` is Go's way to ask "is this
error, or anything it wraps, of type X?", filling in `remote` if so. For the
common question "does it carry this code?" there is a shortcut:

```go
if protobus.IsCode(err, "DIVISION_BY_ZERO") { ... }
```

Failures that never reached the service are sentinel values you test with
`errors.Is`: `protobus.ErrRPCTimeout` (no reply in time), `ErrDisconnected`
(the connection dropped mid-call; the request may or may not have run),
`ErrNotReady` (no connection to publish on), and `*protobus.PublishError` for
a publish the broker did not confirm. [Errors](errors.md) has the full list.

## 6. Events

The service above already uses events: every successful calculation publishes
a `Calculated` message, and the same service subscribes to it.

- **`bus.PublishEvent(ctx, msg)`** publishes on the events exchange under the
  topic `EVENT.Calculator.Calculated` (the message's full name). It returns
  once the broker has confirmed it. An event nobody subscribes to is not an
  error.
- **`protobus.Subscribe(ctx, listener, handler)`** subscribes by type: the
  handler's parameter type, `*calculator.Calculated`, selects which events it
  receives. `Subscribe` is a generic function (Go's type parameters), which is
  why it is a package function and not a method on the listener.
- **`svc.Events()`** is the service's own listener, consuming the durable
  queue `Calculator.Service.Events`. Every subscribing queue gets its own copy
  of each event; replicas sharing a queue compete for it, so each event is
  handled once per service, not once per process.

A process that is not a service creates a listener of its own:

```go
listener, err := bus.NewEventListener("Audit.Calculations") // a durable, shared queue
// subscribe with protobus.Subscribe, then:
err = listener.Start(ctx)
```

An empty queue name gives the process a private queue that disappears with its
connection. Topic patterns, wildcards, `SubscribeAll` and event retries are in
[Events](events.md).

## 7. Test it without a broker

The `protobustest` package is an in-memory broker that models the RabbitMQ
behaviour protobus relies on, retries and dead-letter queues included. Tests
using it run in milliseconds and need no Docker.

```go
// services/calculator/main_test.go
package main

import (
	"context"
	"testing"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/protobustest"

	"example.com/app/gen/calculator"
)

func TestCalculator(t *testing.T) {
	ctx := context.Background()
	broker := protobustest.NewBroker() // in memory: no RabbitMQ needed

	serverBus := broker.Dial(t) // closed automatically when the test ends
	svc, err := calculator.RegisterServiceServer(serverBus, &server{bus: serverBus})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}

	calc := calculator.NewServiceClient(broker.Dial(t))

	sum, err := calc.Add(ctx, &calculator.AddRequest{A: 5, B: 3})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Result != 8 {
		t.Errorf("5 + 3 = %d, want 8", sum.Result)
	}

	_, err = calc.Divide(ctx, &calculator.DivideRequest{Dividend: 1, Divisor: 0})
	if !protobus.IsCode(err, "DIVISION_BY_ZERO") {
		t.Errorf("1 / 0: got %v, want DIVISION_BY_ZERO", err)
	}
}

func TestCalculatedEvent(t *testing.T) {
	ctx := context.Background()
	broker := protobustest.NewBroker()
	bus := broker.Dial(t)

	listener, err := bus.NewEventListener("") // a private queue, gone with the connection
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	err = protobus.Subscribe(ctx, listener,
		func(ctx context.Context, ev *calculator.Calculated, _ protobus.EventInfo) error {
			got <- ev.Operation
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Start(ctx); err != nil {
		t.Fatal(err)
	}

	srv := &server{bus: bus}
	if _, err := srv.Add(ctx, &calculator.AddRequest{A: 1, B: 1}); err != nil {
		t.Fatal(err)
	}
	if op := <-got; op != "add" {
		t.Errorf("event operation %q, want add", op)
	}
}
```

```bash
go test ./services/...
```

Every `Bus` dialled from one `Broker` shares its queues, as processes sharing
a real broker do, so the client and the service above talk exactly as they
would in production. Here the test calls `svc.Start` itself instead of
`protobus.Run`, which would block until a signal. `broker.KillConnections()`
and `broker.Restart()` simulate broker failures. See [Testing](testing.md).

## 8. Configure it

Every tunable has a default shared with the TypeScript and Python ports, and
`protobus.Dial` reads the same environment variables they do:

```bash
RPC_CALL_TIMEOUT_MS=30000 MESSAGE_PROCESSING_TIMEOUT=60000 go run ./services/calculator
```

In code, start from `ConfigFromEnv` (or `DefaultConfig`), change fields and
pass the result to `Dial`:

```go
cfg := protobus.ConfigFromEnv()
cfg.RPCTimeout = 30 * time.Second
bus, err := protobus.Dial(ctx, url,
	protobus.WithConfig(cfg),
	protobus.WithConnectionName("calculator"),
	protobus.WithLogger(slog.Default()),
)
```

Per-service settings are options on `RegisterServiceServer`:

```go
svc, err := calculator.RegisterServiceServer(bus, impl,
	protobus.WithMaxConcurrent(16),
	protobus.WithRetry(protobus.RetryPolicy{MaxRetries: 5, Delay: 10 * time.Second}),
	protobus.WithProcessingTimeout(30*time.Second),
)
```

RabbitMQ fixes a queue's arguments when it is first declared, so changing the
retry delay of a service that has already run fails at startup with
`ErrRetryQueueMismatch` until the old retry queue is deleted. Every field and
variable is in [Configuration](configuration.md).

## Where next

- [Services](services.md): options, instances, interceptors, early ack,
  priority queues and graceful shutdown.
- [Clients](clients.md): call options, message ids, fire-and-forget calls and
  the dynamic client.
- [Streaming](streaming.md): `returns (stream T)` methods; the
  [examples/tokenstream](../examples/tokenstream) example shows cancellation
  stopping the producer.
- [Events](events.md): topics, wildcards and event retries.
- [Compatibility](compatibility.md): serving TypeScript and Python callers
  from the same schema.
