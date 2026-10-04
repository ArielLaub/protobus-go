# ProtoBus for Go

**RabbitMQ-native microservices for Go, with Protocol Buffers on the wire.**

[![Go Reference](https://pkg.go.dev/badge/github.com/ArielLaub/protobus-go/v2.svg)](https://pkg.go.dev/github.com/ArielLaub/protobus-go/v2)
[![go](https://img.shields.io/badge/go-%E2%89%A51.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-%E2%89%A53.8-FF6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com)
[![CI](https://github.com/ArielLaub/protobus-go/actions/workflows/ci.yml/badge.svg)](https://github.com/ArielLaub/protobus-go/actions/workflows/ci.yml)

Define a service in a `.proto` file, implement the interface protobus
generates for it, and call it from anywhere on the bus as if it were local.
ProtoBus turns each service into **one durable RabbitMQ queue with N processes
competing for it**, so load balancing, failover, backpressure, retries and
dead-lettering are the broker's job, not your program's.

This is the Go port of [protobus](https://github.com/ArielLaub/protobus)
(TypeScript) and [protobus-py](https://github.com/ArielLaub/protobus-py)
(Python). The three are **wire-compatible**: a Go service serves TypeScript and
Python callers and the other way round, with streaming, events, custom types
and error codes included. See [Compatibility](docs/compatibility.md).

---

## Install

```bash
go get github.com/ArielLaub/protobus-go/v2
```

The CLI generates Go code from your schemas. It needs no `protoc`:

```bash
go install github.com/ArielLaub/protobus-go/v2/cmd/protobus@latest
```

If you already build with `protoc` or `buf`, install the plugin instead and run
it beside `protoc-gen-go` (see [Code generation](docs/codegen.md)):

```bash
go install github.com/ArielLaub/protobus-go/v2/cmd/protoc-gen-go-protobus@latest
```

You also need a RabbitMQ 3.8+ broker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management-alpine
export AMQP_URL=amqp://guest:guest@localhost:5672/
```

---

## Quick start

Four steps to a working RPC. The code is the
[`examples/calculator`](examples/calculator) example, trimmed.

### 1. Describe the service

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
```

The package plus the service name is the service's name on the bus:
`Calculator.Service`. The file is an ordinary protobus schema, shared as-is
with TypeScript and Python. It needs no `go_package` option.

### 2. Generate the Go code

Inside your Go module (`go mod init example.com/app` if you have none):

```bash
protobus generate        # reads ./proto, writes ./gen
go mod tidy              # adds google.golang.org/protobuf, which the generated code imports
```

Each proto package becomes a Go package: `package Calculator` becomes
`example.com/app/gen/calculator`. It holds the message structs plus:

- `ServiceServer`, the interface your implementation satisfies;
- `UnimplementedServiceServer`, a struct to embed in your implementation;
- `RegisterServiceServer`, which puts an implementation on a bus;
- `NewServiceClient`, which returns a typed client.

### 3. Implement and run the service

```go
// services/calculator/main.go
package main

import (
	"context"
	"log"
	"os"

	protobus "github.com/ArielLaub/protobus-go/v2"

	"example.com/app/gen/calculator"
)

// server implements calculator.ServiceServer, the interface protobus
// generated from `service Service` in Calculator.proto.
type server struct {
	// Embedding keeps server compiling when the .proto gains a method;
	// unimplemented methods answer PROTOCOL_ERROR.
	calculator.UnimplementedServiceServer
}

func (s *server) Add(ctx context.Context, in *calculator.AddRequest) (*calculator.AddResponse, error) {
	return &calculator.AddResponse{Result: in.A + in.B}, nil
}

func (s *server) Divide(ctx context.Context, in *calculator.DivideRequest) (*calculator.DivideResponse, error) {
	if in.Divisor == 0 {
		// A HandledError is an answer, not a failure: never retried.
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
	svc, err := calculator.RegisterServiceServer(bus, &server{}, protobus.WithMaxConcurrent(8))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Calculator.Service is up")
	// Run serves until SIGINT/SIGTERM, then drains in-flight work and closes.
	if err := protobus.Run(ctx, bus, svc); err != nil {
		log.Fatal(err)
	}
}
```

Every handler takes a `context.Context` first. It is Go's standard carrier for
cancellation and deadlines: protobus cancels it when the processing timeout
expires or the bus is closed, and passing it on to your database or HTTP calls
makes them stop too.

`protobus generate:service Calculator` writes a skeleton like this one for you.

### 4. Call it

```go
// cmd/client/main.go
package main

import (
	"context"
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
	resp, err := calc.Add(ctx, &calculator.AddRequest{A: 5, B: 3})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("5 + 3 =", resp.Result)
}
```

```
$ go run ./services/calculator &
$ go run ./cmd/client
5 + 3 = 8
```

The context's 10-second deadline bounds the call. Without one, the call is
bounded by `Config.RPCTimeout` (10 minutes by default, as in the other ports).

The full walkthrough adds events, error handling and a unit test:
**[Getting Started](docs/getting-started.md)**.

---

## Why ProtoBus

### RabbitMQ only, on purpose

ProtoBus is built for one broker, so the things a broker is good at stay in the
broker instead of being reimplemented above it:

| Concern | Where it lives |
|---|---|
| Load balancing | competing consumers on one queue |
| Routing | topic exchange bindings (`REQUEST.<Service>.*`) |
| Redelivery on consumer loss | late ack: an unacked delivery returns to the queue |
| Retry delay | the retry queue's `x-message-ttl`, drained by a dead-letter exchange |
| Persistence | durable queues, persistent messages |
| Dead letters | a real `<Service>.DLQ` |
| Priority | native queue priorities |

A request goes publisher → exchange → queue → consumer. Nothing tracks live
instances, so nothing holds a stale one, and a consumer that dies mid-request
leaves its delivery unacked for the next consumer to take.

If you may need to swap RabbitMQ for another broker, use a transport-agnostic
framework instead. That is a real feature and protobus does not have it.

### Protocol Buffers, not JSON

- **Contract-first.** The `.proto` file is the interface between teams and
  languages, and the generated Go types fail the build when the two drift
  apart.
- **Versioning by field number.** Adding a field does not break an old peer.
- **Usually smaller on the wire**, since fields travel as numbers and packed
  binary rather than text.

### Few dependencies

[`amqp091-go`](https://github.com/rabbitmq/amqp091-go) for AMQP,
[`protobuf`](https://pkg.go.dev/google.golang.org/protobuf) for the wire, and
[`protocompile`](https://github.com/bufbuild/protocompile) to compile schemas
without `protoc`.

---

## Features

- **Retries and dead-lettering.** An unhandled error, a panic or a processing
  timeout parks the request on `<Service>.Retry` and redelivers it; after
  `MaxRetries` (3 by default, 5 seconds apart) it lands on `<Service>.DLQ`.
  A `HandledError` is an answer and is never retried. See
  [Errors](docs/errors.md).
- **Server streaming.** A method declared `returns (stream T)` is handled with
  a `ServerStream[T]` and consumed with a `for ... range` loop. Breaking out
  of the loop, or cancelling the context, stops the producer on the server.
  See [Streaming](docs/streaming.md).
- **Events.** `PublishEvent` publishes on a topic exchange; `Subscribe`
  receives events by message type, by topic pattern (`*`, `#`) or both, with
  optional per-listener retries and a dead-letter queue. See
  [Events](docs/events.md).
- **Custom types.** `bigint` (unsigned 256-bit) and `timestamp` (milliseconds
  since the epoch) are built in and need no import in the schema; they appear
  in Go as `*pbtypes.Bigint` and `*pbtypes.Timestamp`. Declare your own with
  `protoload.WithCustomType`. See [Code generation](docs/codegen.md).
- **Priority.** `WithMaxPriority` makes a service queue a priority queue, and
  `WithPriority` lets a control message overtake a bulk backlog.
- **Early ack.** `WithEarlyAck` trades retries for at-most-once delivery.
- **Processing timeout.** `Config.ProcessingTimeout` or
  `WithProcessingTimeout` caps one attempt at a unary request; the handler's
  context is cancelled and the attempt counts as failed (and is retried).
- **Reconnection.** A lost connection is re-established with capped
  exponential backoff and every service and listener restored on it.
  Publishes issued meanwhile wait for it; calls in flight when it dropped fail
  with `ErrDisconnected`. `WithConnectionObserver` reports each step.
- **Publisher confirms.** Every publish waits for the broker's confirm. A
  failure is a `*PublishError` that says whether the outcome is ambiguous, and
  `WithMessageID` makes a republish safe to deduplicate.
- **Interceptors.** `WithUnaryInterceptor` and `WithStreamInterceptor` wrap
  every method of a service, for logging, metrics, tracing or authorisation.
- **Dynamic API.** `protoload` compiles `.proto` files at runtime;
  `RegisterDynamic`, `Client.Call` and `Client.CallStream` serve and call them
  with no generated code, for gateways and tools.
- **`protobustest`.** An in-memory broker for fast, deterministic unit tests,
  with retries, dead-letter queues, priorities and connection loss modelled.
  See [Testing](docs/testing.md).
- **Graceful shutdown.** `protobus.Run` stops intake on SIGINT/SIGTERM, lets
  in-flight work finish within `Config.ShutdownDrainTimeout`, then closes.

### A short tour

```go
// Options on a service: concurrency, retries, a processing timeout and a
// priority queue.
svc, err := calculator.RegisterServiceServer(bus, impl,
	protobus.WithMaxConcurrent(16),
	protobus.WithRetry(protobus.RetryPolicy{MaxRetries: 5, Delay: 10 * time.Second}),
	protobus.WithProcessingTimeout(30*time.Second),
	protobus.WithMaxPriority(protobus.RecommendedMaxPriority),
)

// Options on a call.
_, err = calc.Add(ctx, &calculator.AddRequest{A: 1, B: 2},
	protobus.WithPriority(protobus.PriorityHigh),
	protobus.WithTimeout(5*time.Second),
	protobus.WithActor("billing-job"),
)

// Events: subscribe by type, publish from anywhere on the bus.
err = protobus.Subscribe(ctx, svc.Events(),
	func(ctx context.Context, ev *calculator.Calculated, info protobus.EventInfo) error {
		log.Printf("%s at %s", ev.Operation, ev.At.AsTime())
		return nil
	})
err = bus.PublishEvent(ctx, &calculator.Calculated{
	Operation: "add",
	At:        pbtypes.NewTimestamp(time.Now()),
})
```

```go
// Streaming, server side: Send each chunk, return to finish.
func (a *assistant) Generate(ctx context.Context, in *chat.GenerateRequest, stream protobus.ServerStream[*chat.Token]) error {
	for i, word := range []string{"hello", "from", "go"} {
		if err := stream.Send(&chat.Token{Index: int32(i), Text: word}); err != nil {
			return err // the caller has gone: stop producing
		}
	}
	return nil // ends the stream normally
}

// Client side: the generated method returns an iter.Seq2, which a
// `for ... range` loop consumes as (chunk, error) pairs.
for tok, err := range assistant.Generate(ctx, &chat.GenerateRequest{Prompt: "hi"}) {
	if err != nil {
		return err
	}
	fmt.Print(tok.Text, " ")
	if tok.Index == 1 {
		break // tells the server to stop producing
	}
}
```

---

## Concurrency

Each unacknowledged delivery runs on its own goroutine, so handlers run truly
in parallel and must be safe for concurrent use. The number in flight is
bounded by the consumer's prefetch: `WithMaxConcurrent` for a service (default
1, one request at a time), `WithEventConcurrency` for event handling (default
`Config.DefaultPrefetch`, also 1). A streaming handler holds its slot for the
life of its stream. Under `WithEarlyAck` a request is acknowledged as a slot
frees up, so the same bound holds.

A `Bus`, a `Client` and generated clients are safe for concurrent use: create
one bus per process and share it.

---

## Wire compatibility

protobus-go speaks the protobus wire protocol exactly as TypeScript protobus
2.4 and protobus-py 2.0 do: the same exchanges, queues, envelopes, headers and
error codes, and the same environment variables for configuration. Replicas of
one service in different languages can share its queue and climb one retry
ladder together. A cross-language suite runs Go against the other ports' real
libraries over a real broker, in both directions.

The type mapping, the topology and the few deliberate behavioural differences
are in **[Compatibility](docs/compatibility.md)**.

---

## Documentation

Full index: **[docs/](docs/README.md)**

| Start | |
|---|---|
| [Getting Started](docs/getting-started.md) | zero to a service, a client, an event and a test |
| [Services](docs/services.md) | implementing, registering and running services |
| [Clients](docs/clients.md) | calling services: options, timeouts, instances |
| [Events](docs/events.md) | publish/subscribe, topics, event retries |
| [Streaming](docs/streaming.md) | server streams, cancellation, backpressure |

| Look it up | |
|---|---|
| [Configuration](docs/configuration.md) | every `Config` field, environment variable and default |
| [Errors](docs/errors.md) | handled vs unhandled, retries, the DLQ, error codes |
| [Code generation](docs/codegen.md) | the `protobus` CLI, the protoc plugin, custom types |
| [Testing](docs/testing.md) | `protobustest`, integration and cross-language suites |

| Run it | |
|---|---|
| [Security](docs/security.md) | what `actor` does and does not prove, error exposure |
| [Migration](docs/migration.md) | upgrading from protobus-go v1 |
| [Compatibility](docs/compatibility.md) | interoperating with TypeScript and Python |

Reference documentation for every exported identifier is on
[pkg.go.dev](https://pkg.go.dev/github.com/ArielLaub/protobus-go/v2), or
locally with `go doc -all github.com/ArielLaub/protobus-go/v2`.

---

## Examples

```bash
docker compose up -d --wait                       # RabbitMQ on 127.0.0.1:25672
export AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
go run ./examples/calculator                      # RPC, a handled error, an event
go run ./examples/tokenstream                     # streaming, and three ways to cancel it
go run ./examples/combat                          # six service instances fight over the bus
```

- [`examples/calculator`](examples/calculator): the quick start, with an event.
- [`examples/tokenstream`](examples/tokenstream): streams tokens like a
  language model, and shows cancellation stopping the producer, not just the
  reader.
- [`examples/combat`](examples/combat): a battle royale of `Combat.Player`
  instances (`WithInstance`) shooting each other over RPC and coordinating
  through events.

---

## Requirements

- Go 1.25+
- RabbitMQ 3.8+

## Development

```bash
go test ./...                                     # unit suite, no broker needed
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673
go test ./...                                     # adds the integration suites
```

The broker in `docker-compose.yml` deliberately avoids ports 5672 and 15672,
because the suites declare and delete queues. The cross-language suite and
how to run it are described in [Testing](docs/testing.md).

## License

MIT — see [LICENSE](LICENSE).
