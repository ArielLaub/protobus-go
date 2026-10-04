# Testing

Two audiences: you, testing services and clients built on protobus-go, and
contributors, running the suites that test protobus-go itself.

## Testing your code: `protobustest`

`github.com/ArielLaub/protobus-go/v2/protobustest` runs protobus without
RabbitMQ. Its `Broker` is an in-memory broker modelling the RabbitMQ behaviour
protobus relies on: exchange routing, publisher confirms and mandatory returns,
prefetch, acknowledgements, message TTL with dead-lettering (so retries and
dead-letter queues work), priority queues and connection loss. Every `Bus`
dialled from one `Broker` shares its exchanges and queues, as processes sharing
a real broker do, so a test can run a service on one bus and its client on
another.

| API | Does |
|---|---|
| `protobustest.NewBroker()` | An empty broker, safe for concurrent use |
| `broker.Dial(t, opts...)` | A `*protobus.Bus` on the broker, closed when the test ends. Uses `protobustest.Config()` and a logger that discards, unless `opts` say otherwise |
| `protobustest.NewBus(t, opts...)` | A bus on a broker of its own |
| `protobustest.Config()` | `DefaultConfig` with reconnection and timeouts shortened for tests (reconnect delay 5 to 50 ms, retrying forever; 30 s processing and call timeouts; 5 s confirm, ready and drain timeouts) |
| `broker.KillConnections()` | Drop every connection, as a broker failure would. Buses reconnect and restore themselves |
| `broker.Restart()` | A broker restart: connections drop, non-durable queues vanish, durable queues keep only persistent messages |
| `broker.QueueDepth(queue)`, `broker.HasQueue(queue)` | Inspect queues |

`t` is the `*testing.T` (or `*testing.B`) of the running test; `Dial` reports
failures through it and registers the cleanup with `t.Cleanup`, so there is
nothing to close by hand.

```go
package calculator_test

import (
	"context"
	"testing"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/calculator/gen/calculator"
	"github.com/ArielLaub/protobus-go/v2/protobustest"
)

type server struct {
	calculator.UnimplementedServiceServer
}

func (server) Divide(_ context.Context, in *calculator.DivideRequest) (*calculator.DivideResponse, error) {
	if in.Divisor == 0 {
		return nil, protobus.NewHandledError("DIVISION_BY_ZERO", "cannot divide by zero")
	}
	return &calculator.DivideResponse{Quotient: in.Dividend / in.Divisor}, nil
}

func TestDivide(t *testing.T) {
	ctx := context.Background()
	broker := protobustest.NewBroker()

	svc, err := calculator.RegisterServiceServer(broker.Dial(t), server{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	client := calculator.NewServiceClient(broker.Dial(t)) // a second "process"

	out, err := client.Divide(ctx, &calculator.DivideRequest{Dividend: 1, Divisor: 4})
	if err != nil || out.Quotient != 0.25 {
		t.Fatalf("got %v, %v", out, err)
	}
	if _, err := client.Divide(ctx, &calculator.DivideRequest{Dividend: 1}); !protobus.IsCode(err, "DIVISION_BY_ZERO") {
		t.Fatalf("want DIVISION_BY_ZERO, got %v", err)
	}
}

func TestSurvivesABrokerFailure(t *testing.T) {
	broker := protobustest.NewBroker()
	reconnected := make(chan struct{}, 1)
	bus := broker.Dial(t, protobus.WithConnectionObserver(func(e protobus.ConnectionEvent) {
		if e.Kind == protobus.EventReconnected {
			select {
			case reconnected <- struct{}{}:
			default:
			}
		}
	}))
	svc, err := calculator.RegisterServiceServer(bus, server{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	broker.KillConnections()
	<-reconnected
	if _, err := calculator.NewServiceClient(bus).Divide(context.Background(),
		&calculator.DivideRequest{Dividend: 1, Divisor: 1}); err != nil {
		t.Fatal(err)
	}
}
```

Retries really wait for their `RetryPolicy.Delay` on the fake broker, as the
TTL does on RabbitMQ, so keep delays short in tests
(`protobus.WithRetry(protobus.RetryPolicy{MaxRetries: 2, Delay: 10 * time.Millisecond})`).

`protobustest` is a model, not RabbitMQ: use it for fast, deterministic tests,
and keep a few integration tests against a real broker for what a model cannot
promise.

## How protobus-go itself is tested

| Suite | Where | Broker | What it covers |
|---|---|---|---|
| Unit | the root package, `pbtypes`, `protoload`, `protobustest`, `internal/...`, `cmd/...` | in-memory fake | the whole library, deterministically |
| Integration | `integration/` | real RabbitMQ 3 and 4 | the behaviours the fake models, checked at the source |
| Cross-language | `crosslang/` | real RabbitMQ 3 | Go against the real TypeScript and Python libraries, both directions |

Plus, in CI: `gofmt`, `go vet`, `staticcheck`, `govulncheck`, fuzzing of the
envelope decoders, a check that every generated file is current, and the
examples run end to end against RabbitMQ.

### Unit tests

The unit tests run against `internal/fakebroker`, the in-memory AMQP 0-9-1
broker that `protobustest` wraps. Besides routing, confirms, prefetch and
dead-lettering, it can inject faults (dial failures, nacked or never-confirmed
publishes, channel exceptions, connection loss) and records every publish and
settlement in order, so tests can assert on sequencing: that a reply is
published before the request is acknowledged, for instance. The suites include
golden vectors produced by the TypeScript reference for the envelopes
(`internal/wire`), parity tests for the TypeScript port's behaviour checklist
(`parity_test.go`), regression tests named after the defects they pin
(`regression_test.go`), and a check that no secret-looking file is tracked.

Every test binary that starts goroutines verifies, with
[goleak](https://github.com/uber-go/goleak), that none is left running when its
tests end, so a leaked consumer or timer fails the build. Tests run under Go's
race detector (`-race`), which fails a test that touches memory from two
goroutines without synchronisation.

```
go test -race -count=1 ./...
```

`-count=1` disables Go's test result cache. Without broker variables (below)
the integration and cross-language suites skip themselves, so this command
needs nothing but Go. CI runs it on Go 1.25 and the latest stable release, and
fuzzes the decoders for 20 seconds each:

```
for f in FuzzDecodeRequest FuzzDecodeResponse FuzzDecodeEvent; do
  go test -run XXX -fuzz "^$f\$" -fuzztime 20s ./internal/wire
done
```

Set `PROTOBUS_TEST_LOG=1` to see the library's debug logs while a test runs.

### Integration tests

`integration/` runs the same scenarios against RabbitMQ: round trips and the
declared topology, unroutable requests, the retry ladder and dead-lettering,
handled errors, streaming and cancellation, event retries, priority queues, a
changed retry delay, reconnection after the broker drops the connection,
calls parked through an outage, many concurrent calls, graceful shutdown,
`StopConsuming`, instance routing, and the heartbeat negotiated from the URL.
Each test gets a fresh virtual host, created and deleted through the
management API, and the suite is leak-checked too.

There is deliberately no default broker: `localhost:5672` is often a
port-forward to a shared cluster, and these tests declare and delete queues.
Point them at one explicitly with both variables:

| Variable | Example |
|---|---|
| `PROTOBUS_TEST_AMQP_URL` | `amqp://guest:guest@127.0.0.1:25672/` |
| `PROTOBUS_TEST_MGMT_URL` | `http://guest:guest@127.0.0.1:25673` |

With neither set, broker tests skip. With them set but the management API
unreachable, they fail rather than skip, so a misconfigured run cannot pass by
skipping everything.

`docker-compose.yml` starts a throwaway RabbitMQ 3 (management image) on
127.0.0.1:25672 (AMQP) and 25673 (management), away from the usual ports:

```
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673
go test -race -count=1 -v ./integration/...
```

`PROTOBUS_AMQP_PORT` and `PROTOBUS_MGMT_PORT` move the published ports. CI runs the
suite against both `rabbitmq:3-management` and `rabbitmq:4-management`. The
compose file starts RabbitMQ 3 by default; RabbitMQ 4 is a profile on ports
26672 and 26673 (`PROTOBUS_AMQP4_PORT`, `PROTOBUS_MGMT4_PORT`):

```
docker compose --profile rmq4 up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:26672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:26673
```

The examples double as a smoke test (CI runs them after the integration
suite):

```
AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ LOG_LEVEL=warn go run ./examples/calculator
AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ LOG_LEVEL=warn go run ./examples/tokenstream
AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ LOG_LEVEL=warn go run ./examples/combat
```

## Cross-language

`crosslang/` checks protobus-go against the real TypeScript and Python
libraries over a real broker, in both directions. It shares one schema,
`crosslang/proto/interop.proto`, and three implementations of the same
participant: `crosslang/gopeer` in Go, `crosslang/peers/ts/peer.js` on the
built TypeScript checkout, and `crosslang/peers/py/peer.py` on the Python one.
Each peer runs as a server (it prints `READY`) or as a client (it prints a
`PASS` or `FAIL` line per check, then `DONE`).

| Test | Does |
|---|---|
| `TestGoClient` | The Go client against a Go, a TypeScript and a Python server |
| `TestPeerClientsAgainstGoServer` | The TypeScript and Python clients against a Go server; each check becomes a Go subtest, and at least 15 must pass |
| `TestMixedReplicasShareOneRetryLadder` | Replicas of one service in Go, TypeScript and Python competing on one queue |

Every client direction runs the same checks: unary calls, priority on a plain
queue, a stream in order, an empty stream, mid-stream handled and unhandled
errors, cancellation reaching the producer, custom types with defaults and
maps, an echo round trip, handled, unhandled and unimplemented errors, call
metadata (actor, message id), instance routing, and events in both directions.

The mixed-replica test is the strongest claim. Every attempt of
`interop.Flaky.fail` fails, so each of nine messages climbs the retry ladder
across replicas of different languages: the `x-retry-count` one port writes is
read by the next, the queue arguments each port declares must be equivalent,
and each message must reach `interop.Flaky.DLQ` exactly once, after exactly
`MaxRetries` retries, with `x-last-error` naming the error class and never its
message. The test fails if the ladder never crossed languages.

### Running it

It needs the broker variables above, Node.js on `PATH`, and the two sibling
checkouts:

| Variable | Default | Must contain |
|---|---|---|
| `PROTOBUS_TS` | `../protobus` (next to this repository) | a built TypeScript protobus: `dist/lib/context.js` |
| `PROTOBUS_PY` | `../protobus-py` | a virtualenv at `venv/` with protobus-py installed |

```
(cd ../protobus && npm ci && npm run build-ts)
(cd ../protobus-py && python -m venv venv && venv/bin/pip install -e .)

docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673
go test -count=1 -v ./crosslang/...
```

A missing or unbuilt peer skips its tests rather than failing them, so check
the output for `SKIP` when you mean to test all three. CI pins the peers to
TypeScript protobus 2.5.0 and protobus-py 2.0.0 by commit and bumps them
deliberately, and runs this suite against RabbitMQ 3 with Node 20 and Python
3.12.

The cross-language checks found a TypeScript enum quirk, recorded in
[Compatibility](compatibility.md#a-typescript-caveat-found-by-the-suite).
