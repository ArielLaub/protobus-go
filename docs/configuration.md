# Configuration

A `Bus` (the Go counterpart of the TypeScript and Python `Context`) takes its
settings from one `protobus.Config` value. `Dial` builds it from the
environment unless you hand it one, validates it, and keeps it for the life of
the bus; `bus.Config()` returns the copy in use.

```go
package main

import (
	"context"
	"log"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
)

func main() {
	ctx := context.Background()

	cfg := protobus.ConfigFromEnv() // the environment, over the shared defaults
	cfg.RPCTimeout = 30 * time.Second
	cfg.Reconnect.MaxRetries = 0 // never give up

	bus, err := protobus.Dial(ctx, "amqp://guest:guest@localhost:5672/",
		protobus.WithConfig(cfg),
		protobus.WithConnectionName("billing-api"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()
}
```

Two Go details matter here. `Config` is a plain struct passed by value, so
changing `cfg` after `Dial` has no effect on the bus. And `WithConfig` replaces
the configuration entirely: start from `ConfigFromEnv()` to keep honouring the
environment, or from `DefaultConfig()` to ignore it.

## Config fields

Every field, the environment variable `ConfigFromEnv` reads for it, the
default, and what `Validate` accepts. The defaults are those of the TypeScript
and Python ports, and so are the variable names and units.

| Field | Environment variable | Default | Valid |
|---|---|---|---|
| `BusExchange` | `BUS_EXCHANGE_NAME` | `proto.bus` | non-empty, distinct from the other three |
| `CallbacksExchange` | `CALLBACKS_EXCHANGE_NAME` | `proto.bus.callback` | non-empty, distinct |
| `EventsExchange` | `EVENTS_EXCHANGE_NAME` | `proto.bus.events` | non-empty, distinct |
| `CancelExchange` | `CANCEL_EXCHANGE_NAME` | `proto.bus.cancel` | non-empty, distinct |
| `ProcessingTimeout` | `MESSAGE_PROCESSING_TIMEOUT` (ms) | 10 min | > 0 |
| `RPCTimeout` | `RPC_CALL_TIMEOUT_MS` | 10 min | > 0 |
| `StreamIdleTimeout` | `STREAM_IDLE_TIMEOUT_MS` | 60 s | > 0 |
| `DefaultPrefetch` | `DEFAULT_PREFETCH` | `1` | 1..65535 |
| `PublishConfirmTimeout` | `PUBLISH_CONFIRM_TIMEOUT_MS` | 30 s | > 0 |
| `Heartbeat` | `AMQP_HEARTBEAT_SECONDS` (s) | 30 s | ≥ 0 |
| `ConnectionReadyTimeout` | `CONNECTION_READY_TIMEOUT_MS` | 30 s | > 0 |
| `MaxOutstandingConfirms` | `MAX_OUTSTANDING_CONFIRMS` | `256` | 1..65535 |
| `StreamMaxBufferedChunks` | `STREAM_MAX_BUFFERED_CHUNKS` | `1024` | ≥ 1 |
| `StreamMaxBufferedBytes` | `STREAM_MAX_BUFFERED_BYTES` | 64 MiB (`67108864`) | ≥ 1 |
| `StreamMaxTotalBufferedBytes` | `STREAM_MAX_TOTAL_BUFFERED_BYTES` | 256 MiB (`268435456`) | ≥ 1 |
| `ExposeInternalErrors` | `PROTOBUS_EXPOSE_INTERNAL_ERRORS` | `true` | |
| `ShutdownDrainTimeout` | `SHUTDOWN_DRAIN_TIMEOUT_MS` | 30 s | ≥ 0 |
| `Reconnect.MaxRetries` | none | `10` | ≥ 0; 0 retries forever |
| `Reconnect.InitialDelay` | none | 1 s | > 0 |
| `Reconnect.MaxDelay` | none | 30 s | ≥ `InitialDelay` |
| `Reconnect.Multiplier` | none | `2` | ≥ 1 |

Durations are Go `time.Duration` values (`30 * time.Second`); the environment
gives them in milliseconds, except `AMQP_HEARTBEAT_SECONDS`.

### What each one does

- **Exchange names** are part of the wire protocol: every process on one bus,
  in every language, must use the same four. See
  [Compatibility](compatibility.md#topology) for their types and flags.
- **`ProcessingTimeout`** caps one attempt of a unary request on the service
  side. When it fires, the handler's context is cancelled and the attempt
  fails with `PROCESSING_TIMEOUT`, which goes through the retry ladder like any
  other unhandled failure. Go cannot stop a goroutine from outside, so a
  handler that ignores its context keeps running and keeps counting in
  `InFlight` until it returns. `WithProcessingTimeout` overrides it per
  service. Streaming methods are not subject to it; they are bounded by the
  caller's idle timeout and cancellation.
- **`RPCTimeout`** bounds a unary call only when neither the call's context
  has a deadline nor `WithTimeout` was passed. It covers the wait for the
  broker's confirm and for the reply.
- **`StreamIdleTimeout`** is the longest gap a streaming caller accepts
  between chunks (`ErrStreamTimeout`). `WithIdleTimeout` overrides it per call.
  The stream as a whole has no deadline unless its context sets one.
- **`DefaultPrefetch`** is the default `WithEventConcurrency` of event
  listeners and of a service's own `Events()` listener. It does not affect
  request concurrency: a service handles one request at a time unless you pass
  `WithMaxConcurrent`.
- **`PublishConfirmTimeout`** bounds the wait for the broker to confirm a
  publish. Expiry is an *ambiguous* outcome (`ErrPublishConfirmTimeout`): the
  broker may have stored the message. See [Errors](errors.md#publish-failures).
- **`Heartbeat`** is the AMQP heartbeat interval asked of the broker, which
  bounds how long a dead peer goes unnoticed; the lower of the client's and the
  broker's value is negotiated. A `heartbeat` parameter in the broker URL
  overrides it (`amqp://host/?heartbeat=10`). With the amqp091-go client, a
  `Heartbeat` of zero in `Config` falls back to amqp091-go's own 10-second
  default, while `?heartbeat=0` in the URL accepts the interval the broker
  proposes.
- **`ConnectionReadyTimeout`** bounds how long a publish, or a `Start`, waits
  for a reconnection in progress before failing with `ErrNotReady`.
- **`MaxOutstandingConfirms`** bounds unconfirmed publishes per channel.
  Further publishes wait for a slot.
- **The three stream buffer bounds** limit what a streaming caller holds that
  it has not consumed yet: chunks and bytes per call, and bytes across every
  call on the bus. Crossing one fails that stream with `ErrStreamBackpressure`
  rather than growing memory without limit. A total below the per-call bound
  is allowed; the total then binds first.
- **`ExposeInternalErrors`** decides whether the text of an *unhandled* service
  error is sent back to the caller. A `HandledError` always crosses. See
  [Errors](errors.md#sanitization) and [Security](security.md).
- **`ShutdownDrainTimeout`** bounds how long `Shutdown` (and so `Run`) waits
  for in-flight work. Zero does not wait.
- **`Reconnect`** shapes reconnection; see [Reconnection](#reconnection).

### How the environment is read

`ConfigFromEnv` uses the parsing rules of the other ports, so one set of
variables configures a mixed deployment:

- An integer is trimmed and must be all digits and greater than zero (and fit
  the field). Anything else, including `0`, a sign or a unit suffix, is
  ignored and the default kept, rather than becoming a surprising zero. One
  consequence: zero-valued settings such as `ShutdownDrainTimeout = 0` can
  only be set in code.
- A boolean is one of `1`, `true`, `yes`, `on` or `0`, `false`, `no`, `off`,
  case-insensitive. Anything else keeps the default.
- A string replaces the default when it is non-empty.

The variables are read when `ConfigFromEnv` runs; `Dial` calls it only when no
`WithConfig` is given.

### Validation

`Dial` calls `cfg.Validate()` and refuses to connect if it fails. Every problem
is reported at once, joined into one error (`errors.Join`), each line starting
`protobus: invalid config:`. You can call `Validate` yourself, for instance in
a test of your configuration code.

A value that passes the environment parser can still fail validation: for
example `DEFAULT_PREFETCH=70000` is a valid positive integer but above 65535,
so `Dial` fails rather than silently clamping it.

### Variables other ports read that protobus-go does not

| Variable | Read by | In Go |
|---|---|---|
| `SHUTDOWN_EXIT_GRACE_MS` | TypeScript `RunnableService` | Not read. `Run` returns instead of exiting the process; your `main` decides when to exit. |
| `AMQP_URL` | The TypeScript and Python CLI-generated services | Not read by the library: `Dial` takes the URL as an argument. The skeleton `protobus generate:service` writes reads it (default `amqp://guest:guest@localhost:5672/`), as do the examples. |
| `PROTO_PATH` | The TypeScript CLI-generated service, Python `RunnableService` | Not read. Generated code embeds its schema; runtime loading takes directories explicitly ([`protoload`](codegen.md#loading-schemas-at-runtime)). |

## Dial options

`Dial(ctx, url, opts...)` takes options in Go's "functional options" style:
each `WithX` call returns a value that configures one aspect, and you pass as
many as you need after the URL.

| Option | Effect |
|---|---|
| `WithConfig(cfg)` | Use `cfg` instead of `ConfigFromEnv()`. |
| `WithLogger(l)` | Log through `l`, an `*slog.Logger`. See [Logging](#logging). |
| `WithConnectionObserver(f)` | Call `f` with every connection state change. See [Connection events](#connection-events). |
| `WithConnectionName(name)` | Label the connection in the RabbitMQ management UI. The default is `protobus-go`. |
| `WithRegistry(files, types)` | Resolve services and message types from these registries instead of the global ones generated code fills. For schemas loaded at runtime; see [Code generation](codegen.md#loading-schemas-at-runtime). |

Every connection also reports `product: protobus-go`, `version` and
`platform: Go` as client properties, so an operator can tell which port and
release each connection runs.

`Dial` has its own deadline: the context you pass bounds the TCP connect and
the AMQP handshake (which are additionally capped at 30 seconds, so a broker
that accepts the socket and then stalls cannot hang it).

## Reconnection

The first connection is not retried. A process that cannot reach its broker at
startup fails `Dial` and should be restarted by whatever supervises it, rather
than appear healthy while serving nothing.

Once connected, a lost connection is re-established automatically. Attempt
*n* waits

```
min(InitialDelay × Multiplier^(n-1), MaxDelay) + up to 30% random jitter
```

so with the defaults: about 1 s, 2 s, 4 s, 8 s, 16 s, then 30 s per attempt,
each plus jitter, so that a fleet does not reconnect in lockstep after a broker
restart. After `MaxRetries` consecutive failed attempts the bus gives up for
good; `MaxRetries = 0` retries forever.

While the connection is down:

- calls waiting for a reply fail with `ErrDisconnected` (the request may or
  may not have been processed);
- new publishes wait for the connection, up to `ConnectionReadyTimeout`, then
  fail with `ErrNotReady`.

On reconnection every component (reply queue, publishers, services, event
listeners, the stream-cancel listener) redeclares its topology and resumes
consuming, in the order they were created, before the bus reports itself
ready. A connection that comes up but cannot be restored is dropped and the
attempt counts as failed. A single channel closing on a live connection (a
channel error, say) rebuilds that component alone.

When the bus gives up, `bus.Done()` is closed, `bus.Err()` returns an error
wrapping `ErrNotReady`, and every later publish fails with `ErrNotReady`.
`Run` returns that error, so a service process exits and can be restarted.

### Connection events

`WithConnectionObserver` receives a `ConnectionEvent` for every state change,
in order:

| `Kind` | `String()` | Meaning | Fields set |
|---|---|---|---|
| `EventDisconnected` | `disconnected` | The connection was lost unexpectedly. | `Err`: the cause |
| `EventReconnecting` | `reconnecting` | An attempt is scheduled after `Delay`. | `Attempt`, `Delay` |
| `EventReconnected` | `reconnected` | Connection and topology are back; traffic flows. | `Attempt` |
| `EventGaveUp` | `gave-up` | `MaxRetries` attempts failed; the bus is finished. | `Attempt`, `Err`: the last failure |

The observer runs on the goroutine that supervises the connection, so it must
return quickly: blocking it stalls reconnection. Hand the event off instead,
for example to a buffered channel with a non-blocking send:

```go
package main

import (
	"context"
	"log"

	protobus "github.com/ArielLaub/protobus-go/v2"
)

func main() {
	events := make(chan protobus.ConnectionEvent, 16)
	bus, err := protobus.Dial(context.Background(), "amqp://guest:guest@localhost:5672/",
		protobus.WithConnectionObserver(func(e protobus.ConnectionEvent) {
			select {
			case events <- e:
			default: // nobody is keeping up; drop rather than block
			}
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()

	go func() {
		for e := range events {
			log.Printf("broker connection: %s (attempt %d)", e.Kind, e.Attempt)
		}
	}()
	<-bus.Done()
	log.Println("bus stopped:", bus.Err())
}
```

## Logging

protobus logs through the standard library's `log/slog` (structured logging:
each record is a message plus key/value attributes).

- **Without `WithLogger`**, it uses `slog.Default()`'s handler, so it follows
  whatever your program configured with `slog.SetDefault`. If `LOG_LEVEL` is
  set, records below that level are dropped: `debug`, `info`, `warn` (or
  `warning`), `error`, or `silent` / `off` / `none` for nothing at all. Unset
  or unrecognised, the handler's own level applies (`slog`'s default handler
  shows `info` and above).
- **With `WithLogger(l)`**, records go to `l` and `LOG_LEVEL` is not consulted:
  filter in your handler.

Every record carries `component=protobus`. `bus.Logger()` returns the logger in
use. The other attribute names match the TypeScript port's `LogRecord`, so logs
from a mixed deployment aggregate under one schema:

| Attribute | Content |
|---|---|
| `operation` | What the framework was doing: `connect`, `reconnect`, `publish`, `consume`, `dispatch`, `handle`, `retry`, `reject`, `dead-letter`, `reply`, `event`, `stream`, `cancel`, `shutdown`, … |
| `service`, `method`, `queue`, `exchange`, `routingKey`, `messageType` | Where it happened |
| `correlationId`, `messageId` | Which message |
| `outcome` | `ok`, `confirmed`, `failed`, `timeout`, `retried`, `rejected`, `dropped`, `dead-lettered`, `cancelled` |
| `attempt`, `durationMs`, `sizeBytes` | Numbers |
| `error` | An error summary (see below) |
| `errorName`, `errorCode` | The error's class (`PublishNackedError`, `HandledError`, …) and its protobus or handled code, when it has one |

```go
package main

import (
	"context"
	"log"
	"log/slog"
	"os"

	protobus "github.com/ArielLaub/protobus-go/v2"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	bus, err := protobus.Dial(context.Background(), "amqp://guest:guest@localhost:5672/",
		protobus.WithLogger(logger))
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()
}
```

### What is never logged

- **Message payloads and headers.** Only sizes, types, ids and routing data.
- **Broker credentials.** The URL is logged with its password replaced by
  `***` (`amqp://user:***@host:5672/vhost`); a URL that does not parse is
  logged as `<redacted>`. A failed dial logs the error's class only, because
  the client library's message may quote the URL.
- **The text of unhandled errors that travel.** Wherever an error is recorded
  for something other than the failing service's own diagnosis (retries,
  dead-lettering, reconnection, settlement), `error` holds a summary, the error
  class and code (`TimeoutError[PROCESSING_TIMEOUT]`), never its message. The
  one place the full error is logged is the service's own record of its
  handler failing (`handler failed`, `stream handler failed`,
  `event handler failed`), which is the log you need to debug it.

Publisher-controlled values (ids, routing keys, method and type names) are cut
at 256 bytes so one message cannot flood a log. See [Security](security.md) for
the full list.

## Shutdown, Drain and InFlight

`protobus.Run(ctx, bus, services...)` is the usual way to run a service
process: it starts the services, serves until `ctx` ends or the process gets
SIGINT or SIGTERM, then calls `bus.Shutdown`. A second signal during shutdown
kills the process the default way. It returns `nil` after a clean shutdown,
the drain error if the deadline cut it short, a service's start error, or
`bus.Err()` if the bus gave up on the broker. Release your own resources
(database pools, files) after `Run` returns: by then no handler is running.

`bus.Shutdown(ctx)` is the graceful stop:

1. every service and listener stops taking new work (`StopConsuming`),
   leaving its channels open so work in hand can still reply and acknowledge;
2. in-flight work gets until `ctx` ends or `ShutdownDrainTimeout` passes,
   whichever is sooner;
3. everything is closed. Work still running at the deadline stays
   unacknowledged, and the broker redelivers it to another replica.

It returns an error only when the drain was cut short.

`bus.Close()` is the immediate stop: consumers stop, calls in flight fail with
`ErrClosed`, the connection closes, and unacknowledged deliveries are
redelivered. Use it in tests and in `defer` as a backstop; it is safe after
`Shutdown`.

The building blocks are public for a shutdown sequence of your own:

| Method | Does |
|---|---|
| `svc.StopConsuming(ctx)`, `listener.StopConsuming(ctx)` | Stop intake, keep channels open. Final: a reconnection does not resume it. |
| `bus.Drain(ctx)` | Wait until no delivery is in flight and no handler is running, or `ctx` ends. |
| `bus.InFlight()` | How many deliveries are being handled now, counting a handler that outlived its processing timeout until it returns. |

```go
package main

import (
	"context"
	"log"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
)

// stop is what Shutdown does, with a hook between draining and closing.
func stop(bus *protobus.Bus, svc *protobus.Service, flush func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := svc.StopConsuming(ctx); err != nil {
		log.Println("stop consuming:", err)
	}
	if err := bus.Drain(ctx); err != nil {
		log.Printf("drain cut short with %d deliveries in flight", bus.InFlight())
	}
	flush()
	_ = bus.Close()
}

func main() {}
```
