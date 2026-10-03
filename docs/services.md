# Services

A service is a Go value that implements the server interface generated from a
`.proto` service. Registered on a `*protobus.Bus` and started, it consumes the
durable queue named after the service and answers every request on it. This
page covers its lifecycle, how requests are run and settled, and the knobs that
change either.

The snippets use the calculator example's generated package
(`examples/calculator/gen/calculator`) and import the library as `protobus`:

```go
import protobus "github.com/ArielLaub/protobus-go/v2"
```

Connecting and configuring the bus are covered in
[Getting started](getting-started.md) and [Configuration](configuration.md);
generating the code in [Code generation](codegen.md).

## Registering a service

The generator emits, per service, a server interface (`ServiceServer`), an
`UnimplementedServiceServer` struct and a `Register<Service>Server` function.

```go
type calcServer struct {
	// Embedding (a field with no name) gives calcServer every method of
	// UnimplementedServiceServer. Methods calcServer defines itself win, so
	// it keeps compiling when the .proto grows a method: the new method
	// answers its callers with PROTOCOL_ERROR until it is written.
	calculator.UnimplementedServiceServer
}

func (s *calcServer) Add(ctx context.Context, in *calculator.AddRequest) (*calculator.AddResponse, error) {
	return &calculator.AddResponse{Result: in.A + in.B}, nil
}

func register(bus *protobus.Bus) (*protobus.Service, error) {
	return calculator.RegisterServiceServer(bus, &calcServer{}, protobus.WithMaxConcurrent(8))
}
```

`Register<Service>Server` calls `Bus.Register` with the generated
`ServiceDesc`. Registration checks, without touching the broker, that:

- the implementation satisfies the server interface;
- the service is in the bus's file registry (the global one that generated
  code fills in, or the one passed to `WithRegistry`), otherwise
  `ErrUnknownService`;
- every registered method exists in the contract and is unary or
  server-streaming as registered;
- every option is valid (`WithMaxConcurrent` within 1..65535, a sane
  `RetryPolicy`, `WithMaxPriority` not combined with `WithEarlyAck`, an
  instance name that is one routing-key word).

A failure is returned as an error. Go has no exceptions: a function that can
fail returns an `error` as its last result, and the caller checks it.

| | Value |
|---|---|
| `svc.Contract()` | the fully-qualified name in the `.proto`, `Calculator.Service` |
| `svc.Name()` | the name it runs under: the contract, plus `.<instance>` with `WithInstance` |
| Queue | `Name()`, bound to `proto.bus` under `REQUEST.<Name()>.*` |

`WithInstance("p6")` runs one instance of a contract under its own queue
(`Combat.Player.p6`), so several instances of one service can run side by
side; clients address it with the same option. The request envelope still
names the contract method, which is what the instance validates against.

## Lifecycle

| Call | What it does |
|---|---|
| `svc.Start(ctx)` | Declares the topology and starts consuming. Idempotent; a start that failed can be retried. Waits for the connection if a reconnection is under way. |
| `svc.StopConsuming(ctx)` | Stops taking new requests and events. Channels stay open so work in hand can reply and settle. Final: a reconnection does not resume consumption. |
| `svc.Close()` | Stops at once and closes the service's channels. Running handlers are cancelled (`context.Cause` is `ErrClosed`) and their requests are redelivered by the broker. Idempotent. |
| `bus.Shutdown(ctx)` | Graceful stop for the whole process: `StopConsuming` on every started service and standalone listener, wait for in-flight work, then `Bus.Close`. |
| `protobus.Run(ctx, bus, svcs...)` | `Start` each service, serve until `ctx` ends or SIGINT/SIGTERM, then `Shutdown`. |

`Start` declares, in order: the service queue and its binding, the retry
topology (when retries are on), the service's event queue (when `Events()` was
called), and the process's stream-cancel listener (once per process). If a
later step fails, consumption that already started is stopped again, so a
failed `Start` leaves nothing half-running.

`Run` is the usual `main`:

```go
func main() {
	ctx := context.Background()
	bus, err := protobus.Dial(ctx, "amqp://guest:guest@127.0.0.1:25672/")
	if err != nil {
		log.Fatal(err)
	}
	svc, err := calculator.RegisterServiceServer(bus, &calcServer{})
	if err != nil {
		log.Fatal(err)
	}
	if err := protobus.Run(ctx, bus, svc); err != nil {
		log.Fatal(err)
	}
	// No handler is running any more: close databases and files here.
}
```

`Run` returns nil after a clean shutdown, the drain error if
`Config.ShutdownDrainTimeout` (default 30 s) cut it short, the `Start` error
of a service that could not start (having closed the bus), or `Bus.Err()` if
the bus gave up reconnecting. A second signal during shutdown kills the
process the default way.

Without `Run`, do the same by hand. `defer` schedules a call for when the
surrounding function returns:

```go
func serveUntil(ctx context.Context, bus *protobus.Bus, svc *protobus.Service) error {
	if err := svc.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done() // block until the context is cancelled
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return bus.Shutdown(shutdownCtx)
}
```

`Shutdown` waits for the sooner of its context and
`Config.ShutdownDrainTimeout`. Work still running at the deadline is left
unacknowledged, so the broker redelivers it to another replica.
`Bus.Drain(ctx)` and `Bus.InFlight()` expose the wait on its own.

## Concurrency

Each delivery runs on its own goroutine (a lightweight thread managed by the Go
runtime). How many run at once is bounded by `WithMaxConcurrent(n)`, which is
the consumer's AMQP prefetch: the broker hands the service at most `n`
unacknowledged requests.

- The default is **1**: a service handles one request at a time unless told
  otherwise.
- With `n > 1`, handler methods run in parallel on the same value, so shared
  state in the server struct needs a `sync.Mutex` or similar.
- Each delivery is settled on its own (single-tag acks), so a slow request
  never holds up the others.
- A streaming handler holds its slot for the life of its stream, so `n` is
  also how many streams the service serves at once.
- Under `WithEarlyAck` the bound is a counter in the process instead; see
  [Early ack](#early-ack).

Event handlers have their own bound, `WithEventConcurrency`; see
[Events](events.md).

## What a handler's result means

A handler (or the library, on its behalf) ends each attempt in one of these
ways. "Answered" means the caller gets a reply now and the request is
acknowledged; "retried" means it climbs the [retry ladder](#the-retry-ladder).

| Outcome | Treated as | The caller receives |
|---|---|---|
| A response and nil error | answered | the response |
| A `*HandledError` (or an error wrapping one) | answered, never retried | `RemoteError` with its `Code` and `Message` |
| A `*RemoteError` relayed from a downstream call, with a code other than empty, `INTERNAL_ERROR` or `PROCESSING_TIMEOUT` | answered, never retried | the downstream code and message |
| `ErrUnimplemented` (from the embedded Unimplemented server) | answered | `PROTOCOL_ERROR`, "invalid service method …" |
| A request payload that does not decode | answered | `PROTOCOL_ERROR` |
| A request the service will not run (see below) | answered | `PROTOCOL_ERROR` |
| Any other error | retried | after the last attempt, the error |
| A panic | retried | after the last attempt, `protobus: handler panicked: <value>` |
| The processing timeout expired | retried | after the last attempt, `PROCESSING_TIMEOUT` |

```go
func (s *calcServer) Divide(ctx context.Context, in *calculator.DivideRequest) (*calculator.DivideResponse, error) {
	if in.Divisor == 0 {
		// An answer, not a failure: delivered at once, never retried.
		return nil, protobus.NewHandledError("DIVISION_BY_ZERO", "cannot divide by zero")
	}
	return &calculator.DivideResponse{Quotient: in.Dividend / in.Divisor}, nil
}
```

`NewHandledError("", msg)` uses the code `HANDLED_ERROR`. A wrapped
`HandledError` (`fmt.Errorf("charging: %w", herr)`; `%w` keeps the original
inside) still crosses with its own code and message, not the wrapping text.

The text of an *unhandled* error reaches the caller only while
`Config.ExposeInternalErrors` is true (the default, as in the other ports).
With it off, the caller sees `INTERNAL_ERROR` / "internal service error" and
the real error stays in the service's log. A `HandledError`'s message always
crosses, and so does the framework's own processing-timeout text.

A panic is recovered, logged with its stack, and treated as an unhandled
error; the service keeps serving.

**Requests the service will not run.** Before decoding the payload, the
service checks the envelope against its contract. Dispatch is bound to the
routing key the broker delivered on, not to the method the body names, so
RabbitMQ topic permissions mean what they say. The request is answered with
`PROTOCOL_ERROR` (never retried: the same bytes would fail every time) when the
envelope does not decode, the routing key belongs to another service, the
body's method contradicts the routing key, or the method is not one of this
contract's.

## The settlement ladder

After the handler finishes, the request is settled. Every path publishes first
and acknowledges second, so the worst a failure in between can cause is a
redelivery, never a lost message.

1. **Answered.** The reply is published to the caller's reply queue (when the
   request has one), then the request is acked. If the reply cannot be
   published, the request is put back on the queue after a 1 s pause
   (`nack` with requeue) and **runs again**.
2. **Unhandled failure, retries left** (`x-retry-count` below `MaxRetries`). A
   copy goes to `<service>.Retry.Exchange` under the request's original
   routing key, then the original is acked. No reply is sent: the caller keeps
   waiting while the request is retried. If the copy cannot be published, the
   request is requeued after the pause.
3. **Unhandled failure, retries exhausted.** The error reply is published (best
   effort: a failure is logged and settlement goes on, since the caller has
   its own timeout), then a copy goes to `<service>.DLQ`, then the original is
   acked. If the DLQ copy cannot be published, the request is requeued after
   the pause.
4. **Unhandled failure, retries disabled** (`MaxRetries: 0`). The error reply
   is published (best effort), then the request is rejected without requeue:
   it is dropped.
5. **Early ack.** The request was acked on arrival; only the reply (or error
   reply) is published, best effort.

Two endings skip settlement:

- **The caller cancelled** (a streaming caller broke off). The request is
  acked, with no reply, no retry and no dead-lettering.
- **The channel is gone** (the connection was lost, or the service was
  closed). Nothing is settled; the broker redelivers the request. On a lost
  connection, running handlers are cancelled with `context.Cause` =
  `ErrDisconnected`, since their work can no longer be acknowledged. Under
  early ack they are left to finish: theirs is the only copy.

Because the ladder can run a handler more than once for one logical request
(a requeue, a retry, a redelivery after a lost connection), make side effects
idempotent. `CallInfo.MessageID` is the same on every one of those attempts;
deduplicate on it.

## The retry ladder

```go
func registerWithRetry(bus *protobus.Bus) (*protobus.Service, error) {
	return calculator.RegisterServiceServer(bus, &calcServer{},
		protobus.WithRetry(protobus.RetryPolicy{
			MaxRetries: 5,
			Delay:      10 * time.Second,
			MessageTTL: time.Hour,
		}))
}
```

| Field | Meaning |
|---|---|
| `MaxRetries` | retries after the first attempt. 0 disables retries and the dead-letter queue. |
| `Delay` | the retry queue's message TTL: how long a failed request waits before its next attempt. Required when `MaxRetries > 0`. |
| `MessageTTL` | optional `x-message-ttl` on the service queue itself: requests waiting longer expire. |

`DefaultRetryPolicy()` is three retries, five seconds apart, as in the other
ports. `WithRetry(protobus.RetryPolicy{})` turns retries off.

### Topology

With retries on (and no early ack), `Start` declares:

| Name | Kind | Arguments |
|---|---|---|
| `<service>` | durable queue, bound to `proto.bus` as `REQUEST.<service>.*` | `x-message-ttl` and `x-max-priority` only when configured |
| `<service>.Retry.Exchange` | durable topic exchange | |
| `<service>.Retry` | durable queue, bound to the retry exchange with `#` | `x-message-ttl` = `Delay`, `x-dead-letter-exchange` = `proto.bus` |
| `<service>.DLQ` | durable queue | |

A failed request is published to the retry exchange under its original routing
key and lands on `<service>.Retry`. When its TTL expires, RabbitMQ
dead-letters it back to `proto.bus` under that same key, which routes it to the
service queue again. After the last retry it is published straight to
`<service>.DLQ` (through the default exchange). Nothing in protobus consumes
the DLQ: it is the durable record for an operator to inspect or replay.

RabbitMQ fixes a queue's arguments when the queue is first declared. Changing
`Delay` later fails `Start` with `ErrRetryQueueMismatch` until the retry queue
is drained and deleted (or the original delay restored). Adding or changing
`MessageTTL` or `WithMaxPriority` on an existing service queue fails the same
way, with RabbitMQ's `PRECONDITION_FAILED`.

### Headers

Retry and DLQ copies keep the request's body, its headers, `messageId`,
`correlationId`, `contentType`, `contentEncoding`, `priority`, `timestamp`,
`type` and `appId`; they are made persistent and lose `expiration` and
`userId`. Retry copies keep `replyTo`, so the final attempt can still answer;
DLQ copies drop it. Both are published `mandatory`. protobus adds:

| Header | On | Value |
|---|---|---|
| `x-retry-count` | retry, DLQ | retry copies: the attempt number they are going to (1, 2, …); the DLQ copy: the count it arrived with |
| `x-original-routing-key` | retry, DLQ | the request's routing key on its first delivery |
| `x-first-failure-time` | retry, DLQ | Unix milliseconds of the first failure, carried forward unchanged |
| `x-last-error` | retry, DLQ | the last error's class and code, such as `Error`, `TimeoutError[PROCESSING_TIMEOUT]` or `HandledError[CODE]: message`; never an unhandled error's message, which often quotes the data that caused it |
| `x-original-queue` | DLQ | the service queue |
| `x-dlq-time` | DLQ | Unix milliseconds when it was dead-lettered |

Inside a handler, `CallInfo.Attempt` is the incoming `x-retry-count` (0 on the
first delivery).

## Early ack

`WithEarlyAck()` acknowledges each request when its handler starts instead of
after its reply: at-most-once delivery.

- No retry topology is declared and nothing is retried or dead-lettered; a
  failure is still reported to the caller.
- If the ack itself fails, the handler is not run: the broker will redeliver
  the message, and running it here as well would break at-most-once.
- A reply that cannot be published is logged; the request cannot be
  requeued, having been acked.
- A request is acked only once a handler slot is free, so at most
  `WithMaxConcurrent` handlers run, with up to as many again waiting
  unacknowledged in the process.
- It cannot be combined with `WithMaxPriority` (`ErrInvalidPriority` at
  registration).

## Processing timeout

Each attempt of a unary method is bounded by `Config.ProcessingTimeout`
(`MESSAGE_PROCESSING_TIMEOUT`, default 600 s), or by
`WithProcessingTimeout(d)` for one service. When it expires:

1. the handler's context is cancelled, with the timeout as its
   `context.Cause`;
2. the attempt fails as an unhandled error and climbs the retry ladder;
3. after the last attempt the caller receives `PROCESSING_TIMEOUT`.

Go cannot stop a goroutine from outside, so a handler that ignores its context
keeps running after it has been abandoned. It still counts in
`Bus.InFlight()`, and `Drain` and `Shutdown` wait for it, but its retry may
start while it is still running. Pass `ctx` to everything a handler calls
(database drivers, HTTP clients, downstream protobus calls) so the abandoned
attempt actually stops.

Streaming methods are not subject to the processing timeout; a stream is
bounded by its caller's idle timeout and cancellation (see
[Streaming](streaming.md)). Event handlers use `Config.ProcessingTimeout`.

## Priority

`WithMaxPriority(n)` declares the service queue with `x-max-priority` = `n`
(1..255). Callers then pass `protobus.WithPriority(p)` (see
[Clients](clients.md#call-options)); higher values are served first among
the requests waiting in the queue.

```go
func registerPrioritised(bus *protobus.Bus) (*protobus.Service, error) {
	return calculator.RegisterServiceServer(bus, &calcServer{},
		protobus.WithMaxPriority(protobus.RecommendedMaxPriority))
}
```

`RecommendedMaxPriority` is 2, which gives the three shared levels
`PriorityNormal` (0), `PriorityHigh` (1) and `PriorityControl` (2). RabbitMQ
keeps structures per level, so keep `n` small; a priority above it is clamped
by the broker. Priority reorders only what is waiting in the queue, not what
the service has already taken under its prefetch. Retry and DLQ copies keep
their priority. A service's event queue never gets a priority argument.

## Request metadata

`CallInfoFromContext` returns what the library knows about the request a
handler is serving. The second result (`ok`) is false for a context that does
not belong to a request.

```go
func (s *calcServer) whoAsked(ctx context.Context) {
	ci, ok := protobus.CallInfoFromContext(ctx)
	if !ok {
		return
	}
	slog.InfoContext(ctx, "serving", "method", ci.Method, "actor", ci.Actor,
		"messageId", ci.MessageID, "attempt", ci.Attempt)
}
```

| Field | Meaning |
|---|---|
| `Method` | the contract method, `<package>.<Service>.<method>` |
| `Actor` | the caller's `WithActor` value. Nothing authenticates it: treat it as a claim, for tracing and auditing. |
| `CorrelationID` | the call's correlation id |
| `MessageID` | stable across every redelivery, retry and dead-letter hop of the same logical request: deduplicate on it |
| `RoutingKey` | the key the broker delivered the request on |
| `Redelivered` | the broker has delivered this message before |
| `Attempt` | retry hops so far; 0 for the first delivery |
| `Headers` | a copy of the AMQP headers |

## Interceptors

An interceptor wraps every method of a service: logging, metrics, tracing,
authorisation. It receives the decoded request and the next handler, and may
return without calling it.

```go
func logCalls(ctx context.Context, req proto.Message, info *protobus.UnaryServerInfo,
	handler protobus.UnaryHandler) (proto.Message, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	slog.InfoContext(ctx, "call", "method", info.FullMethod,
		"took", time.Since(start), "code", protobus.ErrorCode(err))
	return resp, err
}

func requireActor(ctx context.Context, req proto.Message, info *protobus.UnaryServerInfo,
	handler protobus.UnaryHandler) (proto.Message, error) {
	if ci, _ := protobus.CallInfoFromContext(ctx); ci.Actor == "" {
		return nil, protobus.NewHandledError("UNAUTHENTICATED", "an actor is required")
	}
	return handler(ctx, req)
}

func countStreams(ctx context.Context, req proto.Message, stream protobus.RawServerStream,
	info *protobus.StreamServerInfo, handler protobus.StreamHandler) error {
	slog.InfoContext(ctx, "stream opened", "method", info.FullMethod)
	return handler(ctx, req, stream)
}

func registerIntercepted(bus *protobus.Bus) (*protobus.Service, error) {
	return calculator.RegisterServiceServer(bus, &calcServer{},
		protobus.WithUnaryInterceptor(logCalls, requireActor),
		protobus.WithStreamInterceptor(countStreams))
}
```

- `WithUnaryInterceptor` wraps unary methods, `WithStreamInterceptor`
  server-streaming ones.
- Interceptors run in the order given, the first outermost, including across
  repeated uses of the option.
- They run after the envelope is validated and the payload decoded, and inside
  the processing timeout. A request that fails to decode never reaches them.
- What an interceptor returns is classified like a handler's result, so
  returning a `HandledError` refuses the call without a retry.
- A stream interceptor may pass `handler` a wrapped `RawServerStream`
  (`SendMsg`, `Context`) to observe or alter what is sent.
- The context passed to `handler` is the one the method sees, so an
  interceptor can add values to it.

## Service events

`svc.Events()` returns the service's own event listener, on the durable queue
`<Name()>.Events`. Subscribe to it before or after `Start`; the service starts
and stops it with itself, and one subscribed after `Start` begins consuming on
its first subscription. The queue exists only for a service that calls
`Events()`.

```go
func registerWithEvents(ctx context.Context, bus *protobus.Bus) (*protobus.Service, error) {
	svc, err := calculator.RegisterServiceServer(bus, &calcServer{},
		protobus.WithEventConcurrency(4))
	if err != nil {
		return nil, err
	}
	err = protobus.Subscribe(ctx, svc.Events(),
		func(ctx context.Context, ev *calculator.Calculated, info protobus.EventInfo) error {
			slog.InfoContext(ctx, "calculated", "operation", ev.Operation)
			return nil
		})
	return svc, err
}
```

`WithEventConcurrency` and `WithEventRetry` are accepted by `Register` for the
service's listener, as well as by `NewEventListener`. Everything about
subscriptions, topics and event retries is in [Events](events.md).

## Services without generated code

`Bus.RegisterDynamic` serves a service known only at runtime, typically one
loaded with the `protoload` package and dialled with `WithRegistry`. Requests
arrive as the registry's message types: generated types where they are linked
in, `dynamicpb` messages otherwise.

```go
func serveDynamic(ctx context.Context, url string) (*protobus.Service, error) {
	schema, err := protoload.Load(ctx, []string{"./proto"})
	if err != nil {
		return nil, err
	}
	bus, err := protobus.Dial(ctx, url, protobus.WithRegistry(schema.Files, schema.Types))
	if err != nil {
		return nil, err
	}
	svc, err := bus.RegisterDynamic("Calculator.Service", protobus.DynamicHandlers{
		Unary: map[string]protobus.DynamicHandler{
			"add": func(ctx context.Context, req proto.Message) (proto.Message, error) {
				m := req.ProtoReflect()
				a := m.Get(m.Descriptor().Fields().ByName("a")).Int()
				b := m.Get(m.Descriptor().Fields().ByName("b")).Int()
				out, err := schema.Types.FindMessageByName("Calculator.AddResponse")
				if err != nil {
					return nil, err
				}
				resp := out.New()
				resp.Set(resp.Descriptor().Fields().ByName("result"), protoreflect.ValueOfInt32(int32(a+b)))
				return resp.Interface(), nil
			},
		},
		Streams: map[string]protobus.DynamicStreamHandler{},
	})
	if err != nil {
		return nil, err
	}
	return svc, svc.Start(ctx)
}
```

- Map keys are method names exactly as the `.proto` declares them (`add`, not
  `Add`).
- A declared method with no handler answers `PROTOCOL_ERROR`.
- A handler for a method the contract does not declare, or a streaming method
  registered as unary (or the reverse), fails registration.
- A `DynamicStreamHandler` gets a `send` function with the semantics of
  `ServerStream.Send` (see [Streaming](streaming.md#serverstreamsend)).
- Every service option applies, interceptors included.

`Bus.Register` also accepts a hand-written `ServiceDesc`, which is what both
the generated code and `RegisterDynamic` build.
