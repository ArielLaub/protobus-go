# Clients

A client calls the methods of one service. Generated code gives a typed client
per service; `protobus.Client` underneath it can also be used directly, with
generated or runtime-loaded messages. Clients are cheap and safe for concurrent
use: make one and share it between goroutines.

The snippets use the calculator example's generated package
(`examples/calculator/gen/calculator`) and import the library as `protobus`.
Streaming calls are covered in [Streaming](streaming.md).

## Generated clients

```go
func add(ctx context.Context, bus *protobus.Bus) (int32, error) {
	client := calculator.NewServiceClient(bus)
	resp, err := client.Add(ctx, &calculator.AddRequest{A: 20, B: 22})
	if err != nil {
		return 0, err
	}
	return resp.Result, nil
}
```

Every method takes a `context.Context` first. A context carries a deadline and
a cancellation signal down a call chain: when it is cancelled or its deadline
passes, the call stops waiting. In a handler, pass on the handler's own
`ctx`, so a downstream call ends when the request it serves does.

`New<Service>Client(bus, opts...)` accepts `protobus.WithInstance(name)` to
address one named instance of the service (see
[Services](services.md#registering-a-service)).

## Client and Invoke

`protobus.NewClient` makes a client for any service by its fully-qualified
name; `Invoke` calls a method by the name the `.proto` declares (`add`, not
`Add`) and decodes the reply into `out`. This is what the generated methods do.

```go
func addUntyped(ctx context.Context, bus *protobus.Bus) (int32, error) {
	c := protobus.NewClient(bus, "Calculator.Service")
	var out calculator.AddResponse
	if err := c.Invoke(ctx, "add", &calculator.AddRequest{A: 1, B: 2}, &out); err != nil {
		return 0, err
	}
	return out.Result, nil
}
```

`Invoke` publishes the request to `proto.bus` under
`REQUEST.<service>.<method>`, `mandatory` and persistent, and waits for the
reply on the process's reply queue. It checks the reply names the method that
was called (`ErrInvalidResponse` otherwise) and fails with `ErrInvalidRequest`
if `in` cannot be encoded, before anything is sent. An invalid
`WithInstance` name is reported by every call the client makes.

## Call options

Options are passed after the request. Each is a value of a small interface
type, and the compiler only accepts an option where it makes sense: a
unary-only option passed to a streaming call does not compile.

| Option | Unary | Streaming | Effect |
|---|---|---|---|
| `WithActor(actor)` | yes | yes | Records who the call is made on behalf of, in the request envelope. The service sees it as `CallInfo.Actor`. Nothing authenticates it. |
| `WithPriority(p)` | yes | no | Sets the AMQP priority. It reorders only on a service queue declared with `WithMaxPriority`; elsewhere RabbitMQ ignores it. Use `PriorityNormal`, `PriorityHigh`, `PriorityControl`. |
| `WithMessageID(id)` | yes | no | Sets the request's `messageId`, the service's `CallInfo.MessageID`. See [Idempotency](#ambiguous-outcomes-and-idempotency). Also accepted by `PublishEvent`. |
| `WithTimeout(d)` | yes | no | Bounds the whole call: the broker's confirm and the reply. |
| `NoReply()` | yes | no | Fire-and-forget: asks for no reply. |
| `WithIdleTimeout(d)` | no | yes | See [Streaming](streaming.md#idle-timeout). |

```go
func divide(ctx context.Context, client calculator.ServiceClient, orderID string) (*calculator.DivideResponse, error) {
	return client.Divide(ctx, &calculator.DivideRequest{Dividend: 1, Divisor: 4},
		protobus.WithActor("billing"),
		protobus.WithPriority(protobus.PriorityHigh),
		protobus.WithMessageID("divide-"+orderID),
		protobus.WithTimeout(5*time.Second))
}
```

### Timeouts

The call is bounded by `WithTimeout` when given, by the context's deadline when
that is sooner, and by `Config.RPCTimeout` (`RPC_CALL_TIMEOUT_MS`, default
600 s) when neither is set. The bound covers waiting for a connection, the
broker's confirm and the reply.

When it expires, the error wraps both `ErrRPCTimeout` and
`context.DeadlineExceeded`, and its code is `RPC_TIMEOUT`. A context that is
*cancelled* (rather than timing out) ends the call with `context.Canceled`.
Either way the service may still run the request; a reply that arrives later
is dropped.

### Fire-and-forget

With `NoReply()`, the request carries no `replyTo`. The call returns once the
broker has confirmed the request and routed it to a service queue, and leaves
`out` untouched. The service still runs the method and discards the result;
an unhandled failure still climbs its retry ladder and lands in its DLQ.
Fire-and-forget requests are `mandatory` too, so one addressed to a service
with no queue fails at once with `ErrUnroutable` instead of vanishing.

## Errors

A failed call returns one of these. `errors.As` finds an error of a given type
anywhere in a chain of wrapped errors; `errors.Is` finds a given sentinel
value.

| Error | Meaning |
|---|---|
| `*RemoteError` | The service answered with an error. `Method`, `Code`, `Message` are what its reply carried. |
| `*PublishError` | The broker did not positively confirm the request. Its `Err` says how; see below. |
| `ErrRPCTimeout` | No reply in time. |
| `context.Canceled` | The caller's context was cancelled. |
| `ErrDisconnected` | The connection was lost while the call was in flight. The request may or may not have been processed. |
| `ErrNotReady` | No connection within `Config.ConnectionReadyTimeout` (a reconnection was under way), or the bus gave up. Nothing was sent. |
| `ErrClosed` | The bus was closed. |
| `ErrInvalidMessageID` | `WithMessageID` was blank or over 255 bytes. Nothing was sent. |
| `ErrInvalidRequest`, `ErrInvalidResponse` | The request could not be encoded, or the reply could not be decoded or answers another method. |

The codes a `RemoteError` carries are shared with the other ports:

| Code | Sent when |
|---|---|
| `HANDLED_ERROR` | a `HandledError` with no code of its own |
| any service-chosen code | a `HandledError` with that code (`DIVISION_BY_ZERO`) |
| `PROTOCOL_ERROR` | the service would not run the request: an unimplemented method, a payload that did not decode, a misdirected request |
| `INTERNAL_ERROR` | an unhandled error, when the service runs with `PROTOBUS_EXPOSE_INTERNAL_ERRORS=false` |
| `PROCESSING_TIMEOUT` | the last attempt overran the service's processing timeout |
| empty | an unhandled error with the service exposing its text (the default) |

```go
func classify(ctx context.Context, client calculator.ServiceClient) {
	_, err := client.Divide(ctx, &calculator.DivideRequest{Dividend: 1, Divisor: 0})
	var remote *protobus.RemoteError
	var pubErr *protobus.PublishError
	switch {
	case err == nil:
		// success
	case protobus.IsCode(err, "DIVISION_BY_ZERO"):
		fmt.Println("refused:", err)
	case errors.As(err, &remote): // fills remote when err is (or wraps) one
		fmt.Println("service error", remote.Code, remote.Message)
	case errors.Is(err, protobus.ErrRPCTimeout):
		fmt.Println("no reply in time")
	case errors.As(err, &pubErr) && pubErr.Ambiguous():
		fmt.Println("may have been delivered; message id", pubErr.MessageID)
	default:
		fmt.Println("failed:", err)
	}
}
```

`protobus.ErrorCode(err)` returns the code of a remote or handled error, or of
a protobus local failure (`RPC_TIMEOUT`, `UNROUTABLE`, `PUBLISH_NACKED`,
`PUBLISH_CONFIRM_TIMEOUT`, `CHANNEL_CLOSED`, `NOT_READY`), anywhere in the
chain; `IsCode` compares it. `RemoteError.Error()` reads
`protobus: <method>: <code>: <message>`; `Message` holds the service's text
alone. [Errors](errors.md) covers the whole model.

A handler that calls another service and returns its `*RemoteError` unchanged
relays it: the caller receives the downstream code and message, and the
request is not retried, unless the code is empty, `INTERNAL_ERROR` or
`PROCESSING_TIMEOUT`, which count as infrastructure failures and are retried.

## Ambiguous outcomes and idempotency

A `*PublishError` reports a publish the broker did not positively confirm.
`errors.Is(err, protobus.ErrUnroutable)` and the like see through it to `Err`:

| `Err` | Outcome | Republishing |
|---|---|---|
| `ErrPublishNacked` | definite: the broker refused it | safe |
| `ErrUnroutable` | definite: no queue is bound to the routing key | safe (and useless until a service starts) |
| `ErrPublishConfirmTimeout` | **ambiguous**: no confirm within `Config.PublishConfirmTimeout` | may duplicate |
| `ErrChannelClosed` | **ambiguous**: the channel closed before the confirm | may duplicate |
| the context's error | **ambiguous**: the caller's context ended during the confirm wait | may duplicate |

`pubErr.Ambiguous()` reports the last three. When the deadline expires during
the confirm wait, the call returns an `ErrRPCTimeout` that wraps the
`*PublishError`, so `errors.As` still finds it. `ErrRPCTimeout` and
`ErrDisconnected` are ambiguous in the same way: the request may have been
served.

protobus never deduplicates on its own. To make retrying safe, give the
request an identity derived from the work (an order id, never a clock or a
counter) with `WithMessageID`, reuse it on every attempt, and have the service
skip a `CallInfo.MessageID` it has already applied. The id is carried
unchanged across redeliveries and the service's retry and dead-letter hops; it
must be non-blank and at most 255 bytes, or the call fails with
`ErrInvalidMessageID` before anything is sent.

```go
func chargeOnce(ctx context.Context, client calculator.ServiceClient, orderID string) error {
	id := protobus.WithMessageID("charge-" + orderID)
	var err error
	for range 3 {
		_, err = client.Add(ctx, &calculator.AddRequest{A: 1, B: 1}, id)
		var pubErr *protobus.PublishError
		ambiguous := errors.As(err, &pubErr) && pubErr.Ambiguous() ||
			errors.Is(err, protobus.ErrDisconnected) || errors.Is(err, protobus.ErrRPCTimeout)
		if !ambiguous {
			return err // success, or a definite failure
		}
	}
	return err
}
```

Without `WithMessageID` each publish gets a fresh UUID, reported in
`PublishError.MessageID`.

## Calls without generated code

`Bus.ResolveClient(name)` returns a client for a service in the bus's file
registry, typically one loaded at runtime with `protoload` and
`WithRegistry`. `name` may be the contract or a runtime name with an instance:
`"Combat.Player.p6"` resolves to the contract `Combat.Player` and routes to
instance `p6`.

```go
func addDynamic(ctx context.Context, bus *protobus.Bus) (proto.Message, error) {
	c, err := bus.ResolveClient("Calculator.Service")
	if err != nil {
		return nil, err
	}
	req, err := c.NewRequest("add")
	if err != nil {
		return nil, err
	}
	m := req.ProtoReflect()
	m.Set(m.Descriptor().Fields().ByName("a"), protoreflect.ValueOfInt32(20))
	m.Set(m.Descriptor().Fields().ByName("b"), protoreflect.ValueOfInt32(22))
	return c.Call(ctx, "add", req, protobus.WithActor("gateway"))
}
```

- `NewRequest(method)` returns an empty request of the method's input type:
  the generated type when it is linked in, a `dynamicpb` message otherwise.
- `Call` decodes the reply into a new message of the method's output type. It
  refuses a method the contract does not declare, a streaming method, and a
  request of the wrong type (`ErrInvalidRequest`), before sending anything.
- `CallStream` is its streaming counterpart (see
  [Streaming](streaming.md#calls-without-generated-code)).
- A name that matches no service, nor any prefix of it, fails with
  `ErrUnknownService`.

`Call`, `CallStream` and `NewRequest` also work on a client from `NewClient`,
which looks the contract up in the registry on each call. `Invoke` needs no
registry.
