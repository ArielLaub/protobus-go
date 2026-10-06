# Errors

protobus-go reports failures as ordinary Go `error` values. Two standard
library functions do the inspecting, and both see through any wrapping
(`fmt.Errorf("...: %w", err)`) that protobus or your own code adds:

- `errors.Is(err, protobus.ErrUnroutable)` asks "is this, or does it wrap,
  that particular sentinel value?"
- `errors.As(err, &target)` asks "is there an error of this type in the
  chain?" and, if so, stores it in `target` so you can read its fields.

On top of those, every protobus failure that has a machine-readable code can
be read with `protobus.ErrorCode(err)` or tested with
`protobus.IsCode(err, code)`, whatever its Go type.

## Codes

The first group travels on the wire, inside a `ResponseError`, and is shared
with the TypeScript and Python ports. The second names local failures; it
reaches a further caller only when a service relays one (see
[Relaying a downstream error](#relaying-a-downstream-error)).

| Constant | Code | Meaning |
|---|---|---|
| `CodeHandled` | `HANDLED_ERROR` | Default code of a `HandledError` |
| `CodeProtocol` | `PROTOCOL_ERROR` | The service will not run the request: bad envelope, wrong method, payload that does not decode, unimplemented method |
| `CodeInternal` | `INTERNAL_ERROR` | An unhandled error, with its text suppressed (`ExposeInternalErrors` off) |
| `CodeProcessingTimeout` | `PROCESSING_TIMEOUT` | The handler overran its processing timeout on its last attempt |
| `CodeRPCTimeout` | `RPC_TIMEOUT` | No reply in time |
| `CodeNotReady` | `NOT_READY` | No connection to publish on |
| `CodePublishNacked` | `PUBLISH_NACKED` | The broker refused the publish |
| `CodeUnroutable` | `UNROUTABLE` | No queue is bound for the routing key |
| `CodePublishConfirmTimeout` | `PUBLISH_CONFIRM_TIMEOUT` | No broker confirm in time (ambiguous) |
| `CodeChannelClosed` | `CHANNEL_CLOSED` | The channel closed before the confirm (ambiguous) |

A service may use any other code in a `HandledError`
(`"DIVISION_BY_ZERO"`, `"INSUFFICIENT_FUNDS"`); it crosses unchanged.

`ErrorCode(err)` returns the first code found walking the chain: a
`HandledError`'s or `RemoteError`'s `Code`, a `PublishError`'s code, the code of
a protobus sentinel such as `ErrRPCTimeout`, or the result of a `Code() string`
method on an error from another library. It returns `""` when there is none.
`IsCode(err, "")` is always false.

## Error types

### HandledError: a deliberate answer

```go
type HandledError struct {
	Code    string
	Message string
}
```

A service returns a `HandledError` to tell its caller something: a validation
failure, a business rule. It is answered at once, never retried, never
dead-lettered, and its `Message` always reaches the caller, whatever
`ExposeInternalErrors` says.

`protobus.NewHandledError(code, message)` builds one; an empty code becomes
`HANDLED_ERROR`. Note the argument order: code first. (protobus-go v1 took the
message first; see [Migration](migration.md).) `Error()` returns the message
alone.

A wrapped `HandledError` still counts: if a handler returns
`fmt.Errorf("charging %s: %w", id, herr)`, the caller receives `herr`'s own
`Code` and `Message`, not the wrapping text. `protobus.AsHandled(err)` finds one
in a chain:

```go
package main

import (
	"fmt"

	protobus "github.com/ArielLaub/protobus-go/v2"
)

func main() {
	err := fmt.Errorf("charging order 42: %w",
		protobus.NewHandledError("INSUFFICIENT_FUNDS", "balance too low"))
	if h, ok := protobus.AsHandled(err); ok {
		fmt.Println(h.Code, h.Message) // INSUFFICIENT_FUNDS balance too low
	}
}
```

Anything else a handler returns is treated as an infrastructure failure: the
request is retried and, once retries run out, dead-lettered.

### RemoteError: what the service answered

```go
type RemoteError struct {
	Method  string
	Code    string
	Message string
}
```

A failure reported by the remote service arrives at the caller as a
`*RemoteError`, holding exactly what the service's `ResponseError` carried, in
whatever language the service is written. `Message` is the service's text
alone; `Error()` adds the method and code so the error is recognisable in a
log: `protobus: Calculator.Service.divide: DIVISION_BY_ZERO: cannot divide by
zero`. `Code` is empty when the service sent none.

### PublishError: the broker did not confirm

```go
type PublishError struct {
	Err        error
	MessageID  string
	Exchange   string
	RoutingKey string
	Detail     string
}
```

Every publish (a request, an event, a reply) waits for RabbitMQ to confirm it.
When the broker does not confirm positively, the error is a `*PublishError`
whose `Err` is one of the publish sentinels below, or the caller's context
error if the context ended while the confirm was awaited. `MessageID` is the id
the message was published with, `Detail` the broker's or client library's
explanation. `errors.Is(err, protobus.ErrUnroutable)` works on it directly,
because `PublishError` unwraps to `Err`.

`Ambiguous()` reports whether the broker may have stored the message anyway;
see [Publish failures](#publish-failures).

A publish that never left the process is not a `*PublishError`. If the
context ends, or `PublishConfirmTimeout` passes, while the publish waits for
a confirm slot or for the channel's send path, it fails with the context's
error (an RPC deadline as `ErrRPCTimeout`) or a "publish not sent" error, and
is never transmitted afterwards. That outcome is definite. Once the publish
is committed to the transport, the context ending is ambiguous and reported
in a `*PublishError`.

### Sentinels

| Sentinel | Reported when |
|---|---|
| `ErrClosed` | A `Bus`, `Service` or listener is used after `Close`, or a call was in flight when it closed |
| `ErrNotReady` | A publish waited longer than `ConnectionReadyTimeout` for a reconnection, or the bus gave up or was closed. Nothing was published |
| `ErrDisconnected` | The connection was lost while a call waited for its reply. The request may or may not have been processed |
| `ErrRPCTimeout` | No reply in time. Also satisfies `errors.Is(err, context.DeadlineExceeded)` |
| `ErrPublishNacked` | (in a `PublishError`) The broker refused the message. Definite |
| `ErrUnroutable` | (in a `PublishError`) No queue is bound for the key: nothing serves that service. Definite |
| `ErrPublishConfirmTimeout` | (in a `PublishError`) No confirm within `PublishConfirmTimeout`. Ambiguous |
| `ErrChannelClosed` | (in a `PublishError`) The channel closed with the confirm pending. Ambiguous |
| `ErrStreamTimeout` | A streaming call got no chunk within its idle timeout |
| `ErrStreamBackpressure` | A streaming caller crossed a buffer bound; it is not keeping up |
| `ErrStreamSequence` | A stream chunk was lost (a gap in `x-protobus-seq`) |
| `ErrCancelled` | Inside a handler: `context.Cause(ctx)` when the caller abandoned the call. `ServerStream.Send` returns an error wrapping it |
| `ErrStreamFinished` | `Send` called after the stream handler returned |
| `ErrInvalidRequest` | The request (or event) could not be encoded, for example an out-of-range `bigint` |
| `ErrInvalidResponse` | The reply could not be decoded, or answers a different method than the one called |
| `ErrInvalidMessageID` | `WithMessageID` was given a blank id or one over 255 bytes. Nothing was sent |
| `ErrInvalidPriority` | `WithMaxPriority(0)`, or `WithMaxPriority` with `WithEarlyAck` |
| `ErrRetryQueueMismatch` | `Start` found `<Service>.Retry` declared with a different retry delay |
| `ErrUnimplemented` | Returned by generated `Unimplemented…Server` methods; the caller gets `PROTOCOL_ERROR` |
| `ErrUnknownService` | `Register` was given a service that is not in the registry |
| `ErrUnknownEventType` | `SubscribeAll` got an event whose type is not in the registry |

The custom-type range error, `pbtypes.ErrBigintRange`, is wrapped by
`ErrInvalidRequest` and `ErrInvalidResponse` when it is the cause.

## Returning errors from a handler

| The handler returns | The service | The caller gets |
|---|---|---|
| `nil` error | replies with the result | the result |
| a `HandledError` (possibly wrapped) | replies at once, acknowledges, no retry | `*RemoteError` with the handled `Code` and `Message` |
| `protobus.ErrUnimplemented` (an `Unimplemented…Server` method) | replies at once | `*RemoteError`, `PROTOCOL_ERROR`, `invalid service method <name>` |
| any other error, or a panic | retries it (`RetryPolicy`: by default 3 more attempts, 5 s apart), then dead-letters it to `<Service>.DLQ`, then replies | `*RemoteError` with the error's code and text, or `INTERNAL_ERROR` if suppressed ([below](#sanitization)) |
| nothing within the processing timeout | treats it as an unhandled failure, retried the same way | `*RemoteError`, `PROCESSING_TIMEOUT` |

The caller of a retried request waits through the whole ladder before it is
answered: with the defaults that is four attempts and three 5-second retry
delays. Give it a call timeout (`WithTimeout`, a context deadline, or
`RPCTimeout`) that covers that, or it will see `ErrRPCTimeout` first.

The ladder changes with the service's options:

- `WithRetry(protobus.RetryPolicy{})` (no retries): an unhandled failure is
  answered at once and the request dropped, with no dead-letter queue.
- `WithEarlyAck()`: the request is acknowledged on arrival. A failure is still
  answered, but never retried or dead-lettered.

A request the service will not run at all (the envelope does not decode, the
method is not one of the service's, the payload does not decode as the
method's request type, a `bigint` in it is wider than 32 bytes) is answered at
once with `PROTOCOL_ERROR` and never retried: the same bytes would fail the
same way every time.

### Relaying a downstream error

A handler that calls another service and returns that call's error unchanged
relays it. A relayed `*RemoteError` is treated as an answer when its code is a
deliberate one: the caller receives the downstream `Code` and `Message`, at
once, with no retry. A relayed `RemoteError` with no code, or with
`INTERNAL_ERROR` or `PROCESSING_TIMEOUT`, is an infrastructure failure and goes
through the retry ladder like any other.

Any other local failure a handler returns (`ErrRPCTimeout`, a `PublishError`)
is unhandled and retried; once answered, it carries its own code
(`RPC_TIMEOUT`, `UNROUTABLE`, …) when internal errors are exposed.

## What callers see

| Failure | `Invoke` / generated client returns | `ErrorCode` |
|---|---|---|
| Service answered with a `HandledError` | `*RemoteError` | the handled code |
| Service failed, retries exhausted | `*RemoteError` | the error's code, often `""`, or `INTERNAL_ERROR` |
| Service overran its processing timeout | `*RemoteError` | `PROCESSING_TIMEOUT` |
| Request rejected by the service | `*RemoteError` | `PROTOCOL_ERROR` |
| No service bound to the method | `*PublishError` wrapping `ErrUnroutable` | `UNROUTABLE` |
| Broker refused the request | `*PublishError` wrapping `ErrPublishNacked` | `PUBLISH_NACKED` |
| Broker did not confirm in time | `*PublishError` wrapping `ErrPublishConfirmTimeout` | `PUBLISH_CONFIRM_TIMEOUT` |
| Channel closed before the confirm | `*PublishError` wrapping `ErrChannelClosed` | `CHANNEL_CLOSED` |
| No reply within the call timeout or context deadline | error wrapping `ErrRPCTimeout` and `context.DeadlineExceeded` | `RPC_TIMEOUT` |
| Caller's context cancelled | `context.Canceled` | `""` |
| Connection lost while waiting for the reply | `ErrDisconnected` | `""` |
| No connection within `ConnectionReadyTimeout`, or the bus gave up | error wrapping `ErrNotReady` | `NOT_READY` |
| Bus closed | `ErrClosed` | `""` |
| Request could not be encoded | error wrapping `ErrInvalidRequest` | `""` |
| Reply could not be decoded | error wrapping `ErrInvalidResponse` | `""` |
| Invalid `WithMessageID` | error wrapping `ErrInvalidMessageID` | `""` |

With `NoReply()` the call returns once the broker has confirmed the request
and routed it to a service queue, so only the publish rows apply.

A deadline that expires while the call is still waiting for the broker's
confirm returns an error wrapping both `ErrRPCTimeout` and the `PublishError`
(which is then ambiguous): `errors.As` finds the `PublishError` and its
`MessageID`.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/calculator/gen/calculator"
)

func divide(ctx context.Context, client calculator.ServiceClient, a, b float64) (float64, error) {
	out, err := client.Divide(ctx, &calculator.DivideRequest{Dividend: a, Divisor: b})
	var remote *protobus.RemoteError
	switch {
	case err == nil:
		return out.Quotient, nil
	case protobus.IsCode(err, "DIVISION_BY_ZERO"):
		return 0, fmt.Errorf("bad input: %w", err)
	case errors.As(err, &remote):
		return 0, fmt.Errorf("calculator failed (%s): %s", remote.Code, remote.Message)
	case errors.Is(err, protobus.ErrUnroutable):
		return 0, errors.New("no calculator is running")
	case errors.Is(err, protobus.ErrRPCTimeout):
		return 0, errors.New("calculator did not answer in time")
	default:
		return 0, err
	}
}

func main() {
	bus, err := protobus.Dial(context.Background(), "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()
	q, err := divide(context.Background(), calculator.NewServiceClient(bus), 1, 0)
	fmt.Println(q, err)
}
```

## Publish failures

`ErrPublishNacked` and `ErrUnroutable` are *definite*: the message was not
delivered, and publishing it again is safe. `ErrPublishConfirmTimeout`,
`ErrChannelClosed` and a context that ended during the confirm wait are
*ambiguous*: the broker may have stored the message, so publishing again can
deliver it twice. `PublishError.Ambiguous()` tells the two apart.

Before retrying an ambiguous publish, give the message a stable identity with
`WithMessageID`, derived from the work itself (an order id), never from a clock
or a counter. The receiver sees it as `CallInfo.MessageID` or
`EventInfo.MessageID`, unchanged across redeliveries, retries and dead-letter
hops, and can deduplicate on it. The id must be non-blank and at most 255
bytes; anything else fails with `ErrInvalidMessageID` before anything is sent.

```go
package main

import (
	"context"
	"errors"
	"log"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/calculator/gen/calculator"
)

// announce publishes an event, retrying an ambiguous outcome under the same
// message id so subscribers can drop the duplicate.
func announce(ctx context.Context, bus *protobus.Bus, orderID string) error {
	ev := &calculator.Calculated{Operation: "order " + orderID}
	id := protobus.WithMessageID("calculated-" + orderID)
	for attempt := 0; ; attempt++ {
		err := bus.PublishEvent(ctx, ev, id)
		var pe *protobus.PublishError
		if err == nil || !errors.As(err, &pe) || !pe.Ambiguous() || attempt == 2 {
			return err
		}
		time.Sleep(time.Second)
	}
}

func main() {
	bus, err := protobus.Dial(context.Background(), "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()
	if err := announce(context.Background(), bus, "42"); err != nil {
		log.Fatal(err)
	}
}
```

Events are not published `mandatory`, so `PublishEvent` never reports
`ErrUnroutable`: an event no one subscribes to is not an error. Requests are,
fire-and-forget (`NoReply`) included.

## Streaming errors

A streaming call yields at most one error, and it ends the range:

| Yielded error | Cause |
|---|---|
| `*RemoteError` | The service's handler returned an error. It arrives after every chunk sent before it. Mid-stream errors are not retried, because a stream cannot be replayed without the caller seeing chunks twice |
| `ErrStreamTimeout` | No chunk within the idle timeout |
| `ErrStreamBackpressure` | The caller fell behind a buffer bound |
| `ErrStreamSequence` | A chunk was lost |
| `ErrDisconnected` | The connection was lost mid-stream |
| a `PublishError`, `ErrNotReady` | The request was never delivered |
| `ctx.Err()` | The caller's context ended |

Inside the service, a handler whose caller has gone sees
`context.Cause(ctx) == protobus.ErrCancelled`, and `Send` returns an error
wrapping it; stop producing when it does. See [Streaming](streaming.md).

## Event handler errors

On the subscribing side, an event handler's error decides the event's fate:

| Handler returns | Without `WithEventRetry` | With `WithEventRetry` |
|---|---|---|
| `nil` | acknowledged | acknowledged |
| a `HandledError` | dropped | dead-lettered to `<queue>.DLQ` at once |
| any other error | dropped | retried, then dead-lettered |
| (event does not decode) | dropped | dead-lettered at once |

Dropping is the default so that one event that always fails cannot stall a
subscriber. See [Events](events.md).

## Sanitization

What a caller learns about a failure depends on the kind of failure, not on
where it is read:

| Error | Crosses to the caller as |
|---|---|
| `HandledError` (or a relayed coded `RemoteError`) | its `Code` and `Message`, always |
| processing timeout | `PROCESSING_TIMEOUT`, `message <correlation id> exceeded the <limit> processing timeout`: framework text, always |
| any other error, `ExposeInternalErrors` on (default) | `ErrorCode(err)` and `err.Error()` |
| any other error, `ExposeInternalErrors` off | `INTERNAL_ERROR`, `internal service error` |

`ExposeInternalErrors` (`PROTOBUS_EXPOSE_INTERNAL_ERRORS`) defaults to `true`,
as in the other ports: a protobus caller is normally another of your own
services, inside the same trust boundary. Turn it off for a service whose
callers relay errors to untrusted clients, such as one behind a public
gateway. The real error is still logged in full by the service that failed.

Independently of that setting, an unhandled error's *text* never goes
anywhere that outlives the call. The `x-last-error` header on retry and
dead-letter copies, which queues keep and dashboards read, and every log
record other than the failing service's own report of the failure
(`handler failed`, `stream handler failed`, `event handler failed`), carry only
a summary: the error's class and code, `TimeoutError[PROCESSING_TIMEOUT]`,
`PublishNackedError[PUBLISH_NACKED]`, or the Go type name. A `HandledError` is
the exception, summarised as `HandledError[<code>]: <message>`, since its
message was meant to be seen. See [Security](security.md).
