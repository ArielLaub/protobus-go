# Security

What protobus-go guarantees, what it leaves to the broker and to you, and how
it lines up with the security fixes made in the TypeScript and Python ports.

The trust model is the one all three ports share. Every process on the bus
holds broker credentials, so a caller is normally another of your own
services. The broker is the authority on who may publish what; protobus-go
makes sure that what a service runs is exactly what the broker routed to it,
and that nothing it records leaks more than it must.

## Dispatch validation

A request names its method twice: in the routing key RabbitMQ delivered it on
(`REQUEST.<Service>.<method>`) and in the `method` field of the envelope in
the body. The body is whatever the publisher wrote; the routing key is what
the broker authorised. Before anything in the payload is interpreted, the
service checks, in order:

1. the envelope decodes (a known field with the wrong wire type is malformed;
   unknown fields are skipped, so a newer peer can add one);
2. the routing key belongs to this service: it starts with
   `REQUEST.<name>.`, where `<name>` is the service's runtime name (with its
   instance, if it has one);
3. the method in the body is the method the routing key names;
4. the body's method is a qualified name (`<package>.<Service>.<method>`)
   whose service is exactly this service's contract, with no extra segments
   and no other loaded service;
5. the contract declares that method;
6. the implementation registered a handler for it, of the right kind (unary
   or server-streaming).

Only then is the payload decoded, with that method's request type. A request
failing any step is answered with `PROTOCOL_ERROR`, logged as
`rejected request` with the reason, acknowledged and never retried: the same
bytes would fail the same way every time. Rejections are labelled with the
method the routing key names, since the body's is the one in dispute.

`Register` enforces the same contract up front: a `ServiceDesc` naming a
method the `.proto` does not declare, or registering a streaming method as
unary or the reverse, is refused, and the service must be in the bus's file
registry (`ErrUnknownService`). There is no reflection over your Go type:
only the methods in the descriptor can be reached.

The result is that a client allowed to publish one method cannot have another
run, and cannot have its payload parsed under a different schema. That makes
RabbitMQ's routing-key permissions meaningful, but only if you configure them.

### Configure topic permissions

RabbitMQ's ordinary permissions (`set_permissions`) match exchange and queue
*names*. Every caller writes to the one `proto.bus` exchange, so write access to
it authorises any `REQUEST.*` key for any service. The control that looks at
routing keys is `set_topic_permissions`, and with none defined (a fresh
installation) every key is allowed:

```
# billing-svc may publish only its own requests and events
rabbitmqctl set_topic_permissions -p /prod billing-svc proto.bus \
    "^REQUEST\.Billing\..*" "^(REQUEST\.Billing|EVENT)\..*"
```

The broker then decides which keys a process may publish, and the service
refuses a body that contradicts its key. Neither replaces the other.

### Replies and events

- **Replies** come back on a server-named, exclusive, auto-delete queue that
  only the calling connection can read, matched to the call by a random
  correlation id. A reply nobody awaits is dropped. A successful reply must
  name the method that was called, or the call fails with
  `ErrInvalidResponse` instead of decoding it against the wrong schema.
- **Events** are matched to handlers by the routing key the broker delivered
  them on, not by the topic in the body, which the publisher controls.
  `EventInfo` carries both. A typed subscription skips events of another type
  that happen to match its topic.

## `actor` is a claim, not an identity

`WithActor` puts a string in the request envelope, and the handler reads it as
`CallInfo.Actor`. Nothing signs or checks it: any process that can publish to
the bus can put any value there. Use it for tracing and audit logs, never to
decide whether an operation is allowed. If a caller could gain from lying
about it, assume it has.

For real authorisation, put the control where it can be enforced: per-service
broker users and vhosts, topic permissions as above, TLS (`amqps://`, which
amqp091-go supports from the URL) anywhere the broker is not on loopback, and,
for end-user identity, a signed token in the request payload that the handler
or an interceptor verifies.

```go
package main

import (
	"context"
	"log/slog"

	"google.golang.org/protobuf/proto"

	protobus "github.com/ArielLaub/protobus-go/v2"
)

// audit records who a call claims to come from. It records the claim; it does
// not, and cannot, verify it.
func audit(ctx context.Context, req proto.Message, info *protobus.UnaryServerInfo,
	handler protobus.UnaryHandler) (proto.Message, error) {
	ci, _ := protobus.CallInfoFromContext(ctx)
	slog.InfoContext(ctx, "call", "method", info.FullMethod, "claimedActor", ci.Actor, "messageId", ci.MessageID)
	return handler(ctx, req)
}

// Pass it when registering: calculator.RegisterServiceServer(bus, srv, auditing)
var auditing = protobus.WithUnaryInterceptor(audit)

func main() {}
```

## Message ids

A caller-supplied id (`WithMessageID`, on calls and events) must be valid
UTF-8, non-blank and at most 255 bytes, the AMQP limit. Anything else fails
with `ErrInvalidMessageID` before anything is sent. A blank id is refused
rather than replaced, because an id derived from a field that came out empty
would silently get a fresh identity per attempt and defeat deduplication.
Without the option every publish gets a random UUID. The id is carried
unchanged through redeliveries, retries and the dead-letter queue.

Like `actor`, an incoming message id is whatever the publisher set; it
identifies a message for deduplication, not its sender.

## What is never logged

- **Payloads and headers.** Library log records carry sizes, type names,
  ids, routing keys and durations only. An event with no handler is logged by
  type and key, not content.
- **Broker credentials.** The broker URL is logged with its password replaced
  by `***`, keeping scheme, user, host, port, vhost and parameters; a URL that
  does not parse as an absolute URL is logged as `<redacted>`, since it may
  still be a credential. A failed dial logs only the error's class, because
  the client library's message can quote the URL.
- **The text of unhandled errors**, anywhere it travels beyond the failing
  service's own report of the failure; see [Error sanitization](#error-sanitization).

Values a publisher controls (correlation and message ids, routing keys, method
and message type names) are cut at 256 bytes, so one message cannot flood a
log, and slog's handlers escape control characters, so a value cannot forge a
line.

Three records contain more, deliberately, and stay in the failing service's
own log:

- `handler failed`, `stream handler failed` and `event handler failed` log the
  handler's full error, because that is the log you debug it from. Your error
  texts end up there: keep secrets out of them.
- `handler panicked` logs the stack, and the panic value only when the Go
  runtime raised it (a nil dereference, an index out of range); any other
  value is logged by type, since `panic(fmt.Sprintf(...))` can quote request
  data.
- `rejected request` gives the reason, clipped like every publisher-controlled
  value: it can quote the method name the request's body supplied.

See [Configuration](configuration.md#logging) for the attribute names and how
to route the logs.

## Error sanitization

Two surfaces, treated differently on purpose.

**The caller.** A `HandledError` always crosses with its code and message:
exposing it is its purpose. A processing timeout crosses as framework text.
The text of any other error crosses only while `ExposeInternalErrors`
(`PROTOBUS_EXPOSE_INTERNAL_ERRORS`) is on, the default in every port, because
the caller is normally one of your own services. Turn it off for a service
whose callers relay errors to untrusted clients: they then receive
`INTERNAL_ERROR`, `internal service error`, while the real error still reaches
the service's own log. A gateway should still map errors to a public
vocabulary deliberately rather than relay whatever it gets.

**Everything that outlives the call.** The `x-last-error` header on retry and
dead-letter copies, which queues retain and dashboards read, and every log
record other than the three above, carry a summary: the error's class and code
(`TimeoutError[PROCESSING_TIMEOUT]`), never an unhandled error's message, which
routinely interpolates the data that caused it. This does not depend on
`ExposeInternalErrors`. A `HandledError` is summarised with its message,
which was meant to be seen.

See [Errors](errors.md#sanitization).

## Size and stream bounds

| Bound | Limit | On crossing |
|---|---|---|
| Unconsumed stream chunks, per call | `StreamMaxBufferedChunks` (1024) | the stream fails with `ErrStreamBackpressure` and its buffer is freed |
| Unconsumed stream bytes, per call | `StreamMaxBufferedBytes` (64 MiB) | same |
| Unconsumed stream bytes, whole bus | `StreamMaxTotalBufferedBytes` (256 MiB) | same |
| Lost stream chunk | gap in `x-protobus-seq` | `ErrStreamSequence`, never a silently short stream; duplicates are dropped |
| Unconfirmed publishes per channel | `MaxOutstandingConfirms` (256) | the publisher waits for a slot |
| Requests handled at once | `WithMaxConcurrent` (1), the consumer prefetch | the broker holds the rest |
| Events handled at once | `WithEventConcurrency` (`DefaultPrefetch`, 1) | the broker holds the rest |
| `bigint` on the wire | 32 bytes | `PROTOCOL_ERROR` before the handler |
| Message id | 255 bytes | `ErrInvalidMessageID` |
| Logged publisher-controlled value | 256 bytes | truncated |

Prefetch applies in every mode, so a backlog is never pulled into memory at
once. The library sets no limit of its own on a message's size: it relies on
the broker's `max_message_size`. The envelope decoders are fuzzed in CI.

## Bigint range checks

`bigint` is unsigned and at most 2^256-1, carried as 32 bytes. Every value
protobus-go encodes or decodes is checked, including `bigint`s nested in
messages, lists and map values:

- **Decoding**: a wire value wider than 32 bytes is refused before it is
  interpreted. In a request this is a `PROTOCOL_ERROR` answered at once,
  before any handler runs and without a retry; in a reply it is
  `ErrInvalidResponse`; in an event, a protocol failure that drops (or
  dead-letters) the event.
- **Encoding**: `pbtypes.NewBigint` refuses a negative or oversized value
  (`ErrBigintRange`) instead of taking its absolute value or truncating it,
  and a message carrying a `Bigint` whose bytes were set by hand to more than
  32 is refused before it is sent (`ErrInvalidRequest`), so protobus never
  publishes what every peer would reject.

Messages whose type cannot contain a `bigint` skip the walk entirely.

## Stream cancellation

A caller that abandons a stream publishes a cancel notice, carrying the
stream's correlation id, on the `proto.bus.cancel` fanout exchange; every
service process hears it and stops the matching handler. The notice is not
authenticated: any process that can publish to that exchange and knows a
correlation id can stop that stream. Correlation ids are random UUIDs, but they
are visible to anyone who can read the service's queue. Restrict write access
to `proto.bus.cancel` if that matters. Cancellation is best effort: a lost
notice means the stream runs to completion.

## The CLI

- `protobus generate:service NAME` accepts only letters, digits, `-` and `_`
  (at most 100), so a name cannot contain separators or `..`; it writes with
  exclusive create and never overwrites a file.
- `protobus generate` refuses to write any file outside the Go module.
- Schema discovery matches files ending in `.proto`, so `schema.proto.bak` or
  `notes.protocol.txt` is never compiled.

## Compared with the TypeScript and Python fixes

| Issue fixed in the other ports | TypeScript | Python | protobus-go v2 |
|---|---|---|---|
| The body chose the method; the routing key was ignored | 1.5 / 2.0 | 1.5 | Dispatch bound to the routing key and the service ([above](#dispatch-validation)) |
| Body method not checked against the contract (inherited members, foreign schemas) | 2.1 | 1.5 | Only declared, registered methods of this contract; no reflection |
| Event handlers matched on the body's topic | 1.5 / 2.0 | | Matched on the delivered routing key |
| Wide `bigint` decoded (quadratic in TS: a CPU denial of service) | 2.1 | 1.5 | Refused over 32 bytes before interpretation |
| Negative or oversized `bigint` silently wrapped | 1.5 / 2.0 | 1.5 | Refused (`ErrBigintRange`) |
| Payloads logged | 1.5 / 2.0 | 2.0 (fixed safe field set) | Never logged |
| Broker URL logged with credentials | 2.0 | 1.5 | Password redacted |
| Raw error text in `x-last-error` | 2.0 | 2.0 | Class and code only |
| Unhandled error text to callers | opt-out `PROTOBUS_EXPOSE_INTERNAL_ERRORS` (2.0) | same (2.0) | Same variable, same default |
| Blank or over-long `messageId` | 2.3 | | Refused (`ErrInvalidMessageID`) |
| Unbounded streaming buffer | 2.0 | 1.5 | Three bounds, `ErrStreamBackpressure` |
| Lost stream chunk looked like a short stream | 2.1 | 1.5 | `ErrStreamSequence` |
| Undecodable payload handed to the handler or retried | | 1.5 | `PROTOCOL_ERROR`, not retried |
| CLI service name could escape the output directory | 2.1 | | Name validated, exclusive create |
| Proto discovery matched `*.proto*` | 1.5 | | Suffix match |
| Secrets in a published artifact | 2.0 (tarball check) | | A test fails if a secret-looking file is tracked; CI runs `govulncheck` |

Versions are those of each port's CHANGELOG entry ("1.5 / 2.0": developed in
1.5.0, first published in 2.0.0).
