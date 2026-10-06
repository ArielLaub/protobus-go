# Compatibility with the TypeScript and Python ports

protobus-go speaks the protobus wire protocol exactly as
[protobus](https://github.com/ArielLaub/protobus) (TypeScript, 2.5) and
[protobus-py](https://github.com/ArielLaub/protobus-py) (2.0) do. Services and
clients in all three languages can share one broker, one schema and even one
queue: replicas of a service in different languages compete for its requests
and climb one retry ladder together.

This is tested, not assumed. See [Testing](testing.md#cross-language) for the
suite that runs Go against the other ports' real libraries over a real broker,
in both directions.

## The contract is the `.proto`

A schema is shared verbatim. It needs no `go_package` option and no import for
the built-in custom types: the `protobus` CLI (and `protoload`) add both. Each
proto package becomes a Go package (`package Calculator` → `gen/calculator`).

| Proto | TypeScript | Python | Go |
|---|---|---|---|
| `int64`, `uint64`, … | decimal `string` | `int` | `int64`, `uint64` |
| `bigint` (custom) | `bigint` | `int` | `*pbtypes.Bigint` (`.BigInt()` → `*big.Int`) |
| `timestamp` (custom) | `Date` | aware `datetime` (UTC) | `*pbtypes.Timestamp` (`.AsTime()` → `time.Time`) |
| `bytes` | `Buffer` | `bytes` | `[]byte` |
| enum | value name (`string`) | value name | generated enum type |
| unset scalar | its default | its default | its default (proto3) |

The wire bytes are identical in every case; only the in-language
representation differs.

`bigint` is an unsigned integer up to 2^256-1 carried as exactly 32 big-endian
bytes; `timestamp` is signed milliseconds since the epoch. Both are one-field
embedded messages (`message bigint { bytes value = 1; }`), declared at the root
of the type namespace. Every port refuses a negative or oversized `bigint` and
refuses to decode one wider than 32 bytes.

## Topology

Identical names, flags and arguments; the exchange names come from the same
environment variables.

| | Name | Type / flags |
|---|---|---|
| RPC exchange | `proto.bus` (`BUS_EXCHANGE_NAME`) | topic, durable |
| Reply exchange | `proto.bus.callback` (`CALLBACKS_EXCHANGE_NAME`) | direct, durable |
| Event exchange | `proto.bus.events` (`EVENTS_EXCHANGE_NAME`) | topic, durable |
| Stream-cancel exchange | `proto.bus.cancel` (`CANCEL_EXCHANGE_NAME`) | fanout, durable |
| Service queue | `<Service>` bound `REQUEST.<Service>.*` | durable; args only when configured (`x-message-ttl`, `x-max-priority`) |
| Retry | `<Service>.Retry` (TTL, DLX → `proto.bus`), `<Service>.Retry.Exchange` (topic, `#`), `<Service>.DLQ` | durable |
| Event queue | `<Service>.Events` | durable |
| Event retry (opt-in) | `<Service>.Events.Retry`, `.Events.Retry.Exchange`, `.Events.Redelivery`, `.Events.DLQ` | durable |
| Reply queue | server-named, bound to `proto.bus.callback` under its own name | exclusive, auto-delete |
| Cancel queue | server-named, bound to `proto.bus.cancel` | exclusive, auto-delete |

Queue arguments are compared by RabbitMQ as an integer *class*, so the
different integer widths the three AMQP clients pick for the same TTL are
equivalent. The mixed-replica test declares one service's queues from all
three languages to prove it.

## Messages

- Requests, replies and events travel in the protobus envelopes
  (`RequestContainer`, `ResponseContainer`, `EventContainer`). protobus-go
  encodes them byte-for-byte as TypeScript does (golden vectors in
  `internal/wire`), including the empty fields TypeScript writes explicitly.
- Every publish carries a `messageId` (a UUID unless the caller sets one),
  `contentType: application/octet-stream` and a `correlationId`; requests and
  events are persistent. Unary requests are published `mandatory`.
- Streaming replies carry `x-protobus-seq` (from 0) and `x-protobus-final`;
  the last frame is final, an empty stream is one empty final frame, and a
  failure is the final frame. Cancellation is an empty message on
  `proto.bus.cancel` carrying the stream's correlation id.
- Retry and dead-letter copies carry `x-retry-count`, `x-original-routing-key`,
  `x-first-failure-time`, `x-last-error` (the error's class and code, never an
  unhandled error's message), and on the DLQ `x-original-queue` and
  `x-dlq-time`; they keep `contentType`, `contentEncoding`, `priority`,
  `timestamp`, `type` and `appId`, and drop `expiration` and `userId`. The Go
  port routes and labels them by the routing key the broker delivered, never
  by an incoming `x-original-routing-key` (the retry topology keeps the two
  equal for every port's copies), and drops the `CC` and `BCC` headers.
- Readers accept every encoding peers produce: integer headers of any width or
  as strings, `x-protobus-final` as a boolean, number or text.

## Errors

A service error crosses as `ResponseError{method, message, code}`. In Go a
remote error is a `*protobus.RemoteError`; the codes are shared:
`HANDLED_ERROR` (default for a `HandledError`), `PROTOCOL_ERROR` (a request the
service will not run), `INTERNAL_ERROR` (an unhandled error when
`PROTOBUS_EXPOSE_INTERNAL_ERRORS=false`), `PROCESSING_TIMEOUT`, and any code a
service chooses.

## Where the ports differ

These are deliberate and none changes what is on the wire.

| | Go | TypeScript 2.5 | Python 2.0 |
|---|---|---|---|
| Who declares the core exchanges | every process, for what it publishes to | services only | every process |
| Processing timeout, after the last retry | caller answered: `PROCESSING_TIMEOUT` | caller waits for its own timeout | caller answered: `PROCESSING_TIMEOUT` |
| Settlement publish fails | message requeued after 1 s | left unacknowledged | requeued after 1 s |
| Retry/DLQ copies | `mandatory` | not mandatory | `mandatory` |
| Channel lost on a live connection | component rebuilt | not recovered | listener rebuilt |
| `HandledError` from an event handler with event retry on | dead-lettered | dropped | dropped |
| Streaming call | starts when ranged over; `break` or context cancels | starts at call time | starts at iteration |
| Explicit priority 0 | not sent (amqp091 omits it); equivalent at the broker | sent | sent |
| Logging | `log/slog`, attribute names of TS `LogRecord` | `Logger` / `Log` | `logging` |
| Processing timeout on streams | not applied (bounded by the caller's idle timeout) | covers obtaining the iterator | applied |

### A TypeScript caveat found by the suite

TypeScript protobus before 2.5.0 decoded an enum to its value *name* but
encoded through protobufjs `create()`, which does not convert names: an enum
given by name was written as `0`, so a TypeScript service that returned a
decoded message unchanged turned every enum into its zero value. Fixed in
2.5.0 ([protobus#40](https://github.com/ArielLaub/protobus/issues/40)); the
cross-language suite now sends enums by name and echoes decoded messages
unchanged. Use 2.5.0 or later wherever TypeScript services relay decoded
messages. Go and Python were never affected.
