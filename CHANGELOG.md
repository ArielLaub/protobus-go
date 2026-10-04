# Changelog

All notable changes to **protobus-go** are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] — 2026-10-04

protobus-go v2 is a rewrite from scratch, published under a new module path:

```
github.com/ArielLaub/protobus-go/v2
```

It is a wire-compatible port of [protobus](https://github.com/ArielLaub/protobus)
2.4 (TypeScript) and [protobus-py](https://github.com/ArielLaub/protobus-py) 2.0
(Python): the same exchanges, queues, envelopes, headers, error codes and
environment variables, verified by a cross-language suite that runs Go against
the other ports' real libraries over a real broker. The API is new and
idiomatic Go: code generated from the `.proto`, typed messages, `context`
everywhere, and `iter.Seq2` for streams. Nothing from v1 carries over
unchanged; see [Migration](docs/migration.md).

Because the module path changed, v1 and v2 can be imported side by side while
you migrate.

### Added

- **Code generation.** `protobus generate` (the `cmd/protobus` CLI) compiles
  schemas without `protoc` and writes Go messages plus protobus bindings: a
  `<Service>Server` interface, `Unimplemented<Service>Server`,
  `Register<Service>Server` and a typed `<Service>Client`.
  `protobus generate:service` writes a runnable service skeleton.
  `protoc-gen-go-protobus` is the same generator as a `protoc` plugin.
  Schemas are shared verbatim with the other ports: no `go_package` and no
  import for the built-in types.
- **`Bus`**, the process's connection (`Dial`, `Close`, `Shutdown`, `Drain`),
  and **`Run`**, which serves services until SIGINT/SIGTERM and then drains
  in-flight work within `Config.ShutdownDrainTimeout`.
- **Retries and dead-lettering** through `<Service>.Retry` and `<Service>.DLQ`
  (`RetryPolicy`, `WithRetry`; three retries, five seconds apart, by default).
  `HandledError` answers without retrying; unhandled errors, panics and
  processing timeouts are retried.
- **Server streaming** with `ServerStream[T]` on the service side and an
  `iter.Seq2` on the caller's side. Breaking out of the loop, or ending the
  context, cancels the producer through `proto.bus.cancel`. Idle timeouts and
  bounded caller-side buffers (`ErrStreamBackpressure`).
- **Events**: `Bus.PublishEvent`, type-safe `Subscribe[T]`, `SubscribeType`
  and `SubscribeAll`, topic patterns (`WithTopic`), per-service and
  standalone `EventListener`s, and opt-in event retries with a per-listener
  redelivery path and DLQ (`WithEventRetry`).
- **Custom types**: the built-in `bigint` and `timestamp`, as
  `*pbtypes.Bigint` and `*pbtypes.Timestamp`, validated at encode and decode;
  user custom types through `protoload.WithCustomType`.
- **Priority queues** (`WithMaxPriority`, `WithPriority`, and the
  `PriorityNormal` / `PriorityHigh` / `PriorityControl` levels shared with
  the other ports).
- **Early ack** (`WithEarlyAck`) for at-most-once services.
- **Processing timeout** per bus (`Config.ProcessingTimeout`) or per service
  (`WithProcessingTimeout`), cancelling the handler's context.
- **Reconnection** with capped exponential backoff and jitter
  (`Config.Reconnect`), restoring every component's topology, observable with
  `WithConnectionObserver`.
- **Publisher confirms** on every publish, mandatory routing where an
  unroutable message would be lost, and `*PublishError` reporting whether a
  failure is ambiguous. `WithMessageID` makes a republish deduplicable.
- **Interceptors**: `WithUnaryInterceptor` and `WithStreamInterceptor`.
- **Per-call options**: `WithTimeout`, `WithActor`, `WithPriority`,
  `NoReply`, `WithMessageID`, `WithIdleTimeout`.
- **Service instances** (`WithInstance`, `Bus.ResolveClient`) to run several
  instances of one contract side by side.
- **Dynamic API** for schemas with no generated code: `protoload.Load` /
  `protoload.Parse`, `WithRegistry`, `Bus.RegisterDynamic`, `Client.Call`,
  `Client.CallStream` and `StreamWith`.
- **`CallInfo`** (`CallInfoFromContext`) and **`EventInfo`**: actor, message
  id, attempt, routing key and headers of the delivery being handled.
- **`protobustest`**: an in-memory broker modelling routing, confirms,
  prefetch, TTL and dead-lettering, priorities and connection loss, for unit
  tests without RabbitMQ.
- **Configuration** from the environment variables every port reads
  (`ConfigFromEnv`), with the same defaults and parsing rules, validated by
  `Dial`.
- **Structured logging** through `log/slog` (`WithLogger`; `slog.Default`
  filtered by `LOG_LEVEL` otherwise).
- Error codes and helpers: `ErrorCode`, `IsCode`, `AsHandled`, `RemoteError`,
  and sentinel errors for `errors.Is`.

### Changed (breaking, versus v1)

- **Module path** is now `github.com/ArielLaub/protobus-go/v2`.
- **The whole API is new.** `Context`, `ServiceProxy`, `BaseService`,
  `RunnableService`, `ServiceCluster`, `BaseListener`, `MessageFactory` and
  the custom type registry are gone. In their place: `Bus`, generated server
  interfaces and clients, `Service`, `EventListener` and `Run`.
- **Typed messages instead of maps.** Handlers and calls take and return
  generated protobuf structs, not `map[string]interface{}`. Reflection-based
  handler discovery (`RegisterHandlers`) and `Handle`/`HandleStream` are
  replaced by the generated `Register<Service>Server`.
- **Handler signature.** `func(ctx, data, actor, correlationID)` becomes
  `func(ctx, *Request) (*Response, error)`; the actor and correlation id are
  in `CallInfoFromContext(ctx)`.
- **`NewHandledError` takes `(code, message)`**, the reverse of v1's
  `(message, code)`. Both are strings, so old call sites still compile: check
  every one. `IsHandledError` and `GetHandledError` become `AsHandled`.
- **Streaming.** `ServiceProxy.OpenStream` and `ClientStream.Recv` until
  `io.EOF` become a generated method returning `iter.Seq2[*T, error]`, ranged
  over with `for`. `ErrStreamIdleTimeout` becomes `ErrStreamTimeout`;
  `ErrStreamClosed` and `ErrNotStreamingMethod` are gone.
- **Events** are published as typed messages (`PublishEvent(ctx, msg)`)
  under `EVENT.<full name>`, not as `(eventType, map, topic)`.
- **Default exchange names** are those of the other ports: `proto.bus`,
  `proto.bus.callback`, `proto.bus.events` and `proto.bus.cancel`, instead of
  v1's `protobus` and `protobus.events`. A v1 and a v2 process on one broker
  do not see each other unless the names are configured to match.
- **Environment variables** are the other ports' (`BUS_EXCHANGE_NAME`,
  `RPC_CALL_TIMEOUT_MS`, `MESSAGE_PROCESSING_TIMEOUT`,
  `STREAM_IDLE_TIMEOUT_MS`, ...) instead of v1's `PROTOBUS_EXCHANGE`,
  `PROTOBUS_RPC_TIMEOUT`, `PROTOBUS_MESSAGE_TIMEOUT` and
  `PROTOBUS_STREAM_IDLE_TIMEOUT`. Durations are in milliseconds.
- **Default timeouts** follow the other ports: RPC and processing timeouts are
  600 seconds (v1: 30 seconds).
- **No global configuration.** `GetConfig`/`SetConfig` are gone; a `Config`
  is passed to `Dial` with `WithConfig`.
- **Errors.** v1's `TimeoutError`, `ReconnectionError` and most sentinel errors
  are replaced; see [Errors](docs/errors.md).
- The streaming header constants `HeaderProtobusFinal` and
  `HeaderProtobusSeq` are no longer exported.
- Go 1.25 or newer is required.

## [1.4.0] — 2026-06-04

### Added

- **Server-streaming RPC.** Streaming handlers register via
  `BaseService.HandleStream(method, handler)`; clients open streams via
  `ServiceProxy.OpenStream(ctx, method, data)` and drain a `*ClientStream`
  with `Recv()` until `io.EOF`. End-of-stream is signaled via the
  `x-protobus-final` AMQP header.
- New errors: `ErrStreamIdleTimeout`, `ErrStreamClosed`,
  `ErrNotStreamingMethod`.
- New config: `Config.StreamIdleTimeout` / `PROTOBUS_STREAM_IDLE_TIMEOUT`
  env var (default 60s).
- New exported constants: `HeaderProtobusFinal`, `HeaderProtobusSeq`.

### Fixed

- `BaseListener` serializes AMQP channel writes, which concurrent streaming
  publishes made necessary.

Earlier 1.x releases are tagged in git (`v1.2.1`).

[2.0.0]: https://github.com/ArielLaub/protobus-go/releases/tag/v2.0.0
[1.4.0]: https://github.com/ArielLaub/protobus-go/releases/tag/v1.4.0
