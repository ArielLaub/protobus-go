# Changelog

All notable changes to **protobus-go** are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.0] — 2026-06-04

### Added

- **Server-streaming RPC.** Streaming handlers register via
  `BaseService.HandleStream(method, handler)`; clients open streams via
  `ServiceProxy.OpenStream(ctx, method, data)` and drain a `*ClientStream`
  with `Recv()` until `io.EOF`. End-of-stream is signaled via the
  `x-protobus-final` AMQP header, byte-for-byte compatible with the
  TypeScript and Python ports. See
  [`docs/advanced/streaming.md`](docs/advanced/streaming.md).
- New errors: `ErrStreamIdleTimeout`, `ErrStreamClosed`,
  `ErrNotStreamingMethod`.
- New config: `Config.StreamIdleTimeout` / `PROTOBUS_STREAM_IDLE_TIMEOUT`
  env var (default 60s) — idle timeout between streaming chunks.
- New exported constants: `HeaderProtobusFinal`, `HeaderProtobusSeq`.
- New file: `stream.go` — `StreamingHandler` type, `ClientStream` struct,
  internal context-key helpers.

### Fixed

- **`BaseListener` now serializes AMQP channel writes via a `pubMu` mutex.**
  `amqp091-go.Channel` is not safe for concurrent producer goroutines, which
  streaming surfaced because streaming handlers publish many chunks from
  per-delivery goroutines. The mutex is also applied to the existing
  `sendReply` path for consistency with unary RPC.

### Notes

- Tests: 8 streaming + all pre-existing (factory, types, trie) pass against a
  real RabbitMQ broker.
- Go releases use git tags. Tag this release as `v1.4.0` to publish:
  `git tag v1.4.0 && git push origin v1.4.0`.
- Unlike the TS and Python ports, protobus-go does not perform runtime proto
  reflection — streaming methods are registered via the explicit
  `HandleStream` API rather than declared by parsing `.proto`. The
  `returns (stream Chunk)` syntax remains useful as cross-language
  documentation but has no Go runtime effect.
