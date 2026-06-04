# Streaming RPC

Protobus supports **server-streaming RPC** — a single request from the client can produce *many* response messages from the server, delivered as they're produced, instead of one bundled response at the end.

The motivating use case is LLM token streaming: a model generates a 500-word answer over 10 seconds, and you want to show each token to the user as it arrives rather than waiting for the full response.

> **Status:** server-streaming only (one request → many responses). Client-streaming and bidirectional streaming are not implemented and not currently planned.

## TL;DR

```go
// 1. Register a streaming handler on your service.
svc.HandleStream("tick", func(
    ctx context.Context,
    data map[string]interface{},
    actor, correlationID string,
    send func(map[string]interface{}) error,
) error {
    for i := 0; i < 5; i++ {
        if err := send(map[string]interface{}{
            "seq":     float64(i),
            "payload": fmt.Sprintf("chunk-%d", i),
        }); err != nil {
            return err
        }
    }
    return nil
})

// 2. Client opens a stream and drains it with Recv() until io.EOF.
stream, err := proxy.OpenStream(ctx, "tick", map[string]interface{}{"count": 5.0})
if err != nil { return err }
defer stream.Close()

for {
    chunk, err := stream.Recv()
    if errors.Is(err, io.EOF) { break }
    if err != nil { return err }
    fmt.Println(chunk["payload"])
}
```

The framework handles correlation IDs, the reply queue, end-of-stream detection, error propagation, and cancellation. You write the handler and the loop.

## When to use streaming

Use streaming when:

- The response is **incrementally meaningful** — each chunk is useful before the next arrives (LLM tokens, log tails, video frames, progress updates).
- The response **takes too long** to deliver as one blob — users perceive latency by *time to first byte*, not by total response time.
- You want to **cancel** cleanly — `Close()`-ing the stream releases the dispatcher slot.

Don't use streaming when:

- The chunks are tiny and the response is fast — adding stream overhead just to deliver 50 bytes hurts more than it helps.
- The client always needs the full response before doing anything — pagination over unary calls is simpler.
- The data is **not naturally ordered** — streaming guarantees in-order delivery within a single call, which costs flexibility you might not want.

## Wire protocol

A streaming response is **N+1 AMQP messages** published to the client's reply queue, all carrying the same `CorrelationId` as the request. End-of-stream is signaled by an AMQP **header** on the final message; the message body is a regular response payload like any other.

### Per-message headers

| Header | Type | Required | Meaning |
|---|---|---|---|
| `x-protobus-final` | `bool` | yes (on terminal) | `false` (or absent) → more messages follow. `true` → this is the last chunk. |
| `x-protobus-seq` | `int32` | optional | Monotonically increasing 0-based sequence. Useful for diagnostics; not required for correctness (RabbitMQ guarantees order within the single-publisher → single-queue → single-consumer topology of an RPC reply). |

The standard AMQP `CorrelationId` is reused exactly as for unary calls — it ties every chunk back to the request that initiated the stream.

Both header names are exported as constants: `protobus.HeaderProtobusFinal` and `protobus.HeaderProtobusSeq`.

### Why headers, not the payload

Streaming markers are **transport-layer concerns**, not application data. Keeping them on AMQP headers means:

- `ResponseContainer` (JSON envelope) stays semantically clean — it's "result OR error", not "result OR error PLUS streaming state".
- Adding new transport flags later (cancel, ack, window) costs nothing — no envelope change.
- Old unary clients never see streaming concepts they don't understand.
- The same call site code works whether the framework batches one message or a hundred.

### End-of-stream rules

The terminal message carries `x-protobus-final: true`. Its body is a regular response envelope — typically containing the last data chunk, but it can also be empty (signaling end-only) or an error.

Three terminal outcomes the client must handle:

1. **Normal completion** — `x-protobus-final: true` + a result payload. `Recv()` returns the final chunk, then `io.EOF`.
2. **Mid-stream error** — `x-protobus-final: true` + an error payload. `Recv()` returns the error as a `HandledError`.
3. **Timeout / disconnect** — no terminal message arrives within `Config.StreamIdleTimeout`. `Recv()` returns `ErrStreamIdleTimeout`.

## Declaring a streaming method

The Go port doesn't perform proto reflection at runtime (unlike the TypeScript and Python ports), so streaming methods are registered via a **dedicated API**, not by parsing the `.proto`:

```go
svc.Handle("add", unaryHandler)        // unary
svc.HandleStream("tick", streamHandler) // streaming
```

You can still declare the method as `returns (stream Tick)` in the `.proto` for cross-language clarity — the Go server just uses `HandleStream` regardless. Mixing `Handle` and `HandleStream` for the same method name is rejected at request dispatch time.

## Server API

A streaming handler is a `StreamingHandler` — same signature as `MethodHandler` plus a `send` callback for emitting chunks, and returning `error` to signal completion or failure:

```go
type StreamingHandler func(
    ctx context.Context,
    data map[string]interface{},
    actor string,
    correlationID string,
    send func(chunk map[string]interface{}) error,
) error
```

The framework guarantees that calls to `send` are serialized — you can invoke it from the handler goroutine without any locking. Returning `nil` closes the stream cleanly; returning an error closes it with that error as the terminal payload.

Example:

```go
svc.HandleStream("completeStream", func(
    ctx context.Context,
    data map[string]interface{},
    actor, correlationID string,
    send func(map[string]interface{}) error,
) error {
    for event := range bedrock.ConverseStream(ctx, data) {
        if err := send(map[string]interface{}{"delta": event.Text}); err != nil {
            return err
        }
    }
    // Final yield: the framework auto-marks the last yield as terminal.
    return send(map[string]interface{}{
        "stop_reason": "end_turn",
        "usage":       lastUsage,
    })
})
```

### Look-ahead promotion

The framework buffers one chunk at a time so the last yield's published message gets `x-protobus-final: true` automatically — no extra empty terminal needed when the user yields finalization data last.

### Raising errors mid-stream

Return a `HandledError` to publish a terminal error message that the client will surface via `Recv()`:

```go
if guardrail.Flagged(text) {
    return protobus.NewHandledError("guardrail blocked output", "GUARDRAIL_BLOCKED")
}
```

`HandledError` skips retry/DLQ logic the same way it does for unary calls.

### Concurrency safety

The listener serializes AMQP-channel writes via an internal mutex (`pubMu`) so multiple concurrent streaming handlers on the same listener don't interleave frames on the wire. You don't have to think about it from the handler side; just call `send` whenever you have a chunk.

## Client API

A streaming call returns a `*ClientStream` instead of a single result. The caller iterates with `Recv()` until `io.EOF` and **must** call `Close()` (typically via `defer`) to release the dispatcher slot:

```go
stream, err := proxy.OpenStream(ctx, "tick", map[string]interface{}{"count": 5.0})
if err != nil {
    return err
}
defer stream.Close()

for {
    chunk, err := stream.Recv()
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        return fmt.Errorf("recv: %w", err)
    }
    process(chunk)
}
```

### Error handling

Errors are returned from `Recv()` — same pattern as the standard library `io.Reader`:

```go
for {
    chunk, err := stream.Recv()
    switch {
    case errors.Is(err, io.EOF):
        return nil

    case errors.Is(err, protobus.ErrStreamIdleTimeout):
        // No chunk for Config.StreamIdleTimeout (default 60s)
        return fmt.Errorf("stream went silent: %w", err)

    case err != nil:
        if he, ok := protobus.GetHandledError(err); ok {
            // Server returned a known error mid-stream
            logger.Warn("blocked", "code", he.Code, "msg", he.Message)
            return err
        }
        return err
    }
    process(chunk)
}
```

### Early termination

`Close()` is idempotent and safe to defer. In v1, it releases the dispatcher slot but does NOT signal the server to stop generating — wasted upstream work, but no correctness problem. Server-side cancellation is on the roadmap (see [Limitations](#limitations)).

```go
stream, _ := proxy.OpenStream(ctx, "tick", req)
defer stream.Close()

for {
    chunk, err := stream.Recv()
    if errors.Is(err, io.EOF) { break }
    if err != nil { break }
    if userCancelled.Load() {
        // Close runs via defer; safe to return early.
        return
    }
    process(chunk)
}
```

### Timeouts

`*ClientStream` uses an **idle timeout** rather than a total-call timeout, because a long stream can legitimately take minutes. The default is 60 seconds between chunks (configurable via the `PROTOBUS_STREAM_IDLE_TIMEOUT` env var, in milliseconds). A `context.Context` deadline on the call also caps the idle timeout if it's shorter.

`Recv()` returns `ErrStreamIdleTimeout` if no chunk arrives within the window. The standard RPC timeout does not apply to streaming calls.

## Backpressure

The chunk channel inside `*ClientStream` is buffered at 256 entries. If `Recv()` falls behind the dispatcher, additional chunks are silently dropped — there's no exception, just lost messages. For typical chat token rates (~50 chunks/sec) this is unreachable in practice; if you have a producer faster than that, drain `Recv()` in a tight loop and process chunks asynchronously.

## Backward compatibility

The streaming feature is **purely additive**:

- **Existing unary RPCs are unchanged.** No envelope changes, no API changes, no header changes. The dispatcher only inspects the streaming registry when wiring up a method.
- **Old clients calling new unary methods** — works, no change.
- **Old clients calling new streaming methods** — `Call()` will hang on first chunk's reply because it doesn't know to drain multiple. Use `OpenStream` instead.
- **New clients calling old unary methods** — works, no change.
- **Mixed-version services in the same cluster** — fine, as long as the *individual method* contract agrees on whether it's streaming.

## Comparison with gRPC

Protobus streaming intentionally mirrors gRPC's server-streaming model so the mental model ports:

| | gRPC server-streaming (Go) | Protobus server-streaming (Go) |
|---|---|---|
| Proto syntax | `returns (stream Foo)` | Same — declarative; framework registration is separate |
| Client API | `stream.Recv()` returns `(*Msg, error)` | `stream.Recv()` returns `(map[string]interface{}, error)` |
| Transport | HTTP/2 with stream frames | AMQP with multiple replies on a `CorrelationId` |
| Ordering guarantee | Per-stream FIFO | Per-stream FIFO (RabbitMQ single-queue/single-consumer) |
| End-of-stream signal | HTTP/2 END_STREAM frame | `x-protobus-final: true` header |
| Cancellation | `ctx.Cancel()` propagates server-side | v1: client unwinds locally. Server cancellation: roadmap. |
| Client-streaming / bidi | Supported | Not supported, not planned |

The biggest practical difference: gRPC streams ride on HTTP/2's multiplexed connection, so the cost per stream is low and you can have thousands open. Protobus rides on a single AMQP reply queue per proxy, multiplexed by `CorrelationId` — the per-stream cost is the same as a unary call, but very-high-fanout topologies should be benchmarked.

## Limitations

- **Server-streaming only.** Client-streaming and bidirectional streaming aren't supported.
- **No server-side cancellation in v1.** When a client `Close()`s, the server keeps generating until its handler returns. Wasted upstream work, but no correctness problem. Roadmap: a `<CorrelationId>.cancel` sentinel queue.
- **No exactly-once semantics.** If RabbitMQ requeues a chunk during failover, the client may see duplicates. The framework provides no dedup. For idempotent chunks (LLM deltas, log lines) this is fine; for non-idempotent chunks, the caller is responsible.
- **No chunk-level retry/DLQ.** Standard retry/DLQ applies to the entire RPC, not to individual chunks.
- **256-chunk client-side buffer.** Slow consumers drop chunks silently (no error). Tune via `*ClientStream` source if you need more.
- **Single reply queue per proxy.** All in-flight streams to a single proxy share one reply queue. Very-high-concurrency callers may want multiple proxy instances.

## Implementation notes

For framework contributors. Skip if you're just using streaming.

The streaming path differs from unary in four places:

1. **`BaseService.HandleStream(method, handler)`** (`service.go`) — registers a `StreamingHandler` in a separate map from unary handlers. `onMessage` dispatches based on which map contains the method name.

2. **`BaseService.runStream`** (`service.go`) — invokes the user handler with a `send` callback that publishes each chunk. Uses look-ahead-by-one so the last yield's message gets `x-protobus-final: true` without an extra empty terminal. Catches handler errors and emits them as terminal payloads.

3. **`BaseListener.publishStreamChunk`** (`listener.go`) — the actual AMQP publish per chunk. Guarded by `pubMu` so concurrent streaming handlers on the same listener don't interleave frames.

4. **`ServiceProxy.OpenStream` + `handleReplies`** (`proxy.go`) — pre-registers a chunk channel in `pendingStreams[correlationID]` before publishing the request. `handleReplies` reads `x-protobus-final` from delivery headers and closes the channel when the terminal arrives, signaling end-of-stream to `Recv()`.

The wire format itself uses **only AMQP headers** — no `ResponseContainer` JSON-envelope changes. This is what makes the feature purely additive across all three language ports (TypeScript, Python, Go).

## See also

- [Error Handling](./error-handling.md) — `HandledError` and retry semantics, which apply identically to streaming
- [Configuration](../configuration.md) — `PROTOBUS_STREAM_IDLE_TIMEOUT` setting
- [ServiceProxy](../api/service-proxy.md) — full reference for `OpenStream` and `Call`
