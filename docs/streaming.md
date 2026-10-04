# Streaming

A server-streaming method answers one request with a sequence of responses:

```protobuf
service Assistant {
  rpc generate (Chat.GenerateRequest) returns (stream Chat.Token);
}
```

The service sends responses as it produces them; the caller reads them as a
Go iterator and can stop at any point, which tells the service to stop too.
Client-streaming and bidirectional methods are not supported: `Register`
refuses them.

The snippets use the tokenstream example's generated package
(`examples/tokenstream/gen/chat`), which `go run ./examples/tokenstream` runs
end to end.

## The service side

The generated server method receives a `protobus.ServerStream[*chat.Token]`.
The square brackets are Go generics: the stream's `Send` accepts only
`*chat.Token`.

```go
type assistant struct {
	chat.UnimplementedAssistantServer
}

func (a *assistant) Generate(ctx context.Context, in *chat.GenerateRequest, stream protobus.ServerStream[*chat.Token]) error {
	for i, word := range strings.Fields(in.Prompt) {
		if err := stream.Send(&chat.Token{Index: int32(i), Text: word}); err != nil {
			return err // the caller has gone, or a frame could not be published
		}
	}
	return nil // ends the stream normally
}
```

- Returning nil ends the stream; the caller's range ends without an error.
- Returning an error ends the stream with it, after the responses already
  sent. It is classified as in [Services](services.md#what-a-handlers-result-means):
  a `HandledError` crosses with its code and message, an unhandled error's
  text follows `Config.ExposeInternalErrors`. **A stream that fails part way is
  never retried**, whatever the error: it cannot be replayed without the
  caller seeing its responses twice.
- An unimplemented method ends the stream at once with `PROTOCOL_ERROR`.
- A streaming handler holds one of the service's `WithMaxConcurrent` slots
  until it returns.
- The processing timeout does not apply. A stream is bounded by its caller:
  the idle timeout, cancellation and the caller's context.
- `ctx` ends when the caller cancels (`context.Cause(ctx)` is
  `ErrCancelled`), when the connection is lost or when the service is closed.
  Watch it while waiting on slow work, as the tokenstream example does.

### ServerStream.Send

Every frame says whether it is the last, which needs a look-ahead of one. So
`Send` encodes its message and holds it, publishes the message held from the
previous `Send`, and returns once the broker has confirmed that one. The first
`Send` returns at once; the last message is published, marked final, after the
handler returns.

That broker confirm is the only backpressure a producer gets. Nothing flows
back from the caller: a producer faster than its reader fills the caller's
buffer until the caller gives up (see [Buffer bounds](#buffer-bounds-and-backpressure)).

`Send` returns an error when:

| Situation | Error |
|---|---|
| The caller cancelled | wraps `ErrCancelled` |
| The handler's context ended otherwise (connection lost, service closed) | `protobus: stream ended: <cause>` |
| A frame could not be published | that publish error; every later `Send` returns it too |
| Called after the handler returned | `ErrStreamFinished` |

Stop producing on the first error and return it. A frame that could not be
published fails the attempt as an infrastructure error, which goes through the
service's [retry ladder](services.md#the-retry-ladder) and runs the handler
again from the start. The caller keeps its place: it drops frames whose
sequence number it has already received, so a re-run that produces the same
frames continues the stream where it broke off.

A `ServerStream` must not be used after its handler returns.

### On the wire

Frames are published to `proto.bus.callback` under the request's `replyTo`,
each carrying `x-protobus-seq` (from 0) and `x-protobus-final`. Each body is an
ordinary `ResponseContainer`. An empty stream is one empty final frame; a
failing stream ends with its error as the final frame. Every frame is published
before the request is acknowledged. See [Compatibility](compatibility.md).

## The client side

The generated client method returns an `iter.Seq2[*chat.Token, error]`: a
function that a `for ... range` loop calls, yielding a (token, error) pair per
step.

```go
func printTokens(ctx context.Context, client chat.AssistantClient) error {
	for tok, err := range client.Generate(ctx, &chat.GenerateRequest{Prompt: "why stream?"}) {
		if err != nil {
			return err // yielded at most once, as the last step
		}
		fmt.Print(tok.Text)
	}
	return nil // the service ended the stream normally
}
```

- **Lazy start.** Nothing is sent when `Generate` is called; the request is
  published when the loop starts. Each range over the sequence is a separate
  call, so ranging over it twice calls the service twice. A sequence that is
  never ranged over sends nothing.
- **Ending.** The loop ends normally when the service finishes. A failure is
  yielded once, with a nil token, and ends the loop.
- A context that is already cancelled yields its error and publishes nothing.

| Yielded error | Cause |
|---|---|
| `*RemoteError` | the service ended the stream with an error |
| `*PublishError` wrapping `ErrUnroutable` | no service queue is bound: fails at once rather than waiting out the idle timeout |
| other `*PublishError`, `ErrNotReady` | the request could not be published |
| `ErrStreamTimeout` | no frame within the idle timeout |
| `ErrStreamBackpressure` | the caller fell too far behind |
| `ErrStreamSequence` | a frame was lost in transit |
| `ErrDisconnected`, `ErrClosed` | the connection was lost, or the bus closed |
| `context.Canceled`, `context.DeadlineExceeded` | the caller's context ended |

A failure that arrives after the final frame does not spoil a stream that has
already completed: the frames received are still yielded.

Streaming calls take `StreamOption`s: `WithActor` and `WithIdleTimeout`.
Priority, message ids, `WithTimeout` and `NoReply` are unary-only and do not
compile on a streaming call. To bound a whole stream, give its context a
deadline.

## Stopping a stream

The caller stops a stream by leaving the loop early:

```go
func firstWords(ctx context.Context, client chat.AssistantClient, n int) ([]string, error) {
	var words []string
	for tok, err := range client.Generate(ctx, &chat.GenerateRequest{Prompt: "a long answer"}) {
		if err != nil {
			return words, err
		}
		words = append(words, tok.Text)
		if len(words) == n {
			break // tells the service to stop producing
		}
	}
	return words, nil
}
```

`break` (or `return`) out of the loop, the context ending, the idle timeout, a
backpressure or lost-frame failure all send one **cancel notice** to the
service. On the service side:

1. the handler's context is cancelled with cause `ErrCancelled`, and `Send`
   fails with an error wrapping it;
2. no final frame is published;
3. the request is acknowledged: a cancelled stream is not a failure, so it is
   not retried or dead-lettered.

A stopped stream therefore saves the producer's work, not just the reading,
which is the point of the tokenstream example.

### The cancel exchange

A cancel notice is an empty message on the fanout exchange `proto.bus.cancel`
(`CANCEL_EXCHANGE_NAME`) carrying the stream's correlation id. The caller
cannot know which replica is producing its stream, so every process serving a
service binds its own exclusive, auto-delete queue to the exchange (when its
first service starts), hears every cancel, and acts only on the correlation
ids it is serving. It cancels every copy of the request it holds, in case a
redelivery overlaps the original.

Cancellation is best effort by design:

- It is sent at most once per call, detached from the caller's context (often
  the reason the stream ended) and bounded by the sooner of
  `Config.PublishConfirmTimeout` and 5 s. A failure to send it is logged at
  debug level.
- Cancels are consumed with auto-ack. A lost cancel means the stream runs to
  completion, its frames dropped by a caller no longer listening.
- If the process cannot declare the exchange (credentials without configure
  permission on it), services start anyway and log a warning; their streams
  run to completion.
- No cancel is sent when the stream completed, the connection was lost or the
  bus was closed. One is sent after a stream that ended with a service error;
  the service has already finished, so it is ignored.

Only streams listen for cancels; a unary call that times out is not
cancelled.

## Idle timeout

`WithIdleTimeout(d)` bounds the gap between frames for one call, defaulting to
`Config.StreamIdleTimeout` (`STREAM_IDLE_TIMEOUT_MS`, default 60 s). The time
the caller spends in its own loop body does not count: the timer restarts when
the loop asks for the next frame. When it expires the loop yields an error
wrapping `ErrStreamTimeout`, and the service is told to stop.

```go
func patientTokens(ctx context.Context, client chat.AssistantClient) error {
	for _, err := range client.Generate(ctx, &chat.GenerateRequest{Prompt: "think hard"},
		protobus.WithIdleTimeout(2*time.Minute), protobus.WithActor("ui")) {
		if errors.Is(err, protobus.ErrStreamTimeout) {
			return fmt.Errorf("the model stalled: %w", err)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
```

The stream as a whole has no deadline unless its context sets one.

## Buffer bounds and backpressure

Replies for every call in the process arrive on one reply queue, read by one
goroutine that must never block. Frames for a stream are therefore appended to
a buffer for that call, which the loop drains. Three bounds keep a slow reader
from exhausting memory:

| Config | Env | Default | Scope |
|---|---|---|---|
| `StreamMaxBufferedChunks` | `STREAM_MAX_BUFFERED_CHUNKS` | 1024 | frames waiting, per call |
| `StreamMaxBufferedBytes` | `STREAM_MAX_BUFFERED_BYTES` | 64 MiB | bytes waiting, per call |
| `StreamMaxTotalBufferedBytes` | `STREAM_MAX_TOTAL_BUFFERED_BYTES` | 256 MiB | bytes waiting across every stream of the `Bus` |

When a frame would cross any of them, the stream fails. Frames still waiting
in the buffer are discarded, not yielded, and their bytes returned to the
shared allowance; the loop's next step yields an error wrapping
`ErrStreamBackpressure`, and a cancel notice tells the producer to stop. The
empty final frame does not count.

There is no flow control back to the producer, so the remedy is on the caller:
do less work per frame in the loop, or hand frames off to another goroutine,
or raise the bounds (see [Configuration](configuration.md)).

## Ordering, duplicates and loss

The caller checks `x-protobus-seq` on every frame:

- a frame with a sequence number already seen (a redelivery, or a re-run of the
  handler) is dropped;
- a gap means a frame was lost, and the stream fails with
  `ErrStreamSequence` rather than handing back a short stream that looks
  complete;
- frames from a peer that sends no sequence numbers are accepted in arrival
  order.

## Calls without generated code

`Client.CallStream` streams a method by name, decoding each frame into the
method's output type from the registry; `protobus.Stream[Resp]` and
`StreamWith` are the generic forms generated code uses. On the service side, a
`DynamicStreamHandler` receives `send`, with the semantics of
`ServerStream.Send`.

```go
func streamDynamic(ctx context.Context, bus *protobus.Bus) error {
	c, err := bus.ResolveClient("Chat.Assistant")
	if err != nil {
		return err
	}
	req, err := c.NewRequest("generate")
	if err != nil {
		return err
	}
	m := req.ProtoReflect()
	m.Set(m.Descriptor().Fields().ByName("prompt"), protoreflect.ValueOfString("hello there"))
	for tok, err := range c.CallStream(ctx, "generate", req) {
		if err != nil {
			return err
		}
		fmt.Println(tok)
	}
	return nil
}

func streamTyped(ctx context.Context, bus *protobus.Bus) error {
	c := protobus.NewClient(bus, "Chat.Assistant")
	for tok, err := range protobus.Stream[*chat.Token](ctx, c, "generate", &chat.GenerateRequest{Prompt: "hi"}) {
		if err != nil {
			return err
		}
		fmt.Println(tok.Text)
	}
	return nil
}
```

`CallStream` refuses a unary method or a request of the wrong type by yielding
the error, before anything is sent.
