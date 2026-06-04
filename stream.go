package protobus

import (
	"context"
	"io"
	"sync"
	"time"
)

// streamSinkKey is the context.Context key under which the listener stashes
// the per-call reply destination and publisher for a streaming handler.
// Unexported — only the framework reads/writes this.
type streamSinkKey struct{}

// streamSink carries the per-delivery reply context a streaming handler
// needs to publish chunks. ReplyTo is the AMQP routing key for the
// per-client callback queue; Publish is a one-call publisher that handles
// header assembly and the underlying AMQP write.
type streamSink struct {
	ReplyTo string
	Publish func(replyTo, correlationID string, body []byte, seq uint32, final bool)
}

// withStreamSink stashes the sink on the context. Internal — used by the
// listener before dispatching to a streaming handler.
func withStreamSink(ctx context.Context, sink *streamSink) context.Context {
	return context.WithValue(ctx, streamSinkKey{}, sink)
}

// streamSinkFromContext pulls the sink off the context, returning nil if
// none was set (i.e. this isn't a streaming dispatch).
func streamSinkFromContext(ctx context.Context) *streamSink {
	if v, ok := ctx.Value(streamSinkKey{}).(*streamSink); ok {
		return v
	}
	return nil
}

// StreamingHandler is the signature for server-streaming RPC method handlers.
//
// Unlike a unary MethodHandler which returns a single result, a streaming
// handler yields zero or more chunks through `send`, then returns. Returning
// without an error closes the stream cleanly; returning an error closes it
// with that error as the terminal payload.
//
// The framework guarantees that calls to `send` are serialized — you can
// invoke it from the handler goroutine without any locking.
//
// See docs/advanced/streaming.md for the full contract.
type StreamingHandler func(
	ctx context.Context,
	data map[string]interface{},
	actor string,
	correlationID string,
	send func(chunk map[string]interface{}) error,
) error

// ClientStream represents an active server-streaming RPC from the client's
// perspective. Iterate it with Recv() until it returns io.EOF; always call
// Close() (typically via defer) to release the dispatcher slot — even if
// Recv() returned io.EOF, an early Close() is a no-op.
//
// ClientStream is safe to use from a single goroutine. Concurrent Recv()
// from multiple goroutines is not supported.
//
// See docs/advanced/streaming.md for the full lifecycle.
type ClientStream struct {
	mu            sync.Mutex
	chunks        chan *ResponseContainer
	idleTimeout   time.Duration
	correlationID string
	closed        bool
	terminalErr   error // set on terminal chunk if it carries an error
	cleanup       func()
}

// Recv blocks until the next chunk arrives, the idle timeout fires, the
// stream ends naturally (io.EOF), or the stream is closed.
//
// On normal termination (final chunk with no error), Recv returns the final
// chunk on its second-to-last call and io.EOF on the next. On mid-stream
// error, the terminal chunk's error is returned in place of io.EOF.
func (s *ClientStream) Recv() (map[string]interface{}, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrStreamClosed
	}
	s.mu.Unlock()

	select {
	case resp, ok := <-s.chunks:
		if !ok {
			// Channel closed by handleReplies on terminal chunk.
			if s.terminalErr != nil {
				return nil, s.terminalErr
			}
			return nil, io.EOF
		}
		if resp.Error != nil {
			// Terminal error chunk — surface and end stream.
			s.terminalErr = NewHandledError(resp.Error.Message, resp.Error.Code)
			return nil, s.terminalErr
		}
		return resp.Result, nil

	case <-time.After(s.idleTimeout):
		return nil, ErrStreamIdleTimeout
	}
}

// Close releases the dispatcher slot for this stream. Idempotent. Safe to
// call from defer even if Recv already returned io.EOF.
//
// In v1, Close does NOT signal the server to stop generating — the server
// keeps publishing chunks, but they're dropped at the dispatcher. Server
// cancellation is on the roadmap.
func (s *ClientStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.cleanup != nil {
		s.cleanup()
	}
	return nil
}

// CorrelationID returns the AMQP correlation_id used for this stream.
// Useful for diagnostics and matching against server-side logs.
func (s *ClientStream) CorrelationID() string {
	return s.correlationID
}
