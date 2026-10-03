package protobus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/ArielLaub/protobus-go/v2/internal/wire"
)

// serverStream publishes the frames of one server-streaming reply.
//
// Frames go to the callbacks exchange under the request's replyTo, each with
// x-protobus-seq (from 0) and x-protobus-final. The last frame carries
// final=true, which needs a look-ahead of one: every frame is held until the
// next arrives or the handler returns. An empty stream sends one empty final
// frame; a failing stream ends with its error as the final frame.
type serverStream struct {
	ctx    context.Context
	c      *consumer
	pub    *pubChannel
	d      *amqp.Delivery
	method string

	mu      sync.Mutex
	seq     int64
	pending []byte
	err     error
	done    bool
}

var errStreamFinished = errors.New("protobus: Send after the stream handler returned")

func (s *serverStream) Context() context.Context { return s.ctx }

func (s *serverStream) SendMsg(m proto.Message) error {
	if err := s.ctx.Err(); err != nil {
		return fmt.Errorf("protobus: stream ended: %w", context.Cause(s.ctx))
	}
	data, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("protobus: encoding a stream response of %s: %w", s.method, err)
	}
	body, _ := wire.AppendResponse(nil, wire.Response{Result: &wire.Result{Method: s.method, Data: data}})

	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.done:
		return errStreamFinished
	case s.err != nil:
		return s.err
	}
	if s.pending != nil {
		if err := s.publish(s.pending, false); err != nil {
			s.err = err
			return err
		}
	}
	s.pending = body
	return nil
}

// publish sends one frame. Caller holds s.mu.
func (s *serverStream) publish(body []byte, final bool) error {
	if s.d.ReplyTo == "" {
		s.seq++
		return nil // nobody asked for the frames
	}
	// A caller that has gone is not listening: stop before publishing more.
	if errors.Is(context.Cause(s.ctx), ErrCancelled) {
		return fmt.Errorf("protobus: stream ended: %w", ErrCancelled)
	}
	err := s.c.publishReply(s.pub, s.d, body, amqp.Table{
		HeaderFinal: final,
		HeaderSeq:   intHeader(s.seq),
	})
	if err == nil {
		s.seq++
	}
	return err
}

// finish publishes the final frame after the handler returned. It reports a
// publish failure, which fails the attempt.
func (s *serverStream) finish(handlerErr error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = true
	if errors.Is(context.Cause(s.ctx), ErrCancelled) {
		return nil // cancelled: no final frame
	}
	if s.err != nil {
		return s.err
	}
	if handlerErr == nil {
		body := s.pending
		if body == nil {
			body = []byte{}
		}
		return s.publish(body, true)
	}
	if s.pending != nil {
		if err := s.publish(s.pending, false); err != nil {
			return err
		}
	}
	return s.publish(s.c.serviceErrorBody(s, handlerErr), true)
}

func (c *consumer) serviceErrorBody(s *serverStream, err error) []byte {
	ce := sanitizeForCaller(err, c.bus.cfg.ExposeInternalErrors)
	body, _ := wire.AppendResponse(nil, wire.Response{Error: &wire.Error{Method: s.method, Message: ce.Message, Code: ce.Code}})
	return body
}

// serveStream runs a streaming handler. A mid-stream error becomes the
// stream's final frame and is not retried; a stream cannot be replayed
// without the caller seeing it twice. A failure to publish a frame, though,
// fails the attempt like any other infrastructure error.
func (s *Service) serveStream(ctx context.Context, d *amqp.Delivery, ctl *deliveryControl, method string, st StreamDesc, dec DecodeFunc) handlerResult {
	ss := &serverStream{ctx: ctx, c: s.requests, pub: ctl.pub, d: d, method: method}
	err := st.Handler(s.impl, ctx, dec, ss)

	var pe *payloadError
	switch {
	case errors.As(err, &pe):
		err = newProtocolError("payload did not decode as the request type of " + method)
	case errors.Is(err, ErrUnimplemented):
		err = newProtocolError("invalid service method " + lastSegment(method))
	case err != nil && !errors.Is(context.Cause(ctx), ErrCancelled):
		if _, handled := AsHandled(err); !handled {
			s.bus.log.LogAttrs(ctx, slog.LevelError, "stream handler failed", attrOperation("stream"),
				attrService(s.name), attrMethod(method), attrCorrelationID(d.CorrelationId), attrError(err))
		}
	}
	if perr := ss.finish(err); perr != nil {
		return handlerResult{err: perr}
	}
	return handlerResult{}
}
