package protobus

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

// ServiceDesc describes a service implementation for Register. Generated code
// declares one per service; it can also be written by hand, for instance with
// dynamicpb messages.
type ServiceDesc struct {
	// ServiceName is the service's fully-qualified name as its .proto
	// declares it ("<package>.<Service>"). It must resolve in the bus's file
	// registry, which supplies the contract every request is checked against.
	ServiceName string
	// HandlerType is a nil pointer to the server interface, e.g.
	// (*CalcServer)(nil). When set, Register checks the implementation
	// satisfies it.
	HandlerType any
	Methods     []MethodDesc
	Streams     []StreamDesc
}

// DecodeFunc decodes the request payload into m. A decode failure is answered
// with PROTOCOL_ERROR and never retried; return its error unchanged.
type DecodeFunc func(m proto.Message) error

// MethodDesc binds a unary method to its handler.
type MethodDesc struct {
	// MethodName is the rpc's name exactly as the .proto declares it.
	MethodName string
	Handler    func(srv any, ctx context.Context, dec DecodeFunc) (proto.Message, error)
}

// StreamDesc binds a server-streaming method to its handler.
type StreamDesc struct {
	MethodName string
	Handler    func(srv any, ctx context.Context, dec DecodeFunc, stream RawServerStream) error
}

// RawServerStream sends the responses of a server-streaming call. Generated
// code wraps it as a typed ServerStream.
type RawServerStream interface {
	SendMsg(m proto.Message) error
	Context() context.Context
}

// ServerStream sends the responses of a server-streaming call, in order.
//
// Send returns once the broker has confirmed the frame, which is the only
// backpressure a stream has: there is no flow control from the caller. Send
// fails once the caller has gone (context.Cause(ctx) is then ErrCancelled);
// a handler should stop producing when it does.
//
// Returning nil from the handler ends the stream normally. Returning an error
// ends it with that error, delivered to the caller after the responses
// already sent. A ServerStream must not be used after its handler returns.
type ServerStream[T proto.Message] interface {
	Send(T) error
	Context() context.Context
}

// NewServerStream adapts a RawServerStream for generated code.
func NewServerStream[T proto.Message](s RawServerStream) ServerStream[T] { return typedStream[T]{s} }

type typedStream[T proto.Message] struct{ RawServerStream }

func (s typedStream[T]) Send(m T) error { return s.SendMsg(m) }

// CallInfo describes the request a handler is serving.
type CallInfo struct {
	// Method is the contract method, "<package>.<Service>.<method>".
	Method string
	// Actor is the caller-supplied identity from the envelope. Nothing
	// authenticates it; treat it as a claim, for tracing and auditing.
	Actor         string
	CorrelationID string
	// MessageID is stable across every redelivery and retry of the same
	// logical message: deduplicate on it.
	MessageID string
	// RoutingKey is the key the broker delivered the request on.
	RoutingKey string
	// Redelivered reports that the broker has delivered this message before.
	Redelivered bool
	// Attempt counts retry hops: 0 for the first delivery.
	Attempt int
	// Headers are the AMQP headers as delivered.
	Headers amqp.Table
}

type callInfoKey struct{}

// CallInfoFromContext returns the CallInfo of the request a handler's context
// belongs to.
func CallInfoFromContext(ctx context.Context) (CallInfo, bool) {
	ci, ok := ctx.Value(callInfoKey{}).(*CallInfo)
	if !ok {
		return CallInfo{}, false
	}
	return *ci, true
}

func withCallInfo(ctx context.Context, ci *CallInfo) context.Context {
	return context.WithValue(ctx, callInfoKey{}, ci)
}
