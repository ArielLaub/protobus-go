package protobus

import (
	"context"

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

// MethodDesc binds a unary method to its handler. It is used by generated
// code and is not meant to be written by hand except for dynamic services;
// it may gain fields.
type MethodDesc struct {
	// MethodName is the rpc's name exactly as the .proto declares it.
	MethodName string
	// Handler decodes the request with dec and serves it, through
	// interceptor when one is configured (it is nil otherwise).
	Handler func(srv any, ctx context.Context, dec DecodeFunc, interceptor UnaryServerInterceptor) (proto.Message, error)
}

// StreamDesc binds a server-streaming method to its handler; see MethodDesc.
type StreamDesc struct {
	MethodName string
	Handler    func(srv any, ctx context.Context, dec DecodeFunc, stream RawServerStream, interceptor StreamServerInterceptor) error
}

// UnaryServerInfo describes a unary call to an interceptor.
type UnaryServerInfo struct {
	// Server is the service implementation.
	Server any
	// FullMethod is the contract method, "<package>.<Service>.<method>".
	FullMethod string
}

// UnaryHandler serves a decoded unary request.
type UnaryHandler func(ctx context.Context, req proto.Message) (proto.Message, error)

// UnaryServerInterceptor wraps the serving of a unary request: logging,
// metrics, tracing, authorisation. It calls handler to continue, and may
// return without calling it. See WithUnaryInterceptor.
type UnaryServerInterceptor func(ctx context.Context, req proto.Message, info *UnaryServerInfo, handler UnaryHandler) (proto.Message, error)

// StreamServerInfo describes a server-streaming call to an interceptor.
type StreamServerInfo struct {
	Server     any
	FullMethod string
}

// StreamHandler serves a decoded streaming request.
type StreamHandler func(ctx context.Context, req proto.Message, stream RawServerStream) error

// StreamServerInterceptor wraps the serving of a streaming request. It may
// wrap stream to observe or alter what is sent. See WithStreamInterceptor.
type StreamServerInterceptor func(ctx context.Context, req proto.Message, stream RawServerStream, info *StreamServerInfo, handler StreamHandler) error

// chainUnary composes interceptors, the first outermost.
func chainUnary(is []UnaryServerInterceptor) UnaryServerInterceptor {
	switch len(is) {
	case 0:
		return nil
	case 1:
		return is[0]
	}
	return func(ctx context.Context, req proto.Message, info *UnaryServerInfo, handler UnaryHandler) (proto.Message, error) {
		next := handler
		for i := len(is) - 1; i > 0; i-- {
			inner, ic := next, is[i]
			next = func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return ic(ctx, req, info, inner)
			}
		}
		return is[0](ctx, req, info, next)
	}
}

// chainStream composes interceptors, the first outermost.
func chainStream(is []StreamServerInterceptor) StreamServerInterceptor {
	switch len(is) {
	case 0:
		return nil
	case 1:
		return is[0]
	}
	return func(ctx context.Context, req proto.Message, stream RawServerStream, info *StreamServerInfo, handler StreamHandler) error {
		next := handler
		for i := len(is) - 1; i > 0; i-- {
			inner, ic := next, is[i]
			next = func(ctx context.Context, req proto.Message, stream RawServerStream) error {
				return ic(ctx, req, stream, info, inner)
			}
		}
		return is[0](ctx, req, stream, info, next)
	}
}

// WithUnaryInterceptor adds interceptors around every unary method of the
// service. Interceptors run in the order given, the first outermost, across
// repeated uses of the option.
func WithUnaryInterceptor(is ...UnaryServerInterceptor) ServiceOption {
	return serviceOpt(func(o *serviceOptions) { o.unaryInterceptors = append(o.unaryInterceptors, is...) })
}

// WithStreamInterceptor adds interceptors around every server-streaming
// method of the service, like WithUnaryInterceptor.
func WithStreamInterceptor(is ...StreamServerInterceptor) ServiceOption {
	return serviceOpt(func(o *serviceOptions) { o.streamInterceptors = append(o.streamInterceptors, is...) })
}

// RawServerStream sends the responses of a server-streaming call. Generated
// code wraps it as a typed ServerStream.
type RawServerStream interface {
	SendMsg(m proto.Message) error
	Context() context.Context
}

// ServerStream sends the responses of a server-streaming call, in order.
//
// Each response is held until the next one (or the handler's return), so the
// last can be marked final: Send buffers its message and publishes the one
// before, returning once the broker has confirmed it. That confirm is the only
// backpressure a stream has; there is no flow control from the caller. Send
// fails once the caller has gone (context.Cause(ctx) is then ErrCancelled), and
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
	// Headers are the AMQP headers as delivered (a copy).
	Headers map[string]any
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
