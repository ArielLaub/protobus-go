package protobus

import (
	"context"
	"fmt"
	"iter"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Schemas known only at runtime.
//
// Generated code is the usual way to use protobus from Go. A gateway, a
// debugging tool or a test harness may instead load .proto files at runtime
// (see the protoload package), dial with WithRegistry, and serve or call
// services through the API below, with messages from the registry: generated
// types where linked in, dynamicpb messages otherwise.

// DynamicHandler serves a unary method.
type DynamicHandler func(ctx context.Context, req proto.Message) (proto.Message, error)

// DynamicStreamHandler serves a server-streaming method; send publishes one
// response.
type DynamicStreamHandler func(ctx context.Context, req proto.Message, send func(proto.Message) error) error

// DynamicHandlers maps method names, as the .proto declares them, to their
// handlers. A declared method without a handler answers PROTOCOL_ERROR.
type DynamicHandlers struct {
	Unary   map[string]DynamicHandler
	Streams map[string]DynamicStreamHandler
}

// RegisterDynamic prepares handlers to serve service, which must be in the
// bus's file registry. It is Register for services with no generated code.
func (b *Bus) RegisterDynamic(service string, h DynamicHandlers, opts ...ServiceOption) (*Service, error) {
	sd, err := b.serviceDescriptor(service)
	if err != nil {
		return nil, err
	}
	desc := &ServiceDesc{ServiceName: service}
	for name, handler := range h.Unary {
		md := sd.Methods().ByName(protoreflect.Name(name))
		if md == nil {
			continue // Register reports it
		}
		input, full := md.Input(), string(md.FullName())
		desc.Methods = append(desc.Methods, MethodDesc{
			MethodName: name,
			Handler: func(srv any, ctx context.Context, dec DecodeFunc, ic UnaryServerInterceptor) (proto.Message, error) {
				in := b.newMessage(input)
				if err := dec(in); err != nil {
					return nil, err
				}
				if ic == nil {
					return handler(ctx, in)
				}
				return ic(ctx, in, &UnaryServerInfo{Server: srv, FullMethod: full}, UnaryHandler(handler))
			},
		})
	}
	for name, handler := range h.Streams {
		md := sd.Methods().ByName(protoreflect.Name(name))
		if md == nil {
			continue
		}
		input, full := md.Input(), string(md.FullName())
		serve := func(ctx context.Context, req proto.Message, stream RawServerStream) error {
			return handler(ctx, req, stream.SendMsg)
		}
		desc.Streams = append(desc.Streams, StreamDesc{
			MethodName: name,
			Handler: func(srv any, ctx context.Context, dec DecodeFunc, stream RawServerStream, ic StreamServerInterceptor) error {
				in := b.newMessage(input)
				if err := dec(in); err != nil {
					return err
				}
				if ic == nil {
					return serve(ctx, in, stream)
				}
				return ic(ctx, in, stream, &StreamServerInfo{Server: srv, FullMethod: full}, serve)
			},
		})
	}
	// Methods named but not in the contract still need reporting.
	for name := range h.Unary {
		if sd.Methods().ByName(protoreflect.Name(name)) == nil {
			desc.Methods = append(desc.Methods, MethodDesc{MethodName: name})
		}
	}
	for name := range h.Streams {
		if sd.Methods().ByName(protoreflect.Name(name)) == nil {
			desc.Streams = append(desc.Streams, StreamDesc{MethodName: name})
		}
	}
	return b.Register(desc, h, opts...)
}

func (b *Bus) serviceDescriptor(name string) (protoreflect.ServiceDescriptor, error) {
	d, err := b.files.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, fmt.Errorf("%w %q: %v", ErrUnknownService, name, err)
	}
	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("%w: %q is a %T, not a service", ErrUnknownService, name, d)
	}
	return sd, nil
}

// newMessage allocates a message of md's type, using the registered type when
// there is one.
func (b *Bus) newMessage(md protoreflect.MessageDescriptor) proto.Message {
	if mt, err := b.types.FindMessageByName(md.FullName()); err == nil {
		return mt.New().Interface()
	}
	return dynamicpb.NewMessage(md)
}

// ResolveClient returns a client for name, which may be a service's contract
// name or a runtime name with trailing instance segments: "Combat.Player.p6"
// resolves to the contract "Combat.Player" and routes to the instance.
func (b *Bus) ResolveClient(name string) (*Client, error) {
	sd, err := resolveContract(b.files, name)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnknownService, err)
	}
	c := &Client{bus: b, contract: string(sd.FullName()), runtime: name, sd: sd}
	return c, nil
}

func (c *Client) method(name string) (protoreflect.MethodDescriptor, error) {
	// Not cached on c: a Client is shared between goroutines, and the lookup
	// is a map read.
	sd := c.sd
	if sd == nil {
		var err error
		if sd, err = c.bus.serviceDescriptor(c.contract); err != nil {
			return nil, err
		}
	}
	md := sd.Methods().ByName(protoreflect.Name(name))
	if md == nil {
		return nil, fmt.Errorf("protobus: %s declares no method %q", c.contract, name)
	}
	return md, nil
}

// NewRequest returns an empty request message for method.
func (c *Client) NewRequest(method string) (proto.Message, error) {
	md, err := c.method(method)
	if err != nil {
		return nil, err
	}
	return c.bus.newMessage(md.Input()), nil
}

func (c *Client) checkRequest(md protoreflect.MethodDescriptor, in proto.Message) error {
	if got, want := in.ProtoReflect().Descriptor().FullName(), md.Input().FullName(); got != want {
		return fmt.Errorf("%w: %s takes %s, not %s", ErrInvalidRequest, md.FullName(), want, got)
	}
	return nil
}

// Call invokes a unary method by name, decoding the reply into a new message
// of the method's response type.
func (c *Client) Call(ctx context.Context, method string, in proto.Message, opts ...CallOption) (proto.Message, error) {
	md, err := c.method(method)
	if err != nil {
		return nil, err
	}
	if md.IsStreamingServer() {
		return nil, fmt.Errorf("protobus: %s is server-streaming; use CallStream", md.FullName())
	}
	if err := c.checkRequest(md, in); err != nil {
		return nil, err
	}
	out := c.bus.newMessage(md.Output())
	if err := c.Invoke(ctx, method, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

// CallStream invokes a server-streaming method by name. See Stream.
func (c *Client) CallStream(ctx context.Context, method string, in proto.Message, opts ...StreamOption) iter.Seq2[proto.Message, error] {
	md, err := c.method(method)
	if err == nil && !md.IsStreamingServer() {
		err = fmt.Errorf("protobus: %s is unary; use Call", md.FullName())
	}
	if err == nil {
		err = c.checkRequest(md, in)
	}
	if err != nil {
		return func(yield func(proto.Message, error) bool) { yield(nil, err) }
	}
	output := md.Output()
	return StreamWith(ctx, c, method, in, func() proto.Message { return c.bus.newMessage(output) }, opts...)
}
