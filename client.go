package protobus

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ArielLaub/protobus-go/v2/internal/wire"
)

var (
	// ErrInvalidRequest reports a request message that could not be encoded.
	ErrInvalidRequest = errors.New("protobus: invalid request")
	// ErrInvalidResponse reports a reply that could not be decoded, or that
	// answers a different method than the one called.
	ErrInvalidResponse = errors.New("protobus: invalid response")
)

// Client calls the methods of one service. Generated clients wrap it; it can
// also be used directly with any proto.Message types.
//
// A Client is cheap and safe for concurrent use.
type Client struct {
	bus      *Bus
	contract string // the service as its .proto declares it, e.g. "Calc.Service"
	runtime  string // the name its queue is bound under; contract unless instance-named
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithInstanceName addresses one named instance of a service, as served with
// the service option of the same name: the request routes to
// REQUEST.<service>.<instance>.<method> while the envelope still names the
// contract method, which is what the instance validates against.
func WithInstanceName(instance string) ClientOption {
	return func(c *Client) {
		if instance != "" {
			c.runtime = c.contract + "." + instance
		}
	}
}

// NewClient returns a client for service, the fully-qualified name its .proto
// declares ("<package>.<Service>").
func NewClient(bus *Bus, service string, opts ...ClientOption) *Client {
	c := &Client{bus: bus, contract: service, runtime: service}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Service returns the contract name the client calls.
func (c *Client) Service() string { return c.contract }

func (c *Client) routingKey(method string) string {
	return "REQUEST." + c.runtime + "." + method
}

func (c *Client) envelope(method string, in proto.Message, actor string) ([]byte, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("%w for %s.%s: %w", ErrInvalidRequest, c.contract, method, err)
	}
	return wire.AppendRequest(nil, wire.Request{Method: c.contract + "." + method, Actor: actor, Data: data}), nil
}

// Invoke calls method (its name as the .proto declares it, e.g. "add") with
// in and decodes the reply into out.
//
// A service error arrives as a *RemoteError. Broker-level failures arrive as
// a *PublishError, ErrRPCTimeout, ErrDisconnected or ErrNotReady. With
// NoReply, Invoke returns once the broker confirms the request and leaves out
// untouched.
func (c *Client) Invoke(ctx context.Context, method string, in, out proto.Message, opts ...CallOption) error {
	var o callOptions
	for _, opt := range opts {
		opt.applyCall(&o)
	}
	body, err := c.envelope(method, in, o.actor)
	if err != nil {
		return err
	}
	reply, err := c.bus.dispatcher.call(ctx, c.routingKey(method), body, &o)
	if err != nil || o.noReply {
		return err
	}
	return decodeReply(reply, c.contract+"."+method, out)
}

// decodeReply decodes one ResponseContainer into out, or into the error it
// carries.
func decodeReply(reply []byte, method string, out proto.Message) error {
	resp, err := wire.DecodeResponse(reply)
	if err != nil {
		return fmt.Errorf("%w for %s: %w", ErrInvalidResponse, method, err)
	}
	if e := resp.Error; e != nil {
		return &RemoteError{Method: e.Method, Code: e.Code, Message: e.Message}
	}
	// The reply names the method it answers. One naming another method is
	// not ours: refuse it rather than decode it against the wrong schema.
	if got := resp.Result.Method; got != method {
		return fmt.Errorf("%w: reply for %s answers %q", ErrInvalidResponse, method, got)
	}
	if err := proto.Unmarshal(resp.Result.Data, out); err != nil {
		return fmt.Errorf("%w for %s: %w", ErrInvalidResponse, method, err)
	}
	return nil
}

// Stream calls a server-streaming method and yields each response, decoded
// into a fresh message from newResp.
//
// Nothing is sent until the sequence is ranged over, and each range makes one
// call. Ranging ends normally when the service finishes. A service error, a
// broker failure, a stalled stream (WithIdleTimeout) or the context ending is
// yielded once as the error and ends the range. Breaking out of the range,
// like the context ending, tells the service to stop producing.
func StreamWith[Resp proto.Message](ctx context.Context, c *Client, method string, in proto.Message, newResp func() Resp, opts ...StreamOption) iter.Seq2[Resp, error] {
	return func(yield func(Resp, error) bool) {
		var zero Resp
		var o streamOptions
		for _, opt := range opts {
			opt.applyStream(&o)
		}
		body, err := c.envelope(method, in, o.actor)
		if err != nil {
			yield(zero, err)
			return
		}
		full := c.contract + "." + method
		for frame, err := range c.bus.dispatcher.stream(ctx, c.routingKey(method), body, &o) {
			if err != nil {
				yield(zero, err)
				return
			}
			out := newResp()
			if err := decodeReply(frame, full, out); err != nil {
				// A service error is the stream's terminal frame; the
				// stream is over either way.
				yield(zero, err)
				return
			}
			if !yield(out, nil) {
				return
			}
		}
	}
}

// Stream is StreamWith for generated message types, whose zero value's type
// can create new instances.
func Stream[Resp proto.Message](ctx context.Context, c *Client, method string, in proto.Message, opts ...StreamOption) iter.Seq2[Resp, error] {
	return StreamWith(ctx, c, method, in, newMessage[Resp], opts...)
}

// newMessage allocates a message of type T (a pointer to a generated struct).
func newMessage[T proto.Message]() T {
	var zero T
	return zero.ProtoReflect().Type().New().Interface().(T)
}

// splitMethodName splits "<service>.<method>" at its last dot. The method is
// the final segment; the service is everything before it, so dotted packages
// work.
func splitMethodName(full string) (service, method string, ok bool) {
	i := strings.LastIndexByte(full, '.')
	if i <= 0 || i == len(full)-1 {
		return "", "", false
	}
	return full[:i], full[i+1:], true
}

func lastSegment(s string) string {
	return s[strings.LastIndexByte(s, '.')+1:]
}

// resolveContract finds the service a runtime name serves, by trimming
// trailing segments until one names a service in files: an instance name
// such as "Combat.Player.player6" serves the contract "Combat.Player".
func resolveContract(files interface {
	FindDescriptorByName(protoreflect.FullName) (protoreflect.Descriptor, error)
}, runtime string) (protoreflect.ServiceDescriptor, error) {
	for name := runtime; ; {
		if d, err := files.FindDescriptorByName(protoreflect.FullName(name)); err == nil {
			if sd, ok := d.(protoreflect.ServiceDescriptor); ok {
				return sd, nil
			}
		}
		i := strings.LastIndexByte(name, '.')
		if i <= 0 {
			return nil, fmt.Errorf("protobus: no service in the schema matches %q or any prefix of it", runtime)
		}
		name = name[:i]
	}
}
