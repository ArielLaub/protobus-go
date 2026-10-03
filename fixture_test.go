package protobus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
)

// This file mirrors what protoc-gen-go-protobus emits for Test.Calc, so the
// engine is tested against the exact shape generated code has. The generator
// has its own golden tests that keep the two in step.

type CalcServer interface {
	Add(context.Context, *testpb.AddRequest) (*testpb.AddResponse, error)
	Fail(context.Context, *testpb.FailRequest) (*testpb.AddResponse, error)
	Slow(context.Context, *testpb.SlowRequest) (*testpb.AddResponse, error)
	Count(context.Context, *testpb.CountRequest, ServerStream[*testpb.CountChunk]) error
	Echo(context.Context, *testpb.Order) (*testpb.Order, error)
	Missing(context.Context, *testpb.AddRequest) (*testpb.AddResponse, error)
}

type UnimplementedCalcServer struct{}

func (UnimplementedCalcServer) Add(context.Context, *testpb.AddRequest) (*testpb.AddResponse, error) {
	return nil, ErrUnimplemented
}
func (UnimplementedCalcServer) Fail(context.Context, *testpb.FailRequest) (*testpb.AddResponse, error) {
	return nil, ErrUnimplemented
}
func (UnimplementedCalcServer) Slow(context.Context, *testpb.SlowRequest) (*testpb.AddResponse, error) {
	return nil, ErrUnimplemented
}
func (UnimplementedCalcServer) Count(context.Context, *testpb.CountRequest, ServerStream[*testpb.CountChunk]) error {
	return ErrUnimplemented
}
func (UnimplementedCalcServer) Echo(context.Context, *testpb.Order) (*testpb.Order, error) {
	return nil, ErrUnimplemented
}
func (UnimplementedCalcServer) Missing(context.Context, *testpb.AddRequest) (*testpb.AddResponse, error) {
	return nil, ErrUnimplemented
}

// unaryMethod and streamMethod produce exactly the handler shape the
// generator emits, including the interceptor path.
func unaryMethod[In proto.Message](name string, newIn func() In, call func(CalcServer, context.Context, In) (proto.Message, error)) MethodDesc {
	return MethodDesc{MethodName: name, Handler: func(srv any, ctx context.Context, dec DecodeFunc, ic UnaryServerInterceptor) (proto.Message, error) {
		in := newIn()
		if err := dec(in); err != nil {
			return nil, err
		}
		if ic == nil {
			return call(srv.(CalcServer), ctx, in)
		}
		info := &UnaryServerInfo{Server: srv, FullMethod: "Test.Calc." + name}
		return ic(ctx, in, info, func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return call(srv.(CalcServer), ctx, req.(In))
		})
	}}
}

func streamMethod[In proto.Message](name string, newIn func() In, call func(CalcServer, context.Context, In, RawServerStream) error) StreamDesc {
	return StreamDesc{MethodName: name, Handler: func(srv any, ctx context.Context, dec DecodeFunc, stream RawServerStream, ic StreamServerInterceptor) error {
		in := newIn()
		if err := dec(in); err != nil {
			return err
		}
		if ic == nil {
			return call(srv.(CalcServer), ctx, in, stream)
		}
		info := &StreamServerInfo{Server: srv, FullMethod: "Test.Calc." + name}
		return ic(ctx, in, stream, info, func(ctx context.Context, req proto.Message, stream RawServerStream) error {
			return call(srv.(CalcServer), ctx, req.(In), stream)
		})
	}}
}

var calcServiceDesc = ServiceDesc{
	ServiceName: "Test.Calc",
	HandlerType: (*CalcServer)(nil),
	Methods: []MethodDesc{
		unaryMethod("add", func() *testpb.AddRequest { return new(testpb.AddRequest) },
			func(s CalcServer, ctx context.Context, in *testpb.AddRequest) (proto.Message, error) {
				return s.Add(ctx, in)
			}),
		unaryMethod("fail", func() *testpb.FailRequest { return new(testpb.FailRequest) },
			func(s CalcServer, ctx context.Context, in *testpb.FailRequest) (proto.Message, error) {
				return s.Fail(ctx, in)
			}),
		unaryMethod("slow", func() *testpb.SlowRequest { return new(testpb.SlowRequest) },
			func(s CalcServer, ctx context.Context, in *testpb.SlowRequest) (proto.Message, error) {
				return s.Slow(ctx, in)
			}),
		unaryMethod("echo", func() *testpb.Order { return new(testpb.Order) },
			func(s CalcServer, ctx context.Context, in *testpb.Order) (proto.Message, error) {
				return s.Echo(ctx, in)
			}),
		unaryMethod("missing", func() *testpb.AddRequest { return new(testpb.AddRequest) },
			func(s CalcServer, ctx context.Context, in *testpb.AddRequest) (proto.Message, error) {
				return s.Missing(ctx, in)
			}),
	},
	Streams: []StreamDesc{
		streamMethod("count", func() *testpb.CountRequest { return new(testpb.CountRequest) },
			func(s CalcServer, ctx context.Context, in *testpb.CountRequest, stream RawServerStream) error {
				return s.Count(ctx, in, NewServerStream[*testpb.CountChunk](stream))
			}),
	},
}

type calcClient struct{ c *Client }

func newCalcClient(b *Bus, opts ...ClientOption) *calcClient {
	return &calcClient{NewClient(b, "Test.Calc", opts...)}
}

func (c *calcClient) Add(ctx context.Context, in *testpb.AddRequest, opts ...CallOption) (*testpb.AddResponse, error) {
	out := new(testpb.AddResponse)
	if err := c.c.Invoke(ctx, "add", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *calcClient) Fail(ctx context.Context, in *testpb.FailRequest, opts ...CallOption) (*testpb.AddResponse, error) {
	out := new(testpb.AddResponse)
	if err := c.c.Invoke(ctx, "fail", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *calcClient) Slow(ctx context.Context, in *testpb.SlowRequest, opts ...CallOption) (*testpb.AddResponse, error) {
	out := new(testpb.AddResponse)
	if err := c.c.Invoke(ctx, "slow", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *calcClient) Echo(ctx context.Context, in *testpb.Order, opts ...CallOption) (*testpb.Order, error) {
	out := new(testpb.Order)
	if err := c.c.Invoke(ctx, "echo", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *calcClient) Missing(ctx context.Context, in *testpb.AddRequest, opts ...CallOption) (*testpb.AddResponse, error) {
	out := new(testpb.AddResponse)
	if err := c.c.Invoke(ctx, "missing", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *calcClient) Count(ctx context.Context, in *testpb.CountRequest, opts ...StreamOption) func(func(*testpb.CountChunk, error) bool) {
	return Stream[*testpb.CountChunk](ctx, c.c, "count", in, opts...)
}

// calcImpl is the test implementation.
type calcImpl struct {
	UnimplementedCalcServer

	attempts sync.Map // messageId -> *atomic.Int32
	running  atomic.Int32
	maxSeen  atomic.Int32
	ended    chan streamEnd // how each count stream ended
	slowDone chan struct{}  // closed... signalled when a slow call returns
}

type streamEnd struct {
	sent      int
	cancelled bool
	cause     error
}

func newCalcImpl() *calcImpl {
	return &calcImpl{ended: make(chan streamEnd, 16), slowDone: make(chan struct{}, 64)}
}

func (c *calcImpl) Add(_ context.Context, in *testpb.AddRequest) (*testpb.AddResponse, error) {
	return &testpb.AddResponse{Sum: in.A + in.B}, nil
}

func (c *calcImpl) Echo(_ context.Context, in *testpb.Order) (*testpb.Order, error) { return in, nil }

func (c *calcImpl) Fail(ctx context.Context, in *testpb.FailRequest) (*testpb.AddResponse, error) {
	switch in.Mode {
	case "handled":
		return nil, NewHandledError(in.Code, in.Message)
	case "panic":
		panic(in.Message)
	case "transient":
		ci, _ := CallInfoFromContext(ctx)
		v, _ := c.attempts.LoadOrStore(ci.MessageID, new(atomic.Int32))
		if n := v.(*atomic.Int32).Add(1); n <= in.SucceedAfter {
			return nil, errors.New(in.Message)
		}
		return &testpb.AddResponse{Sum: int32(ci.Attempt)}, nil
	default:
		return nil, errors.New(in.Message)
	}
}

func (c *calcImpl) Slow(ctx context.Context, in *testpb.SlowRequest) (*testpb.AddResponse, error) {
	n := c.running.Add(1)
	defer c.running.Add(-1)
	for {
		m := c.maxSeen.Load()
		if n <= m || c.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}
	defer func() { c.slowDone <- struct{}{} }()
	t := time.NewTimer(time.Duration(in.Ms) * time.Millisecond)
	defer t.Stop()
	if in.IgnoreCancel {
		<-t.C
		return &testpb.AddResponse{Sum: in.Ms}, nil
	}
	select {
	case <-t.C:
		return &testpb.AddResponse{Sum: in.Ms}, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (c *calcImpl) Count(ctx context.Context, in *testpb.CountRequest, stream ServerStream[*testpb.CountChunk]) error {
	sent := 0
	defer func() {
		c.ended <- streamEnd{sent: sent, cancelled: errors.Is(context.Cause(ctx), ErrCancelled), cause: context.Cause(ctx)}
	}()
	for i := range in.N {
		if in.FailAt > 0 && i == in.FailAt {
			if in.Unhandled {
				return errors.New("stream broke")
			}
			return NewHandledError("TEST_FAIL", "failed at chunk")
		}
		if err := stream.Send(&testpb.CountChunk{I: i}); err != nil {
			return err
		}
		sent++
		if in.DelayMs > 0 {
			select {
			case <-time.After(time.Duration(in.DelayMs) * time.Millisecond):
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	}
	return nil
}

func startCalc(t interface {
	Helper()
	Fatal(...any)
	Cleanup(func())
}, bus *Bus, impl CalcServer, opts ...ServiceOption) *Service {
	t.Helper()
	svc, err := bus.Register(&calcServiceDesc, impl, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return svc
}
