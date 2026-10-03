package protobus

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/protoload"
)

// A schema that exists only at runtime, as a gateway or a tool would load it.
const dynamicProto = `syntax = "proto3";
package Dyn;
service Math {
  rpc add(Pair) returns (Sum);
  rpc range(Pair) returns (stream Sum);
}
service Player {
  rpc ping(Pair) returns (Sum);
}
message Pair { int32 a = 1; int32 b = 2; bigint big = 3; }
message Sum { int32 value = 1; }
`

func dynamicBus(t *testing.T, b *fakebroker.Broker) (*Bus, *protoload.Result) {
	t.Helper()
	res, err := protoload.Parse(context.Background(), map[string]string{"dyn.proto": dynamicProto})
	if err != nil {
		t.Fatal(err)
	}
	return dialTest(t, b, fastConfig(), WithRegistry(res.Files, res.Types)), res
}

func field(m proto.Message, name string) protoreflect.Value {
	r := m.ProtoReflect()
	return r.Get(r.Descriptor().Fields().ByName(protoreflect.Name(name)))
}

func set(m proto.Message, name string, v protoreflect.Value) {
	r := m.ProtoReflect()
	r.Set(r.Descriptor().Fields().ByName(protoreflect.Name(name)), v)
}

func newSum(t *testing.T, res *protoload.Result, value int32) proto.Message {
	t.Helper()
	mt, err := res.Types.FindMessageByName("Dyn.Sum")
	if err != nil {
		t.Fatal(err)
	}
	m := mt.New().Interface()
	set(m, "value", protoreflect.ValueOfInt32(value))
	return m
}

func TestDynamicServiceAndClient(t *testing.T) {
	b := fakebroker.New()
	bus, res := dynamicBus(t, b)
	svc, err := bus.RegisterDynamic("Dyn.Math", DynamicHandlers{
		Unary: map[string]DynamicHandler{
			"add": func(_ context.Context, req proto.Message) (proto.Message, error) {
				return newSum(t, res, int32(field(req, "a").Int()+field(req, "b").Int())), nil
			},
		},
		Streams: map[string]DynamicStreamHandler{
			"range": func(_ context.Context, req proto.Message, send func(proto.Message) error) error {
				for i := field(req, "a").Int(); i < field(req, "b").Int(); i++ {
					if err := send(newSum(t, res, int32(i))); err != nil {
						return err
					}
				}
				return nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(testCtx(t)); err != nil {
		t.Fatal(err)
	}

	c, err := bus.ResolveClient("Dyn.Math")
	if err != nil {
		t.Fatal(err)
	}
	req, err := c.NewRequest("add")
	if err != nil {
		t.Fatal(err)
	}
	set(req, "a", protoreflect.ValueOfInt32(2))
	set(req, "b", protoreflect.ValueOfInt32(40))
	out, err := c.Call(testCtx(t), "add", req)
	if err != nil {
		t.Fatal(err)
	}
	if got := field(out, "value").Int(); got != 42 {
		t.Fatalf("value %d", got)
	}

	set(req, "a", protoreflect.ValueOfInt32(0))
	set(req, "b", protoreflect.ValueOfInt32(3))
	var got []int64
	for m, err := range c.CallStream(testCtx(t), "range", req) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, field(m, "value").Int())
	}
	if len(got) != 3 || got[2] != 2 {
		t.Fatalf("stream %v", got)
	}
}

func TestDynamicCallValidatesTheMethod(t *testing.T) {
	b := fakebroker.New()
	bus, res := dynamicBus(t, b)
	c, err := bus.ResolveClient("Dyn.Math")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Call(testCtx(t), "nope", newSum(t, res, 1)); err == nil {
		t.Fatal("an undeclared method must be refused")
	}
	if _, err := c.Call(testCtx(t), "range", newSum(t, res, 1)); err == nil {
		t.Fatal("a streaming method must be refused by Call")
	}
	if _, err := c.Call(testCtx(t), "add", newSum(t, res, 1)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a request of the wrong type must be refused: %v", err)
	}
}

func TestResolveClientTrimsInstanceNames(t *testing.T) {
	// "Dyn.Player.p6" is no service; its contract "Dyn.Player" is, and the
	// instance keeps routing under the full name.
	b := fakebroker.New()
	bus, res := dynamicBus(t, b)
	svc, err := bus.RegisterDynamic("Dyn.Player", DynamicHandlers{Unary: map[string]DynamicHandler{
		"ping": func(context.Context, proto.Message) (proto.Message, error) { return newSum(t, res, 6), nil },
	}}, WithInstance("p6"))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	c, err := bus.ResolveClient("Dyn.Player.p6")
	if err != nil {
		t.Fatal(err)
	}
	if c.Service() != "Dyn.Player" {
		t.Fatalf("contract %q", c.Service())
	}
	req, _ := c.NewRequest("ping")
	out, err := c.Call(testCtx(t), "ping", req)
	if err != nil || field(out, "value").Int() != 6 {
		t.Fatalf("%v %v", out, err)
	}
	if _, err := bus.ResolveClient("Nope.Nothing.x"); err == nil {
		t.Fatal("a name matching no contract must be refused")
	}
}

func TestRegisterDynamicValidates(t *testing.T) {
	b := fakebroker.New()
	bus, _ := dynamicBus(t, b)
	if _, err := bus.RegisterDynamic("Dyn.Math", DynamicHandlers{Unary: map[string]DynamicHandler{"range": nil}}); err == nil {
		t.Fatal("a streaming method registered as unary must be refused")
	}
	if _, err := bus.RegisterDynamic("Dyn.Nope", DynamicHandlers{}); !errors.Is(err, ErrUnknownService) {
		t.Fatalf("got %v", err)
	}
}
