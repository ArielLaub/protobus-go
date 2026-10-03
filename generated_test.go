package protobus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/gentest"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

// These tests drive the engine through the code protoc-gen-go-protobus
// generated for internal/gentest, exactly as an application would.

type server struct {
	gentest.UnimplementedCalcServer
}

func (server) Add(_ context.Context, in *gentest.AddRequest) (*gentest.AddResponse, error) {
	return &gentest.AddResponse{Sum: in.A + in.B}, nil
}

func (server) Double(_ context.Context, in *pbtypes.Bigint) (*pbtypes.Bigint, error) {
	v, err := in.BigInt()
	if err != nil {
		return nil, err
	}
	return pbtypes.NewBigint(v.Lsh(v, 1))
}

func (server) Count(_ context.Context, in *gentest.CountRequest, s protobus.ServerStream[*gentest.CountChunk]) error {
	for i := range in.N {
		if err := s.Send(&gentest.CountChunk{I: i}); err != nil {
			return err
		}
	}
	return nil
}

func dial(t *testing.T) *protobus.Bus {
	t.Helper()
	cfg := protobus.DefaultConfig()
	cfg.Reconnect.InitialDelay = 5 * time.Millisecond
	bus, err := protobus.Dial(context.Background(), "amqp://fake", protobus.WithConfig(cfg),
		protobus.WithLogger(discard()), protobus.WithDialerForTest(fakebroker.New().Dial))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return bus
}

func TestGeneratedBindings(t *testing.T) {
	bus := dial(t)
	svc, err := gentest.RegisterCalcServer(bus, server{}, protobus.WithMaxConcurrent(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := gentest.NewCalcClient(bus)
	ctx := context.Background()

	out, err := client.Add(ctx, &gentest.AddRequest{A: 40, B: 2})
	if err != nil || out.Sum != 42 {
		t.Fatalf("Add: %v %v", out, err)
	}

	d, err := client.Double(ctx, pbtypes.BigintFromUint64(1<<63))
	if v, _ := d.BigInt(); err != nil || v.String() != "18446744073709551616" {
		t.Fatalf("Double: %v %v", v, err)
	}

	var got []int32
	for chunk, err := range client.Count(ctx, &gentest.CountRequest{N: 4}) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, chunk.I)
	}
	if len(got) != 4 || got[3] != 3 {
		t.Fatalf("Count: %v", got)
	}

	// Embedding Unimplemented answers unimplemented methods with a protocol
	// error rather than failing to compile when the service grows.
	_, err = client.Unimplemented(ctx, &gentest.AddRequest{})
	var re *protobus.RemoteError
	if !errors.As(err, &re) || re.Code != protobus.CodeProtocol {
		t.Fatalf("Echo: %v", err)
	}
	if gentest.Calc_ServiceName != "GenTest.Calc" || gentest.Calc_ServiceDesc.ServiceName != "GenTest.Calc" {
		t.Fatal("service name constants")
	}
}
