package protobustest_test

import (
	"context"
	"errors"
	"testing"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/internal/gentest"
	"github.com/ArielLaub/protobus-go/v2/protobustest"
)

type calc struct {
	gentest.UnimplementedCalcServer
}

func (calc) Add(_ context.Context, in *gentest.AddRequest) (*gentest.AddResponse, error) {
	return &gentest.AddResponse{Sum: in.A + in.B}, nil
}

func TestServiceAndClientWithoutRabbitMQ(t *testing.T) {
	broker := protobustest.NewBroker()
	server := broker.Dial(t)
	svc, err := gentest.RegisterCalcServer(server, calc{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := gentest.NewCalcClient(broker.Dial(t)) // a second process on the same bus
	out, err := client.Add(context.Background(), &gentest.AddRequest{A: 2, B: 3})
	if err != nil || out.Sum != 5 {
		t.Fatalf("%v %v", out, err)
	}
	if broker.QueueDepth("GenTest.Calc") != 0 {
		t.Fatal("the request was consumed")
	}
}

func TestOutagesCanBeSimulated(t *testing.T) {
	broker := protobustest.NewBroker()
	events := make(chan protobus.ConnectionEvent, 16)
	bus := broker.Dial(t, protobus.WithConnectionObserver(func(e protobus.ConnectionEvent) { events <- e }))
	svc, _ := gentest.RegisterCalcServer(bus, calc{})
	_ = svc.Start(context.Background())
	broker.KillConnections()
	for e := range events {
		if e.Kind == protobus.EventReconnected {
			break
		}
	}
	if _, err := gentest.NewCalcClient(bus).Add(context.Background(), &gentest.AddRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestNewBusIsIsolated(t *testing.T) {
	bus := protobustest.NewBus(t)
	_, err := gentest.NewCalcClient(bus).Add(context.Background(), &gentest.AddRequest{})
	if !errors.Is(err, protobus.ErrUnroutable) {
		t.Fatalf("nothing serves on a fresh broker: %v", err)
	}
}
