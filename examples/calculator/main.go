// Command calculator is the protobus getting-started example: a service, a
// client, and an event, in one binary.
//
//	docker compose up -d --wait          # RabbitMQ on 127.0.0.1:25672
//	export AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
//	go run ./examples/calculator -mode server   # in one terminal
//	go run ./examples/calculator -mode client   # in another
//	go run ./examples/calculator                # or both at once
//
// The schema (proto/Calculator.proto) is an ordinary protobus schema, shared
// as-is with TypeScript and Python services. Regenerate its Go code with
// go generate.
package main

//go:generate go run ../../cmd/protobus generate -proto ./proto -out ./gen

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/calculator/gen/calculator"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

// server implements Calculator.Service. Embedding the Unimplemented server
// keeps it compiling when the service grows a method.
type server struct {
	calculator.UnimplementedServiceServer
	bus *protobus.Bus
}

func (s *server) Add(ctx context.Context, in *calculator.AddRequest) (*calculator.AddResponse, error) {
	s.announce(ctx, "add")
	return &calculator.AddResponse{Result: in.A + in.B}, nil
}

func (s *server) Divide(ctx context.Context, in *calculator.DivideRequest) (*calculator.DivideResponse, error) {
	if in.Divisor == 0 {
		// A HandledError is an answer, not a failure: the caller gets it at
		// once and it is never retried. Any other error is retried.
		return nil, protobus.NewHandledError("DIVISION_BY_ZERO", "cannot divide by zero")
	}
	s.announce(ctx, "divide")
	return &calculator.DivideResponse{Quotient: in.Dividend / in.Divisor}, nil
}

func (s *server) announce(ctx context.Context, op string) {
	// Events are fire-and-forget fan-out: every subscribing service gets one.
	ev := &calculator.Calculated{Operation: op, At: pbtypes.NewTimestamp(time.Now())}
	if err := s.bus.PublishEvent(ctx, ev); err != nil {
		log.Printf("publishing %s: %v", op, err)
	}
}

func serve(ctx context.Context, bus *protobus.Bus) error {
	svc, err := calculator.RegisterServiceServer(bus, &server{bus: bus}, protobus.WithMaxConcurrent(8))
	if err != nil {
		return err
	}
	// Subscribers get typed events: the handler's parameter type selects the
	// event type, and the default topic EVENT.Calculator.Calculated.
	err = protobus.Subscribe(ctx, svc.Events(), func(_ context.Context, ev *calculator.Calculated, info protobus.EventInfo) error {
		log.Printf("event: %s at %s", ev.Operation, ev.At.AsTime().Format(time.RFC3339))
		return nil
	})
	if err != nil {
		return err
	}
	log.Println("calculator service running; Ctrl-C to stop")
	// Run starts the service and shuts down gracefully on SIGINT/SIGTERM.
	return protobus.Run(ctx, bus, svc)
}

func call(ctx context.Context, bus *protobus.Bus) error {
	client := calculator.NewServiceClient(bus)

	sum, err := client.Add(ctx, &calculator.AddRequest{A: 20, B: 22})
	if err != nil {
		return err
	}
	fmt.Println("20 + 22 =", sum.Result)

	q, err := client.Divide(ctx, &calculator.DivideRequest{Dividend: 1, Divisor: 4}, protobus.WithActor("example-client"))
	if err != nil {
		return err
	}
	fmt.Println("1 / 4 =", q.Quotient)

	_, err = client.Divide(ctx, &calculator.DivideRequest{Dividend: 1, Divisor: 0})
	var remote *protobus.RemoteError
	if errors.As(err, &remote) {
		fmt.Printf("1 / 0 -> %s (%s)\n", remote.Message, remote.Code)
	}
	return nil
}

func main() {
	mode := flag.String("mode", "both", "server, client or both")
	flag.Parse()

	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@127.0.0.1:25672/"
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bus, err := protobus.Dial(ctx, url)
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()

	switch *mode {
	case "server":
		err = serve(ctx, bus)
	case "client":
		err = call(ctx, bus)
	default:
		svcCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- serve(svcCtx, bus) }()
		time.Sleep(200 * time.Millisecond) // let the service declare its queue
		err = call(ctx, bus)
		time.Sleep(100 * time.Millisecond) // let the events print
		cancel()
		if serr := <-done; err == nil {
			err = serr
		}
	}
	if err != nil {
		log.Fatal(err)
	}
}
