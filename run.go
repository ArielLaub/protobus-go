package protobus

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// Run starts the services and serves until ctx ends or the process receives
// SIGINT or SIGTERM, then shuts the bus down gracefully: services stop taking
// work, in-flight work gets up to Config.ShutdownDrainTimeout to finish, and
// the connection is closed. A second signal during shutdown terminates the
// process the default way.
//
// Run returns nil after a clean shutdown, the drain error if the deadline cut
// it short, the start error of a service that could not start, or Bus.Err
// when the bus gave up on the broker. Release your own resources (databases,
// files) after Run returns: by then no handler is running.
//
//	func main() {
//		bus, err := protobus.Dial(ctx, url)
//		...
//		svc, err := calc.RegisterCalcServer(bus, &server{})
//		...
//		if err := protobus.Run(ctx, bus, svc); err != nil {
//			log.Fatal(err)
//		}
//	}
func Run(ctx context.Context, bus *Bus, services ...*Service) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	for _, s := range services {
		if err := s.Start(ctx); err != nil {
			_ = bus.Close()
			return err
		}
	}
	select {
	case <-ctx.Done():
	case <-bus.Done():
		_ = bus.Close()
		return bus.Err()
	}
	stop()
	return bus.Shutdown(context.Background())
}
