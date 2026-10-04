// Package protobustest runs protobus without RabbitMQ, for unit tests.
//
// A Broker is an in-memory broker modelling the RabbitMQ behaviours protobus
// relies on: exchange routing, publisher confirms and mandatory returns,
// prefetch, acknowledgements, message TTL with dead-lettering (so retries and
// dead-letter queues work), priority queues and connection loss. Every Bus
// dialled from one Broker shares its exchanges and queues, as processes
// sharing a real broker do.
//
//	func TestCheckout(t *testing.T) {
//		broker := protobustest.NewBroker()
//		svc, _ := orders.RegisterServiceServer(broker.Dial(t), &server{})
//		_ = svc.Start(context.Background())
//		client := orders.NewServiceClient(broker.Dial(t))
//		...
//	}
//
// It is a model, not RabbitMQ: use it for fast, deterministic tests of your
// services and clients, and keep a few integration tests against the real
// broker.
package protobustest

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testhook"
)

// Broker is an in-memory broker. It is safe for concurrent use.
type Broker struct{ b *fakebroker.Broker }

// NewBroker returns an empty broker.
func NewBroker() *Broker { return &Broker{b: fakebroker.New()} }

// Config is DefaultConfig with reconnection and timeouts shortened for
// tests.
func Config() protobus.Config {
	c := protobus.DefaultConfig()
	c.Reconnect.InitialDelay = 5 * time.Millisecond
	c.Reconnect.MaxDelay = 50 * time.Millisecond
	c.Reconnect.MaxRetries = 0
	c.ProcessingTimeout = 30 * time.Second
	c.RPCTimeout = 30 * time.Second
	c.PublishConfirmTimeout = 5 * time.Second
	c.ConnectionReadyTimeout = 5 * time.Second
	c.ShutdownDrainTimeout = 5 * time.Second
	return c
}

// Dial connects a Bus to the broker, closed when the test ends. It uses
// Config and a logger that discards, unless opts say otherwise.
func (b *Broker) Dial(t testing.TB, opts ...protobus.DialOption) *protobus.Bus {
	t.Helper()
	base := []protobus.DialOption{
		protobus.WithConfig(Config()),
		protobus.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		testhook.WithDialer(b.b.Dial).(protobus.DialOption),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bus, err := protobus.Dial(ctx, "amqp://protobustest/", append(base, opts...)...)
	if err != nil {
		t.Fatalf("protobustest: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return bus
}

// NewBus dials a Bus to a broker of its own.
func NewBus(t testing.TB, opts ...protobus.DialOption) *protobus.Bus {
	t.Helper()
	return NewBroker().Dial(t, opts...)
}

// KillConnections drops every connection, as a broker failure would. Buses
// reconnect and restore themselves.
func (b *Broker) KillConnections() { b.b.KillConnections() }

// Restart simulates a broker restart: connections drop, non-durable queues
// vanish and durable queues keep only persistent messages.
func (b *Broker) Restart() { b.b.Restart() }

// QueueDepth is the number of messages waiting in a queue.
func (b *Broker) QueueDepth(queue string) int { return b.b.QueueDepth(queue) }

// HasQueue reports whether a queue exists.
func (b *Broker) HasQueue(queue string) bool { return b.b.HasQueue(queue) }
