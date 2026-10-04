// Package transport is the seam between protobus and the AMQP client.
//
// protobus talks to the broker only through Conn and Channel, the subset of
// amqp091-go it uses. The production implementation is a thin adapter over
// amqp091-go (Dial); tests substitute an in-memory broker with the same
// semantics, which is what lets the settlement, retry, reconnection and
// streaming logic be tested deterministically without RabbitMQ.
package transport

import (
	"context"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Conn is an AMQP connection.
type Conn interface {
	Channel() (Channel, error)
	// NotifyClose registers a listener for the connection closing. A nil
	// error is sent for a graceful close; the channel is then closed.
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Close() error
	IsClosed() bool
}

// Channel is an AMQP channel. *amqp091.Channel satisfies it.
type Channel interface {
	Confirm(noWait bool) error
	NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyCancel(c chan string) chan string
	Qos(prefetchCount, prefetchSize int, global bool) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(consumer string, noWait bool) error
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	GetNextPublishSeqNo() uint64
	IsClosed() bool
	Close() error
}

// Dialer opens connections. Production code uses Dial; tests inject a fake.
type Dialer func(ctx context.Context, url string, cfg amqp.Config) (Conn, error)

// handshakeTimeout bounds the TCP connect and AMQP handshake when the
// caller's context carries no earlier deadline. Heartbeats only start once the
// connection is open, so without a bound a broker that accepts the socket and
// then stalls would hang the dial forever.
const handshakeTimeout = 30 * time.Second

// Dial connects to a broker with amqp091-go, honouring ctx for both the TCP
// connect and the AMQP handshake.
func Dial(ctx context.Context, url string, cfg amqp.Config) (Conn, error) {
	if cfg.Dial == nil {
		cfg.Dial = func(network, addr string) (net.Conn, error) {
			var d net.Dialer
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			deadline := time.Now().Add(handshakeTimeout)
			if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
				deadline = dl
			}
			// amqp091 clears the deadline once the handshake completes.
			if err := conn.SetDeadline(deadline); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		}
	}

	type result struct {
		conn *amqp.Connection
		err  error
	}
	done := make(chan result, 1)
	go func() {
		c, err := amqp.DialConfig(url, cfg)
		done <- result{c, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return nil, r.err
		}
		return amqpConn{r.conn}, nil
	case <-ctx.Done():
		// The dial goroutine finishes on its own (the socket deadline bounds
		// it); close whatever it produces so nothing leaks.
		go func() {
			if r := <-done; r.conn != nil {
				_ = r.conn.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

type amqpConn struct{ *amqp.Connection }

func (c amqpConn) Channel() (Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

var _ Channel = (*amqp.Channel)(nil)
