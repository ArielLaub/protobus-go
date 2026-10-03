package protobus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
	"github.com/ArielLaub/protobus-go/v2/internal/uuid"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
)

// PublishOption configures one event publish.
type PublishOption func(*publishOptions)

type publishOptions struct {
	topic     string
	messageID *string
}

// WithTopic publishes under topic instead of the default "EVENT.<type>".
func WithTopic(topic string) PublishOption {
	return func(o *publishOptions) { o.topic = topic }
}

// WithEventMessageID sets the event's message id, for deduplication by
// subscribers. The same rules as WithMessageID apply.
func WithEventMessageID(id string) PublishOption {
	return func(o *publishOptions) { o.messageID = &id }
}

// EventTopic is the default topic of an event type: "EVENT.<full name>".
func EventTopic(msg proto.Message) string {
	return "EVENT." + string(proto.MessageName(msg))
}

// PublishEvent publishes msg on the events exchange. It returns once the
// broker has confirmed the event. An event with no subscriber is not an error.
//
// The event's type is msg's fully-qualified protobuf name; subscribers decode
// it with that type.
func (b *Bus) PublishEvent(ctx context.Context, msg proto.Message, opts ...PublishOption) error {
	var o publishOptions
	for _, opt := range opts {
		opt(&o)
	}
	typ := string(proto.MessageName(msg))
	if typ == "" {
		return fmt.Errorf("%w: event has no message type", ErrInvalidRequest)
	}
	if o.topic == "" {
		o.topic = "EVENT." + typ
	}
	co := callOptions{messageID: o.messageID}
	if err := co.validate(); err != nil {
		return err
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("%w: event %s: %w", ErrInvalidRequest, typ, err)
	}
	pubMsg := amqp.Publishing{
		ContentType:   contentTypeOctetStream,
		CorrelationId: uuid.New(),
		DeliveryMode:  amqp.Persistent,
		Body:          wire.AppendEvent(nil, wire.Event{Type: typ, Topic: o.topic, Data: data}),
	}
	if o.messageID != nil {
		pubMsg.MessageId = *o.messageID
	}
	return b.events.publish(ctx, o.topic, pubMsg)
}

// eventPublisher owns the channel events are published on.
type eventPublisher struct {
	bus    *Bus
	mu     sync.Mutex
	pub    *pubChannel
	closed bool
}

func newEventPublisher(b *Bus) *eventPublisher { return &eventPublisher{bus: b} }

func (e *eventPublisher) restoreTopology(_ context.Context, conn transport.Conn) error {
	raw, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := raw.ExchangeDeclare(e.bus.cfg.EventsExchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring exchange %s: %w", e.bus.cfg.EventsExchange, err)
	}
	pub, err := newPubChannel(raw, e.bus.cfg)
	if err != nil {
		return err
	}
	e.mu.Lock()
	old := e.pub
	e.pub = pub
	e.mu.Unlock()
	if old != nil {
		old.close()
	}
	return nil
}

func (e *eventPublisher) connectionLost(error) {}

func (e *eventPublisher) close() {
	e.mu.Lock()
	e.closed = true
	pub := e.pub
	e.pub = nil
	e.mu.Unlock()
	if pub != nil {
		pub.close()
	}
}

func (e *eventPublisher) publish(ctx context.Context, topic string, msg amqp.Publishing) error {
	for {
		if err := e.bus.sess.whenReady(ctx); err != nil {
			return err
		}
		e.mu.Lock()
		closed, pub := e.closed, e.pub
		e.mu.Unlock()
		if closed {
			return ErrClosed
		}
		if pub == nil || pub.isClosed() {
			if err := e.repair(ctx); err != nil {
				return err
			}
			continue
		}
		err := pub.publish(ctx, e.bus.cfg.EventsExchange, topic, false, msg)
		if errors.Is(err, errChannelGone) {
			continue
		}
		return err
	}
}

func (e *eventPublisher) repair(ctx context.Context) error {
	e.bus.sess.restoreMu.Lock()
	defer e.bus.sess.restoreMu.Unlock()
	e.mu.Lock()
	healthy := e.pub != nil && !e.pub.isClosed()
	e.mu.Unlock()
	if healthy {
		return nil
	}
	conn, _, ready := e.bus.sess.current()
	if !ready {
		return nil
	}
	return e.restoreTopology(ctx, conn)
}
