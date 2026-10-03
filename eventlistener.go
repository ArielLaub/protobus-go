package protobus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ArielLaub/protobus-go/v2/internal/topic"
	"github.com/ArielLaub/protobus-go/v2/internal/transport"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
)

// ErrUnknownEventType reports an event whose type is not in the bus's type
// registry, so it cannot be decoded for a SubscribeAll handler.
var ErrUnknownEventType = errors.New("protobus: unknown event type")

// EventInfo describes a delivered event.
type EventInfo struct {
	// Type is the event's fully-qualified protobuf message name.
	Type string
	// Topic is the topic the publisher declared in the event.
	Topic string
	// RoutingKey is the key the broker delivered the event on. Handlers are
	// matched against it, not against Topic, which the publisher controls.
	RoutingKey    string
	MessageID     string
	CorrelationID string
	Redelivered   bool
	// Attempt counts retry hops (with WithEventRetry): 0 for the first.
	Attempt int
	// Headers are the AMQP headers as delivered (a copy).
	Headers map[string]any
}

// EventListener consumes events from one queue and runs the handlers whose
// topic patterns match each delivery. Every queue gets a copy of an event;
// replicas sharing a queue compete for it.
//
// Handlers for one event run in the order they were subscribed, and the
// first failure stops the rest. Without retries a failing event is dropped
// (rejected); see WithEventRetry.
type EventListener struct {
	bus      *Bus
	queue    string // "" for a private, exclusive, auto-delete queue
	consumer *consumer

	mu         sync.Mutex
	router     *topic.Trie[*subscription]
	all        []*subscription
	patterns   []string
	started    bool
	lazy       bool // start on first subscription
	standalone bool // made by NewEventListener, not owned by a Service
}

type subscription struct {
	typeName protoreflect.FullName // "" accepts every type
	handle   func(ctx context.Context, ev *wire.Event, info EventInfo) error
}

// NewEventListener returns an event listener on queue. A named queue is
// durable and shared by every process listening under the name; an empty
// name gives this process a private queue that disappears with its
// connection (and cannot use retries). Subscribe, then Start.
//
// A Service has its own listener: Service.Events.
func (b *Bus) NewEventListener(queue string, opts ...ListenerOption) (*EventListener, error) {
	o := listenerOptions{concurrency: b.cfg.DefaultPrefetch}
	for _, opt := range opts {
		opt.applyListener(&o)
	}
	if o.concurrency < 1 || o.concurrency > 65535 {
		return nil, fmt.Errorf("protobus: listener concurrency must be within 1..65535, got %d", o.concurrency)
	}
	if o.retry.MaxRetries < 0 || o.retry.MaxRetries > 0 && o.retry.Delay <= 0 {
		return nil, fmt.Errorf("protobus: invalid EventRetryPolicy %+v", o.retry)
	}
	if queue == "" && o.retry.MaxRetries > 0 {
		return nil, errors.New("protobus: event retries need a named queue: a private queue disappears with its connection")
	}
	l := b.newEventListener(queue, o.concurrency, o.retry)
	l.standalone = true
	return l, nil
}

func (b *Bus) newEventListener(queue string, concurrency int, retry EventRetryPolicy) *EventListener {
	l := &EventListener{bus: b, queue: queue, router: topic.New[*subscription]()}
	spec := consumerSpec{
		queue:    queue,
		exchange: b.cfg.EventsExchange,
		bindings: l.bindings,
		lateAck:  true,
		prefetch: concurrency,
		timeout:  b.cfg.ProcessingTimeout,
		handle:   l.handle,
		describe: queue,
	}
	if retry.MaxRetries > 0 && queue != "" {
		rs := &retrySpec{
			maxRetries: retry.MaxRetries,
			exchange:   queue + ".Retry.Exchange",
			queue:      queue + ".Retry",
			dlq:        queue + ".DLQ",
		}
		redelivery := queue + ".Redelivery"
		spec.retry = rs
		spec.dlqHandled = true
		// Events fan out, so the retry queue must not dead-letter back to
		// the events exchange: every subscriber bound to the topic would get
		// it again, including those that handled it. It dead-letters to a
		// per-listener exchange bound only to this listener's queue, which
		// keeps the routing key the handlers match on.
		spec.declare = func(ch transport.Channel, q string) error {
			if err := ch.ExchangeDeclare(redelivery, "topic", true, false, false, false, nil); err != nil {
				return fmt.Errorf("declaring %s: %w", redelivery, err)
			}
			if err := ch.QueueBind(q, "#", redelivery, false, nil); err != nil {
				return fmt.Errorf("binding %s: %w", redelivery, err)
			}
			return declareRetryTopology(ch, rs, retry.Delay, redelivery)
		}
	}
	l.consumer = newConsumer(b, spec)
	return l
}

func (l *EventListener) bindings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.patterns)
}

// Queue reports the listener's queue name: the configured one, or the
// broker-assigned name of a private queue once started.
func (l *EventListener) Queue() string {
	if q := l.consumer.queueName(); q != "" {
		return q
	}
	return l.queue
}

// Start declares the queue and its bindings and begins consuming. It is
// idempotent, and a start that failed can be retried.
func (l *EventListener) Start(ctx context.Context) error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return nil
	}
	l.started = true
	l.mu.Unlock()
	if err := l.consumer.start(ctx); err != nil {
		l.mu.Lock()
		l.started = false
		l.mu.Unlock()
		return fmt.Errorf("protobus: starting event listener %s: %w", l.queue, err)
	}
	if l.standalone {
		l.bus.mu.Lock()
		l.bus.listeners = append(l.bus.listeners, l)
		l.bus.mu.Unlock()
	}
	return nil
}

// StopConsuming stops taking new events, leaving the channel open so events
// in hand can settle; see Service.StopConsuming. Bus.Shutdown calls it.
func (l *EventListener) StopConsuming(context.Context) error {
	l.consumer.stopConsuming()
	return nil
}

// Close stops the listener and closes its channel. Handlers still running
// are cancelled; their events are redelivered. It is idempotent.
func (l *EventListener) Close() error {
	l.consumer.close()
	return nil
}

func (l *EventListener) add(ctx context.Context, pattern string, s *subscription, all bool) error {
	l.mu.Lock()
	if all {
		l.all = append(l.all, s)
	} else {
		l.router.Add(pattern, s)
	}
	fresh := !slices.Contains(l.patterns, pattern)
	if fresh {
		l.patterns = append(l.patterns, pattern)
	}
	startNow := l.lazy && !l.started
	started := l.started
	l.mu.Unlock()

	if startNow {
		return l.Start(ctx)
	}
	if fresh && started {
		if err := l.consumer.bind(pattern); err != nil {
			return fmt.Errorf("protobus: binding %s: %w", pattern, err)
		}
	}
	return nil
}

// Subscribe runs handler for every event of type T delivered on the
// listener's queue under the subscription's topic (default "EVENT.<T's full
// name>"). An event of another type matching the same topic is skipped by
// this handler.
//
// Return a HandledError to refuse an event deliberately: it is not retried,
// and goes to the dead-letter queue if the listener has retries. Any other
// error is retried when retries are on, and drops the event when they are
// not.
func Subscribe[T proto.Message](ctx context.Context, l *EventListener, handler func(context.Context, T, EventInfo) error, opts ...SubscribeOption) error {
	var zero T
	mt := zero.ProtoReflect().Type()
	return SubscribeType(ctx, l, mt, func(ctx context.Context, m proto.Message, info EventInfo) error {
		return handler(ctx, m.(T), info)
	}, opts...)
}

// SubscribeType is Subscribe for a message type known only at runtime, such as
// a dynamicpb type from protoload. handler receives messages of type mt.
func SubscribeType(ctx context.Context, l *EventListener, mt protoreflect.MessageType, handler func(context.Context, proto.Message, EventInfo) error, opts ...SubscribeOption) error {
	name := mt.Descriptor().FullName()
	o := subscribeOptions{topic: "EVENT." + string(name)}
	for _, opt := range opts {
		opt.applySubscribe(&o)
	}
	s := &subscription{typeName: name, handle: func(ctx context.Context, ev *wire.Event, info EventInfo) error {
		msg := mt.New().Interface()
		if err := unmarshal(ev.Data, msg); err != nil {
			return eventDecodeError(ev.Type, err)
		}
		return handler(ctx, msg, info)
	}}
	return l.add(ctx, o.topic, s, false)
}

// eventDecodeError marks an event that cannot be read. Like an undecodable
// request it fails the same way on every redelivery, so it is not retried:
// it goes to the dead-letter queue when the listener has one, and is dropped
// otherwise.
func eventDecodeError(typ string, err error) error {
	return fmt.Errorf("%w: %w", newProtocolError("event "+typ+" did not decode"), err)
}

// SubscribeAll runs handler for every event delivered to the listener,
// binding its queue to every topic ("#"). The event is decoded with the type
// it names, resolved in the bus's type registry; an event of a type the
// registry does not know is a protocol failure. It runs before any typed
// subscription matching the same event.
func (l *EventListener) SubscribeAll(ctx context.Context, handler func(context.Context, proto.Message, EventInfo) error) error {
	s := &subscription{handle: func(ctx context.Context, ev *wire.Event, info EventInfo) error {
		mt, err := l.bus.types.FindMessageByName(protoreflect.FullName(ev.Type))
		if err != nil {
			return eventDecodeError(ev.Type, fmt.Errorf("%w %q", ErrUnknownEventType, ev.Type))
		}
		msg := mt.New().Interface()
		if err := unmarshal(ev.Data, msg); err != nil {
			return eventDecodeError(ev.Type, err)
		}
		return handler(ctx, msg, info)
	}}
	return l.add(ctx, "#", s, true)
}

func (l *EventListener) handle(ctx context.Context, d *amqp.Delivery, _ *deliveryControl) handlerResult {
	ev, err := wire.DecodeEvent(d.Body)
	if err != nil {
		return handlerResult{err: eventDecodeError("(unknown)", err), handled: true}
	}
	// Prefer the key the broker routed on over the body's topic: the body is
	// publisher-controlled, and trusting it would let a publisher reach
	// handlers its routing key was never permitted to reach.
	matchOn := d.RoutingKey
	if matchOn == "" {
		matchOn = ev.Topic
	}
	info := EventInfo{
		Type: ev.Type, Topic: ev.Topic, RoutingKey: d.RoutingKey, MessageID: d.MessageId,
		CorrelationID: d.CorrelationId, Redelivered: d.Redelivered, Attempt: retryCount(d.Headers), Headers: copyHeaders(d.Headers),
	}

	l.mu.Lock()
	subs := slices.Clone(l.all)
	l.mu.Unlock()
	subs = l.router.Match(matchOn, subs)

	ran := false
	for _, s := range subs {
		if s.typeName != "" && string(s.typeName) != ev.Type {
			continue
		}
		ran = true
		if err := s.handle(ctx, &ev, info); err != nil {
			_, handled := AsHandled(err)
			l.bus.log.LogAttrs(ctx, slog.LevelError, "event handler failed", attrOperation("event"),
				attrQueue(l.Queue()), attrMessageType(ev.Type), attrRoutingKey(d.RoutingKey), attrError(err))
			return handlerResult{err: err, handled: handled}
		}
	}
	if !ran {
		// Type and key only: the payload is application data.
		l.bus.log.LogAttrs(ctx, slog.LevelWarn, "no handler for event", attrOperation("event"),
			attrQueue(l.Queue()), attrMessageType(ev.Type), attrRoutingKey(d.RoutingKey), attrOutcome(outcomeDropped))
	}
	return handlerResult{}
}
