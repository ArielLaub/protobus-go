package protobus

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

type received[T proto.Message] struct {
	mu   sync.Mutex
	msgs []T
	info []EventInfo
	ch   chan struct{}
}

func newReceived[T proto.Message]() *received[T] { return &received[T]{ch: make(chan struct{}, 64)} }

func (r *received[T]) handler(err func(T) error) func(context.Context, T, EventInfo) error {
	return func(_ context.Context, m T, info EventInfo) error {
		r.mu.Lock()
		r.msgs = append(r.msgs, m)
		r.info = append(r.info, info)
		r.mu.Unlock()
		r.ch <- struct{}{}
		if err != nil {
			return err(m)
		}
		return nil
	}
}

func (r *received[T]) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func TestEventPublishAndSubscribe(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	svc, _ := bus.Register(&calcServiceDesc, newCalcImpl())
	got := newReceived[*testpb.OrderCreated]()
	if err := Subscribe(testCtx(t), svc.Events(), got.handler(nil)); err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if err := bus.PublishEvent(testCtx(t), &testpb.OrderCreated{Id: "o1", Amount: pbtypes.BigintFromUint64(7)}, WithMessageID("evt-1")); err != nil {
		t.Fatal(err)
	}
	recvWithin(t, got.ch, 2*time.Second)
	m, info := got.msgs[0], got.info[0]
	if m.Id != "o1" || info.Type != "Test.OrderCreated" || info.Topic != "EVENT.Test.OrderCreated" ||
		info.RoutingKey != "EVENT.Test.OrderCreated" || info.MessageID != "evt-1" {
		t.Fatalf("event %v %+v", m, info)
	}
	if q, ok := b.Queue("Test.Calc.Events"); !ok || !q.Durable || len(q.Args) != 0 {
		t.Fatalf("event queue %+v", q)
	}
	pub := b.OpsOf("publish")
	var ev fakebroker.Op
	for _, op := range pub {
		if op.Exchange == "proto.bus.events" {
			ev = op
		}
	}
	if ev.Mandatory || ev.Msg.DeliveryMode != amqp.Persistent || ev.Msg.ContentType != contentTypeOctetStream || ev.Msg.CorrelationId == "" {
		t.Fatalf("event publish properties %+v", ev)
	}
	env, err := wire.DecodeEvent(ev.Msg.Body)
	if err != nil || env.Type != "Test.OrderCreated" || env.Topic != "EVENT.Test.OrderCreated" {
		t.Fatalf("envelope %+v %v", env, err)
	}
}

func TestEventWithoutSubscribersIsNotAnError(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	if err := bus.PublishEvent(testCtx(t), &testpb.Ping{Id: "p"}); err != nil {
		t.Fatal(err)
	}
}

func TestEventWildcardsAndOrder(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, err := bus.NewEventListener("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	var mu sync.Mutex
	var order []string
	done := make(chan struct{}, 8)
	add := func(name, pattern string) {
		err := Subscribe(testCtx(t), l, func(_ context.Context, p *testpb.Ping, _ EventInfo) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			done <- struct{}{}
			return nil
		}, WithTopic(pattern))
		if err != nil {
			t.Fatal(err)
		}
	}
	add("star", "ORDERS.*.CREATED")
	add("hash", "ORDERS.#")
	add("other", "INVOICES.#")
	if err := l.Start(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if err := bus.PublishEvent(testCtx(t), &testpb.Ping{}, WithTopic("ORDERS.US.CREATED")); err != nil {
		t.Fatal(err)
	}
	recvWithin(t, done, time.Second)
	recvWithin(t, done, time.Second)
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(order, []string{"star", "hash"}) {
		t.Fatalf("handlers ran %v; want subscription order, matching ones only", order)
	}
	if q, _ := b.Queue(l.Queue()); !q.Exclusive || !q.AutoDelete || q.Durable {
		t.Fatalf("an unnamed listener has a private queue: %+v", q)
	}
}

func TestEventMatchesTheDeliveredRoutingKeyNotTheBodyTopic(t *testing.T) {
	// A publisher may only be permitted to publish under one topic; the body
	// topic must not let it reach handlers of another.
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("")
	t.Cleanup(func() { _ = l.Close() })
	public := newReceived[*testpb.Ping]()
	admin := newReceived[*testpb.Ping]()
	_ = Subscribe(testCtx(t), l, public.handler(nil), WithTopic("PUBLIC.#"))
	_ = Subscribe(testCtx(t), l, admin.handler(nil), WithTopic("ADMIN.#"))
	_ = l.Start(testCtx(t))

	data, _ := proto.Marshal(&testpb.Ping{Id: "x"})
	body := wire.AppendEvent(nil, wire.Event{Type: "Test.Ping", Topic: "ADMIN.reset", Data: data})
	if err := b.Publish("proto.bus.events", "PUBLIC.hello", amqp.Publishing{Body: body}); err != nil {
		t.Fatal(err)
	}
	recvWithin(t, public.ch, time.Second)
	time.Sleep(20 * time.Millisecond)
	if admin.count() != 0 {
		t.Fatal("the body topic routed an event to a handler its routing key never reached")
	}
	if public.info[0].Topic != "ADMIN.reset" || public.info[0].RoutingKey != "PUBLIC.hello" {
		t.Fatalf("info %+v", public.info[0])
	}
}

func TestEventTypeMismatchIsSkipped(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("")
	t.Cleanup(func() { _ = l.Close() })
	pings := newReceived[*testpb.Ping]()
	orders := newReceived[*testpb.OrderCreated]()
	_ = Subscribe(testCtx(t), l, pings.handler(nil), WithTopic("shared"))
	_ = Subscribe(testCtx(t), l, orders.handler(nil), WithTopic("shared"))
	_ = l.Start(testCtx(t))
	_ = bus.PublishEvent(testCtx(t), &testpb.Ping{Id: "p"}, WithTopic("shared"))
	recvWithin(t, pings.ch, time.Second)
	time.Sleep(20 * time.Millisecond)
	if orders.count() != 0 {
		t.Fatal("a handler for another type must not see the event")
	}
}

func TestEventSubscribeAll(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("")
	t.Cleanup(func() { _ = l.Close() })
	got := make(chan proto.Message, 4)
	err := l.SubscribeAll(testCtx(t), func(_ context.Context, m proto.Message, _ EventInfo) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Start(testCtx(t))
	if keys := b.Bindings("proto.bus.events", l.Queue()); !slices.Equal(keys, []string{"#"}) {
		t.Fatalf("bindings %v", keys)
	}
	_ = bus.PublishEvent(testCtx(t), &testpb.OrderCreated{Id: "a"}, WithTopic("anything.at.all"))
	m := recvWithin(t, got, time.Second)
	if oc, ok := m.(*testpb.OrderCreated); !ok || oc.Id != "a" {
		t.Fatalf("decoded with the registry: %T %v", m, m)
	}
}

func TestEventSubscribeAfterStartBindsImmediately(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	svc := startCalc(t, bus, newCalcImpl())
	got := newReceived[*testpb.Ping]()
	if err := Subscribe(testCtx(t), svc.Events(), got.handler(nil)); err != nil {
		t.Fatal(err)
	}
	_ = bus.PublishEvent(testCtx(t), &testpb.Ping{Id: "late"})
	recvWithin(t, got.ch, 2*time.Second)
}

func TestEventFailureWithoutRetryDropsAndKeepsConsuming(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("Sub.Events")
	t.Cleanup(func() { _ = l.Close() })
	got := newReceived[*testpb.Ping]()
	_ = Subscribe(testCtx(t), l, got.handler(func(p *testpb.Ping) error {
		if p.Id == "bad" {
			return errors.New("cannot handle")
		}
		return nil
	}))
	_ = l.Start(testCtx(t))
	_ = bus.PublishEvent(testCtx(t), &testpb.Ping{Id: "bad"})
	_ = bus.PublishEvent(testCtx(t), &testpb.Ping{Id: "good"})
	recvWithin(t, got.ch, time.Second)
	recvWithin(t, got.ch, time.Second)
	eventually(t, "settled", func() bool { return len(b.OpsOf("reject")) == 1 && len(b.OpsOf("ack")) == 1 })
	if b.QueueDepth("Sub.Events") != 0 || b.HasQueue("Sub.Events.DLQ") {
		t.Fatal("without retries a failed event is dropped, and there is no DLQ")
	}
}

func TestEventRetryRedeliversOnlyToTheFailingSubscriber(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())

	// Two services subscribe to the same event; only the first fails, once.
	failing, _ := bus.Register(&calcServiceDesc, newCalcImpl(), WithEventRetry(EventRetryPolicy{MaxRetries: 3, Delay: 20 * time.Millisecond}))
	var attempts atomic.Int32
	flaky := newReceived[*testpb.OrderCreated]()
	_ = Subscribe(testCtx(t), failing.Events(), flaky.handler(func(*testpb.OrderCreated) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	}))
	if err := failing.Start(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	other, _ := bus.NewEventListener("Other.Events")
	t.Cleanup(func() { _ = other.Close() })
	steady := newReceived[*testpb.OrderCreated]()
	_ = Subscribe(testCtx(t), other, steady.handler(nil))
	_ = other.Start(testCtx(t))

	_ = bus.PublishEvent(testCtx(t), &testpb.OrderCreated{Id: "o"}, WithMessageID("evt-7"))
	recvWithin(t, flaky.ch, time.Second)
	recvWithin(t, flaky.ch, time.Second) // redelivered after the retry delay
	recvWithin(t, steady.ch, time.Second)
	time.Sleep(60 * time.Millisecond)
	if steady.count() != 1 {
		t.Fatalf("the redelivery reached a subscriber that had succeeded (%d)", steady.count())
	}
	if flaky.info[1].MessageID != "evt-7" || flaky.info[1].Attempt != 1 || flaky.info[1].RoutingKey != "EVENT.Test.OrderCreated" {
		t.Fatalf("redelivery info %+v", flaky.info[1])
	}
	for name, kind := range map[string]string{"Test.Calc.Events.Redelivery": "topic", "Test.Calc.Events.Retry.Exchange": "topic"} {
		if b.ExchangeKind(name) != kind {
			t.Errorf("exchange %s", name)
		}
	}
	retry, _ := b.Queue("Test.Calc.Events.Retry")
	if retry.Args["x-dead-letter-exchange"] != "Test.Calc.Events.Redelivery" {
		t.Fatalf("retry queue dead-letters to %v", retry.Args["x-dead-letter-exchange"])
	}
}

func TestEventRetryDeadLettersAfterMaxRetries(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, err := bus.NewEventListener("Sub.Events", WithEventRetry(EventRetryPolicy{MaxRetries: 2, Delay: 10 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	got := newReceived[*testpb.Ping]()
	_ = Subscribe(testCtx(t), l, got.handler(func(*testpb.Ping) error { return errors.New("always") }))
	_ = l.Start(testCtx(t))
	_ = bus.PublishEvent(testCtx(t), &testpb.Ping{Id: "doomed"})
	eventually(t, "dead-lettered", func() bool { return b.QueueDepth("Sub.Events.DLQ") == 1 })
	if got.count() != 3 {
		t.Fatalf("handler ran %d times, want 1 + 2 retries", got.count())
	}
	dl := b.Messages("Sub.Events.DLQ")[0]
	if dl.Headers[headerOriginalQueue] != "Sub.Events" || dl.Headers[headerLastError] != "Error" {
		t.Fatalf("DLQ metadata %v", dl.Headers)
	}
}

func TestEventHandledErrorGoesStraightToTheDLQWhenRetryIsOn(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("Sub.Events", WithEventRetry(EventRetryPolicy{MaxRetries: 5, Delay: 10 * time.Millisecond}))
	t.Cleanup(func() { _ = l.Close() })
	got := newReceived[*testpb.Ping]()
	_ = Subscribe(testCtx(t), l, got.handler(func(*testpb.Ping) error { return NewHandledError("POISON", "refused") }))
	_ = l.Start(testCtx(t))
	_ = bus.PublishEvent(testCtx(t), &testpb.Ping{Id: "poison"})
	eventually(t, "dead-lettered", func() bool { return b.QueueDepth("Sub.Events.DLQ") == 1 })
	if got.count() != 1 {
		t.Fatalf("a handled refusal is not retried; ran %d times", got.count())
	}
	if h := b.Messages("Sub.Events.DLQ")[0].Headers[headerLastError]; h != "HandledError[POISON]: refused" {
		t.Fatalf("x-last-error %v", h)
	}
}

func TestEventRetryNeedsANamedQueue(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	if _, err := bus.NewEventListener("", WithEventRetry(EventRetryPolicy{MaxRetries: 1, Delay: time.Second})); err == nil {
		t.Fatal("expected an error")
	}
}

func TestEventListenerHandlesInParallel(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("Par.Events", WithEventConcurrency(4))
	t.Cleanup(func() { _ = l.Close() })
	var running, peak atomic.Int32
	done := make(chan struct{}, 8)
	_ = Subscribe(testCtx(t), l, func(context.Context, *testpb.Ping, EventInfo) error {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(40 * time.Millisecond)
		running.Add(-1)
		done <- struct{}{}
		return nil
	})
	_ = l.Start(testCtx(t))
	for range 8 {
		_ = bus.PublishEvent(testCtx(t), &testpb.Ping{})
	}
	for range 8 {
		recvWithin(t, done, 2*time.Second)
	}
	if peak.Load() != 4 {
		t.Fatalf("peak concurrency %d, want 4", peak.Load())
	}
}
