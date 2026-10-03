package fakebroker

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/internal/transport"
)

// These tests pin the RabbitMQ behaviours protobus depends on. Each one is
// also exercised against a real broker by the integration suite, which is
// what keeps this fake honest.

func dial(t *testing.T, b *Broker) transport.Conn {
	t.Helper()
	c, err := b.Dial(context.Background(), "amqp://fake", amqp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func openCh(t *testing.T, c transport.Conn) transport.Channel {
	t.Helper()
	ch, err := c.Channel()
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func recv[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v, ok := <-c:
		if !ok {
			t.Fatal("channel closed")
		}
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
	var zero T
	return zero
}

func noRecv[T any](t *testing.T, c <-chan T, d time.Duration) {
	t.Helper()
	select {
	case v, ok := <-c:
		if ok {
			t.Fatalf("unexpected value %+v", v)
		}
	case <-time.After(d):
	}
}

func mustDeclareQueue(t *testing.T, ch transport.Channel, name string, durable, autoDelete, exclusive bool, args amqp.Table) string {
	t.Helper()
	q, err := ch.QueueDeclare(name, durable, autoDelete, exclusive, false, args)
	if err != nil {
		t.Fatal(err)
	}
	return q.Name
}

func publish(t *testing.T, ch transport.Channel, ex, key string, mandatory bool, msg amqp.Publishing) {
	t.Helper()
	if err := ch.PublishWithContext(context.Background(), ex, key, mandatory, false, msg); err != nil {
		t.Fatal(err)
	}
}

func TestTopicRoutingAndConsume(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	if err := ch.ExchangeDeclare("proto.bus", "topic", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	q := mustDeclareQueue(t, ch, "Calc.Service", true, false, false, nil)
	if err := ch.QueueBind(q, "REQUEST.Calc.Service.*", "proto.bus", false, nil); err != nil {
		t.Fatal(err)
	}
	ds, err := ch.ConsumeWithContext(context.Background(), q, "c1", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	publish(t, ch, "proto.bus", "REQUEST.Calc.Service.add", false, amqp.Publishing{Body: []byte("x"), CorrelationId: "c"})
	publish(t, ch, "proto.bus", "REQUEST.Calc.Service.add.extra", false, amqp.Publishing{Body: []byte("y")})
	d := recv(t, ds)
	if string(d.Body) != "x" || d.RoutingKey != "REQUEST.Calc.Service.add" || d.Exchange != "proto.bus" || d.CorrelationId != "c" {
		t.Fatalf("unexpected delivery %+v", d)
	}
	if err := d.Ack(false); err != nil {
		t.Fatal(err)
	}
	noRecv(t, ds, 50*time.Millisecond) // the four-word key matched nothing
}

func TestServerNamedExclusiveQueueAndDirectExchange(t *testing.T) {
	b := New()
	c := dial(t, b)
	ch := openCh(t, c)
	if err := ch.ExchangeDeclare("proto.bus.callback", "direct", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	q := mustDeclareQueue(t, ch, "", false, true, true, nil)
	if len(q) < len("amq.gen-") || q[:8] != "amq.gen-" {
		t.Fatalf("server-named queue %q", q)
	}
	if err := ch.QueueBind(q, q, "proto.bus.callback", false, nil); err != nil {
		t.Fatal(err)
	}
	// Another connection may not touch an exclusive queue.
	other := openCh(t, dial(t, b))
	if _, err := other.QueueDeclare(q, false, true, true, false, nil); !isCode(err, amqp.ResourceLocked) {
		t.Fatalf("want RESOURCE_LOCKED, got %v", err)
	}
	// The exclusive queue goes away with its connection.
	_ = c.Close()
	if b.HasQueue(q) {
		t.Fatal("exclusive queue must be deleted with its connection")
	}
}

func isCode(err error, code int) bool {
	var ae *amqp.Error
	return errors.As(err, &ae) && ae.Code == code
}

func TestRedeclareWithDifferentArgsClosesTheChannel(t *testing.T) {
	b := New()
	c := dial(t, b)
	ch := openCh(t, c)
	mustDeclareQueue(t, ch, "Svc.Retry", true, false, false, amqp.Table{"x-message-ttl": int32(5000)})
	// Integer width does not matter to RabbitMQ's equivalence check.
	ch2 := openCh(t, c)
	if _, err := ch2.QueueDeclare("Svc.Retry", true, false, false, false, amqp.Table{"x-message-ttl": int64(5000)}); err != nil {
		t.Fatalf("same TTL with another integer width must be equivalent: %v", err)
	}
	closed := ch2.NotifyClose(make(chan *amqp.Error, 1))
	_, err := ch2.QueueDeclare("Svc.Retry", true, false, false, false, amqp.Table{"x-message-ttl": int32(1000)})
	if !isCode(err, amqp.PreconditionFailed) {
		t.Fatalf("want PRECONDITION_FAILED, got %v", err)
	}
	if e := recv(t, closed); e == nil || e.Code != amqp.PreconditionFailed {
		t.Fatalf("channel must close with 406, got %v", e)
	}
	if !ch2.IsClosed() {
		t.Fatal("channel must report closed")
	}
}

func TestPublishToMissingExchangeClosesTheChannel(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	publish(t, ch, "nope", "k", false, amqp.Publishing{})
	if e := recv(t, closed); e == nil || e.Code != amqp.NotFound {
		t.Fatalf("want 404 channel close, got %v", e)
	}
	if err := ch.PublishWithContext(context.Background(), "", "k", false, false, amqp.Publishing{}); !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("publishing on a closed channel must fail with ErrClosed, got %v", err)
	}
}

func TestConfirmsAndMandatoryReturnArriveInOrder(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 8))
	returns := ch.NotifyReturn(make(chan amqp.Return))
	if err := ch.ExchangeDeclare("ex", "topic", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	mustDeclareQueue(t, ch, "q", true, false, false, nil)
	if err := ch.QueueBind("q", "k", "ex", false, nil); err != nil {
		t.Fatal(err)
	}

	if ch.GetNextPublishSeqNo() != 1 {
		t.Fatal("sequence numbers start at 1")
	}
	publish(t, ch, "ex", "k", true, amqp.Publishing{MessageId: "routed"})
	publish(t, ch, "ex", "nowhere", true, amqp.Publishing{MessageId: "lost"})
	publish(t, ch, "ex", "nowhere", false, amqp.Publishing{MessageId: "dropped"})

	if c := recv(t, confirms); c.DeliveryTag != 1 || !c.Ack {
		t.Fatalf("first confirm %+v", c)
	}
	// RabbitMQ sends basic.return BEFORE the ack for the same message.
	r := recv(t, returns)
	if r.MessageId != "lost" || r.ReplyCode != amqp.NoRoute {
		t.Fatalf("unexpected return %+v", r)
	}
	if c := recv(t, confirms); c.DeliveryTag != 2 || !c.Ack {
		t.Fatalf("a returned message is still acked: %+v", c)
	}
	if c := recv(t, confirms); c.DeliveryTag != 3 || !c.Ack {
		t.Fatalf("third confirm %+v", c)
	}
}

func TestConfirmFaultInjection(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	_ = ch.Confirm(false)
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 8))
	mustDeclareQueue(t, ch, "q", true, false, false, nil)

	b.SetConfirmPolicy(func(p Published) ConfirmAction {
		switch p.Msg.MessageId {
		case "nack":
			return Nack
		case "drop":
			return Drop
		}
		return Ack
	})
	publish(t, ch, "", "q", false, amqp.Publishing{MessageId: "nack"})
	publish(t, ch, "", "q", false, amqp.Publishing{MessageId: "drop"})
	publish(t, ch, "", "q", false, amqp.Publishing{MessageId: "ok"})
	if c := recv(t, confirms); c.DeliveryTag != 1 || c.Ack {
		t.Fatalf("want nack, got %+v", c)
	}
	if c := recv(t, confirms); c.DeliveryTag != 3 || !c.Ack {
		t.Fatalf("dropped confirm must be skipped, got %+v", c)
	}
	if n := b.QueueDepth("q"); n != 1 {
		t.Fatalf("a nacked or dropped publish is not enqueued; depth %d", n)
	}
}

func TestPrefetchBoundsUnackedDeliveries(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	mustDeclareQueue(t, ch, "q", true, false, false, nil)
	if err := ch.Qos(2, 0, false); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		publish(t, ch, "", "q", false, amqp.Publishing{Body: []byte("m")})
	}
	ds, _ := ch.ConsumeWithContext(context.Background(), "q", "c", false, false, false, false, nil)
	d1 := recv(t, ds)
	d2 := recv(t, ds)
	noRecv(t, ds, 50*time.Millisecond)
	_ = d1.Ack(false)
	d3 := recv(t, ds)
	_ = d2.Ack(false)
	_ = d3.Ack(false)
	recv(t, ds)
	recv(t, ds)
}

func TestRejectAndRequeue(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	mustDeclareQueue(t, ch, "q", true, false, false, nil)
	publish(t, ch, "", "q", false, amqp.Publishing{Body: []byte("a")})
	ds, _ := ch.ConsumeWithContext(context.Background(), "q", "c", false, false, false, false, nil)
	d := recv(t, ds)
	if d.Redelivered {
		t.Fatal("first delivery is not redelivered")
	}
	_ = d.Nack(false, true)
	d = recv(t, ds)
	if !d.Redelivered {
		t.Fatal("a requeued message comes back redelivered")
	}
	_ = d.Reject(false)
	noRecv(t, ds, 50*time.Millisecond)
	if b.QueueDepth("q") != 0 {
		t.Fatal("reject without requeue drops the message")
	}
}

func TestUnknownDeliveryTagClosesTheChannel(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	mustDeclareQueue(t, ch, "q", true, false, false, nil)
	publish(t, ch, "", "q", false, amqp.Publishing{})
	ds, _ := ch.ConsumeWithContext(context.Background(), "q", "c", false, false, false, false, nil)
	d := recv(t, ds)
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	_ = d.Ack(false)
	_ = d.Ack(false) // double ack
	if e := recv(t, closed); e == nil || e.Code != amqp.PreconditionFailed {
		t.Fatalf("double ack must close the channel with 406, got %v", e)
	}
}

func TestClosingAChannelRequeuesUnacked(t *testing.T) {
	b := New()
	c := dial(t, b)
	ch := openCh(t, c)
	mustDeclareQueue(t, ch, "q", true, false, false, nil)
	publish(t, ch, "", "q", false, amqp.Publishing{Body: []byte("a")})
	ds, _ := ch.ConsumeWithContext(context.Background(), "q", "c", false, false, false, false, nil)
	recv(t, ds)
	_ = ch.Close()
	if _, ok := <-ds; ok {
		t.Fatal("deliveries channel must close with its channel")
	}
	ch2 := openCh(t, c)
	ds2, _ := ch2.ConsumeWithContext(context.Background(), "q", "c2", false, false, false, false, nil)
	if d := recv(t, ds2); !d.Redelivered || string(d.Body) != "a" {
		t.Fatalf("unacked message must be redelivered, got %+v", d)
	}
}

func TestTTLDeadLettersWithTheOriginalRoutingKey(t *testing.T) {
	// The retry ladder depends on this: a message parked on <svc>.Retry
	// expires and is dead-lettered to proto.bus under its OWN routing key.
	b := New()
	ch := openCh(t, dial(t, b))
	_ = ch.ExchangeDeclare("proto.bus", "topic", true, false, false, false, nil)
	_ = ch.ExchangeDeclare("Svc.Retry.Exchange", "topic", true, false, false, false, nil)
	mustDeclareQueue(t, ch, "Svc", true, false, false, nil)
	_ = ch.QueueBind("Svc", "REQUEST.Svc.*", "proto.bus", false, nil)
	mustDeclareQueue(t, ch, "Svc.Retry", true, false, false, amqp.Table{
		"x-message-ttl": int32(30), "x-dead-letter-exchange": "proto.bus",
	})
	_ = ch.QueueBind("Svc.Retry", "#", "Svc.Retry.Exchange", false, nil)

	ds, _ := ch.ConsumeWithContext(context.Background(), "Svc", "c", false, false, false, false, nil)
	publish(t, ch, "Svc.Retry.Exchange", "REQUEST.Svc.add", false, amqp.Publishing{
		Body: []byte("again"), Headers: amqp.Table{"x-retry-count": int32(1)},
	})
	start := time.Now()
	d := recv(t, ds)
	if time.Since(start) < 25*time.Millisecond {
		t.Fatal("message must wait out the TTL")
	}
	if d.RoutingKey != "REQUEST.Svc.add" || d.Headers["x-retry-count"] != int32(1) {
		t.Fatalf("unexpected redelivery %+v", d)
	}
	if _, ok := d.Headers["x-death"]; !ok {
		t.Fatal("the broker adds x-death on dead-lettering")
	}
}

func TestPriorityQueueOrdersWaitingMessages(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	mustDeclareQueue(t, ch, "q", true, false, false, amqp.Table{"x-max-priority": int8(2)})
	publish(t, ch, "", "q", false, amqp.Publishing{Body: []byte("low")})
	publish(t, ch, "", "q", false, amqp.Publishing{Body: []byte("high"), Priority: 9}) // clamped to 2
	publish(t, ch, "", "q", false, amqp.Publishing{Body: []byte("mid"), Priority: 1})
	ds, _ := ch.ConsumeWithContext(context.Background(), "q", "c", true, false, false, false, nil)
	for _, want := range []string{"high", "mid", "low"} {
		if got := string(recv(t, ds).Body); got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	}
}

func TestFanoutReachesEveryBoundQueue(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	_ = ch.ExchangeDeclare("proto.bus.cancel", "fanout", true, false, false, false, nil)
	q1 := mustDeclareQueue(t, ch, "", false, true, true, nil)
	q2 := mustDeclareQueue(t, ch, "", false, true, true, nil)
	_ = ch.QueueBind(q1, "", "proto.bus.cancel", false, nil)
	_ = ch.QueueBind(q2, "", "proto.bus.cancel", false, nil)
	publish(t, ch, "proto.bus.cancel", "ignored", false, amqp.Publishing{CorrelationId: "s"})
	if b.QueueDepth(q1) != 1 || b.QueueDepth(q2) != 1 {
		t.Fatal("fanout must copy to every bound queue")
	}
}

func TestKillConnectionsSimulatesABrokerOutage(t *testing.T) {
	b := New()
	c := dial(t, b)
	ch := openCh(t, c)
	connClosed := c.NotifyClose(make(chan *amqp.Error, 1))
	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))
	b.KillConnections()
	if e := recv(t, connClosed); e == nil || e.Code != amqp.ConnectionForced {
		t.Fatalf("connection must close with 320, got %v", e)
	}
	if e := recv(t, chClosed); e == nil {
		t.Fatal("channels close with their connection")
	}
	if !c.IsClosed() {
		t.Fatal("connection must report closed")
	}
	if _, err := c.Channel(); err == nil {
		t.Fatal("a closed connection opens no channels")
	}
}

func TestDialFaults(t *testing.T) {
	b := New()
	b.SetDialFault(errors.New("connection refused"))
	if _, err := b.Dial(context.Background(), "x", amqp.Config{}); err == nil {
		t.Fatal("dial fault must fail the dial")
	}
	b.SetDialFault(nil)
	dial(t, b)
	if b.Dials() != 2 {
		t.Fatalf("dial count %d", b.Dials())
	}
}

func TestOperationLogRecordsSettlementOrder(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	_ = ch.Confirm(false)
	mustDeclareQueue(t, ch, "q", true, false, false, nil)
	mustDeclareQueue(t, ch, "reply", true, false, false, nil)
	publish(t, ch, "", "q", false, amqp.Publishing{MessageId: "req"})
	ds, _ := ch.ConsumeWithContext(context.Background(), "q", "c", false, false, false, false, nil)
	d := recv(t, ds)
	publish(t, ch, "", "reply", false, amqp.Publishing{MessageId: "rep"})
	_ = d.Ack(false)
	ops := b.Ops()
	var kinds []string
	for _, op := range ops {
		if op.Kind == "deliver" {
			continue
		}
		kinds = append(kinds, op.Kind+":"+op.MessageID)
	}
	want := []string{"publish:req", "publish:rep", "ack:req"}
	if len(kinds) != len(want) {
		t.Fatalf("ops %v", kinds)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("ops %v, want %v", kinds, want)
		}
	}
}

func TestCancelConsumerClosesItsDeliveries(t *testing.T) {
	b := New()
	ch := openCh(t, dial(t, b))
	mustDeclareQueue(t, ch, "q", true, false, false, nil)
	ds, _ := ch.ConsumeWithContext(context.Background(), "q", "c", false, false, false, false, nil)
	if err := ch.Cancel("c", false); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-ds; ok {
		t.Fatal("cancel must close the deliveries channel")
	}
	publish(t, ch, "", "q", false, amqp.Publishing{})
	if b.QueueDepth("q") != 1 {
		t.Fatal("a cancelled consumer receives nothing")
	}
}
