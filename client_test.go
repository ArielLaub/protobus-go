package protobus

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
	"github.com/ArielLaub/protobus-go/v2/internal/transport"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
)

func dialTest(t *testing.T, b *fakebroker.Broker, cfg Config, opts ...Option) *Bus {
	t.Helper()
	opts = append([]Option{WithConfig(cfg), WithLogger(testLogger(t)), withDialer(b.Dial)}, opts...)
	bus, err := Dial(testCtx(t), "amqp://guest:guest@fake/", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return bus
}

// responder is a raw service written against the wire protocol, standing in
// for a TypeScript or Python peer.
type responder struct {
	t     *testing.T
	conn  transport.Conn
	ch    transport.Channel
	mu    sync.Mutex
	seen  []amqp.Delivery
	reply func(d amqp.Delivery, req wire.Request) []amqp.Publishing
}

func newResponder(t *testing.T, b *fakebroker.Broker, queue, binding string, reply func(amqp.Delivery, wire.Request) []amqp.Publishing) *responder {
	t.Helper()
	conn := b.DialPeer()
	ch, _ := conn.Channel()
	_ = ch.ExchangeDeclare("proto.bus", "topic", true, false, false, false, nil)
	_ = ch.ExchangeDeclare("proto.bus.callback", "direct", true, false, false, false, nil)
	_, _ = ch.QueueDeclare(queue, true, false, false, false, nil)
	_ = ch.QueueBind(queue, binding, "proto.bus", false, nil)
	ds, err := ch.ConsumeWithContext(context.Background(), queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := &responder{t: t, conn: conn, ch: ch, reply: reply}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for d := range ds {
			req, _ := wire.DecodeRequest(d.Body)
			r.mu.Lock()
			r.seen = append(r.seen, d)
			r.mu.Unlock()
			for _, p := range r.reply(d, req) {
				_ = ch.PublishWithContext(context.Background(), "proto.bus.callback", d.ReplyTo, false, false, p)
			}
			_ = d.Ack(false)
		}
	}()
	t.Cleanup(func() { _ = conn.Close(); <-done })
	return r
}

func (r *responder) deliveries() []amqp.Delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]amqp.Delivery(nil), r.seen...)
}

func resultReply(d amqp.Delivery, method string, m proto.Message) amqp.Publishing {
	data, _ := proto.Marshal(m)
	body, _ := wire.AppendResponse(nil, wire.Response{Result: &wire.Result{Method: method, Data: data}})
	return amqp.Publishing{ContentType: contentTypeOctetStream, CorrelationId: d.CorrelationId, Body: body}
}

func errorReply(d amqp.Delivery, method, code, msg string) amqp.Publishing {
	body, _ := wire.AppendResponse(nil, wire.Response{Error: &wire.Error{Method: method, Code: code, Message: msg}})
	return amqp.Publishing{ContentType: contentTypeOctetStream, CorrelationId: d.CorrelationId, Body: body}
}

func adder(d amqp.Delivery, req wire.Request) []amqp.Publishing {
	var in testpb.AddRequest
	_ = proto.Unmarshal(req.Data, &in)
	return []amqp.Publishing{resultReply(d, req.Method, &testpb.AddResponse{Sum: in.A + in.B})}
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestInvokeRoundTripAndWireProperties(t *testing.T) {
	b := fakebroker.New()
	r := newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", adder)
	bus := dialTest(t, b, fastConfig())
	c := NewClient(bus, "Test.Calc")

	var out testpb.AddResponse
	if err := c.Invoke(testCtx(t), "add", &testpb.AddRequest{A: 2, B: 3}, &out, WithActor("user:7")); err != nil {
		t.Fatal(err)
	}
	if out.Sum != 5 {
		t.Fatalf("sum %d", out.Sum)
	}

	d := r.deliveries()[0]
	req, _ := wire.DecodeRequest(d.Body)
	if req.Method != "Test.Calc.add" || req.Actor != "user:7" {
		t.Fatalf("envelope %+v", req)
	}
	if d.RoutingKey != "REQUEST.Test.Calc.add" || d.Exchange != "proto.bus" {
		t.Fatalf("routed via %s/%s", d.Exchange, d.RoutingKey)
	}
	if d.ContentType != "application/octet-stream" || d.DeliveryMode != amqp.Persistent {
		t.Fatalf("properties %q / %d", d.ContentType, d.DeliveryMode)
	}
	if !uuidV4.MatchString(d.CorrelationId) || !uuidV4.MatchString(d.MessageId) || d.CorrelationId == d.MessageId {
		t.Fatalf("ids %q %q", d.CorrelationId, d.MessageId)
	}
	if !strings.HasPrefix(d.ReplyTo, "amq.gen-") || d.Priority != 0 {
		t.Fatalf("replyTo %q priority %d", d.ReplyTo, d.Priority)
	}
	if b.Bindings("proto.bus.callback", d.ReplyTo)[0] != d.ReplyTo {
		t.Fatal("the reply queue is bound to the callbacks exchange under its own name")
	}
	publishes := b.OpsOf("publish")
	if !publishes[0].Mandatory {
		t.Fatal("an RPC request is published mandatory, so an unbound service fails fast")
	}
}

func TestDialDeclaresTheExchangesItPublishesTo(t *testing.T) {
	b := fakebroker.New()
	dialTest(t, b, fastConfig())
	for name, kind := range map[string]string{
		"proto.bus": "topic", "proto.bus.callback": "direct", "proto.bus.events": "topic", "proto.bus.cancel": "fanout",
	} {
		if got := b.ExchangeKind(name); got != kind {
			t.Errorf("exchange %s is %q, want %q", name, got, kind)
		}
	}
}

func TestInvokeReturnsTheRemoteError(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", func(d amqp.Delivery, req wire.Request) []amqp.Publishing {
		return []amqp.Publishing{errorReply(d, req.Method, "NOT_FOUND", "no such account")}
	})
	bus := dialTest(t, b, fastConfig())
	err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{})
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != "NOT_FOUND" || re.Message != "no such account" || re.Method != "Test.Calc.add" {
		t.Fatalf("got %#v", err)
	}
}

func TestInvokeRefusesAReplyForAnotherMethod(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", func(d amqp.Delivery, req wire.Request) []amqp.Publishing {
		return []amqp.Publishing{resultReply(d, "Test.Other.add", &testpb.AddResponse{Sum: 1})}
	})
	bus := dialTest(t, b, fastConfig())
	err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{})
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("got %v", err)
	}
}

func TestInvokeFailsFastWhenNoServiceIsBound(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	start := time.Now()
	err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{})
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("an unroutable request must not wait out the RPC timeout")
	}
	if n := len(bus.dispatcher.calls); n != 0 {
		t.Fatalf("pending call leaked: %d", n)
	}
}

func TestInvokeTimesOut(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", func(amqp.Delivery, wire.Request) []amqp.Publishing { return nil })
	bus := dialTest(t, b, fastConfig())
	err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{}, WithTimeout(30*time.Millisecond))
	if !errors.Is(err, ErrRPCTimeout) || !errors.Is(err, context.DeadlineExceeded) || !IsCode(err, CodeRPCTimeout) {
		t.Fatalf("got %v", err)
	}
	bus.dispatcher.mu.Lock()
	defer bus.dispatcher.mu.Unlock()
	if len(bus.dispatcher.calls) != 0 {
		t.Fatal("a timed-out call must not leak its entry")
	}
}

func TestInvokeHonoursAnEarlierContextDeadline(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", func(amqp.Delivery, wire.Request) []amqp.Publishing { return nil })
	bus := dialTest(t, b, fastConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := NewClient(bus, "Test.Calc").Invoke(ctx, "add", &testpb.AddRequest{}, &testpb.AddResponse{})
	if !errors.Is(err, ErrRPCTimeout) || time.Since(start) > time.Second {
		t.Fatalf("got %v after %v", err, time.Since(start))
	}
}

func TestInvokeCancelledContext(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", func(amqp.Delivery, wire.Request) []amqp.Publishing { return nil })
	bus := dialTest(t, b, fastConfig())
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	err := NewClient(bus, "Test.Calc").Invoke(ctx, "add", &testpb.AddRequest{}, &testpb.AddResponse{})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrRPCTimeout) {
		t.Fatalf("got %v", err)
	}
}

func TestInvokeCallOptions(t *testing.T) {
	b := fakebroker.New()
	r := newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", adder)
	bus := dialTest(t, b, fastConfig())
	c := NewClient(bus, "Test.Calc")
	if err := c.Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{}, WithPriority(PriorityHigh), WithMessageID("order-42")); err != nil {
		t.Fatal(err)
	}
	d := r.deliveries()[0]
	if d.Priority != 1 || d.MessageId != "order-42" {
		t.Fatalf("priority %d messageId %q", d.Priority, d.MessageId)
	}
}

func TestInvokeRejectsAnInvalidMessageIDBeforeAnyIO(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	c := NewClient(bus, "Test.Calc")
	before := len(b.OpsOf("publish"))
	for _, id := range []string{"", "   ", strings.Repeat("x", 256), strings.Repeat("é", 128)} {
		err := c.Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{}, WithMessageID(id))
		if !errors.Is(err, ErrInvalidMessageID) {
			t.Errorf("id %q: got %v", id, err)
		}
	}
	if err := c.Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{}, WithMessageID(strings.Repeat("x", 255)), NoReply()); err != nil {
		t.Fatalf("255 bytes is the limit, inclusive: %v", err)
	}
	if got := len(b.OpsOf("publish")) - before; got != 1 {
		t.Fatalf("invalid ids must not publish; publishes=%d", got)
	}
}

func TestInvokeNoReply(t *testing.T) {
	b := fakebroker.New()
	r := newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", func(amqp.Delivery, wire.Request) []amqp.Publishing { return nil })
	bus := dialTest(t, b, fastConfig())
	var out testpb.AddResponse
	if err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{A: 1}, &out, NoReply()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "delivery", func() bool { return len(r.deliveries()) == 1 })
	d := r.deliveries()[0]
	if d.ReplyTo != "" {
		t.Fatal("a fire-and-forget request asks for no reply")
	}
	if b.OpsOf("publish")[0].Mandatory {
		t.Fatal("a fire-and-forget request is not mandatory")
	}
	// Unbound fire-and-forget is fine.
	if err := NewClient(bus, "Nobody.Here").Invoke(testCtx(t), "x", &testpb.AddRequest{}, &out, NoReply()); err != nil {
		t.Fatalf("unrouted fire-and-forget: %v", err)
	}
}

func TestInvokeInstanceName(t *testing.T) {
	b := fakebroker.New()
	r := newResponder(t, b, "Test.Calc.i7", "REQUEST.Test.Calc.i7.*", adder)
	bus := dialTest(t, b, fastConfig())
	var out testpb.AddResponse
	if err := NewClient(bus, "Test.Calc", WithInstanceName("i7")).Invoke(testCtx(t), "add", &testpb.AddRequest{A: 1, B: 1}, &out); err != nil {
		t.Fatal(err)
	}
	d := r.deliveries()[0]
	req, _ := wire.DecodeRequest(d.Body)
	if d.RoutingKey != "REQUEST.Test.Calc.i7.add" || req.Method != "Test.Calc.add" || out.Sum != 2 {
		t.Fatalf("routing key %q, envelope method %q", d.RoutingKey, req.Method)
	}
}

func TestInvokeConcurrentCalls(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", adder)
	bus := dialTest(t, b, fastConfig())
	c := NewClient(bus, "Test.Calc")
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := range int32(100) {
		wg.Go(func() {
			var out testpb.AddResponse
			if err := c.Invoke(context.Background(), "add", &testpb.AddRequest{A: i, B: i}, &out); err != nil {
				errs <- err
				return
			}
			if out.Sum != 2*i {
				errs <- fmt.Errorf("call %d got %d: replies were crossed", i, out.Sum)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestInvokeDisconnectFailsPendingCalls(t *testing.T) {
	b := fakebroker.New()
	got := make(chan struct{}, 1)
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", func(amqp.Delivery, wire.Request) []amqp.Publishing {
		got <- struct{}{}
		return nil
	})
	bus := dialTest(t, b, fastConfig())
	errs := make(chan error, 1)
	go func() {
		errs <- NewClient(bus, "Test.Calc").Invoke(context.Background(), "add", &testpb.AddRequest{}, &testpb.AddResponse{})
	}()
	recvWithin(t, got, 2*time.Second)
	b.KillConnections()
	if err := recvWithin(t, errs, 2*time.Second); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("got %v", err)
	}
}

func TestInvokeParksThroughAReconnection(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	c := NewClient(bus, "Test.Calc")
	b.SetDialFault(errors.New("down"))
	b.KillConnections()
	eventually(t, "not ready", func() bool { _, _, ok := bus.sess.current(); return !ok })

	errs := make(chan error, 1)
	var out testpb.AddResponse
	go func() { errs <- c.Invoke(context.Background(), "add", &testpb.AddRequest{A: 4, B: 4}, &out) }()
	time.Sleep(20 * time.Millisecond)
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", adder)
	b.SetDialFault(nil)
	if err := recvWithin(t, errs, 3*time.Second); err != nil {
		t.Fatalf("a call issued mid-reconnection must complete once restored: %v", err)
	}
	if out.Sum != 8 {
		t.Fatal("wrong result")
	}
}

func TestInvokeRecoversFromADeadPublishChannel(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", adder)
	bus := dialTest(t, b, fastConfig())
	// A channel exception on a live connection: the publish channel dies.
	bus.dispatcher.mu.Lock()
	pub := bus.dispatcher.pub
	bus.dispatcher.mu.Unlock()
	_ = pub.ch.PublishWithContext(context.Background(), "missing.exchange", "x", false, false, amqp.Publishing{})
	<-pub.closedCh()
	var out testpb.AddResponse
	if err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{A: 1, B: 2}, &out); err != nil {
		t.Fatalf("the dispatcher must replace its dead channel: %v", err)
	}
}

func TestInvokeAfterCloseFails(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	_ = bus.Close()
	err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-bus.Done():
	default:
		t.Fatal("Done must be closed after Close")
	}
}

func TestDialRejectsInvalidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DefaultPrefetch = 0
	if _, err := Dial(context.Background(), "amqp://x", WithConfig(cfg), withDialer(fakebroker.New().Dial)); err == nil {
		t.Fatal("expected a config error")
	}
}

func TestDialLogsARedactedURL(t *testing.T) {
	b := fakebroker.New()
	var buf syncBuffer
	logger := slogTo(&buf)
	bus, err := Dial(testCtx(t), "amqp://user:hunter2@fake/", WithConfig(fastConfig()), WithLogger(logger), withDialer(b.Dial))
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()
	if strings.Contains(buf.String(), "hunter2") {
		t.Fatalf("the broker password reached the log: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "user:***@fake") {
		t.Fatalf("expected the redacted URL in %s", buf.String())
	}
}
