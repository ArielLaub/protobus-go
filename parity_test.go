package protobus

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
	"github.com/ArielLaub/protobus-go/v2/protoload"
)

// Tests mirroring the TypeScript reference's behavioural checklist (its unit
// and integration suites), for the items not covered elsewhere.

// ---- config ---------------------------------------------------------------------

func TestConfigFromEnvReadsEveryVariable(t *testing.T) {
	env := map[string]string{
		"CALLBACKS_EXCHANGE_NAME": "cb", "EVENTS_EXCHANGE_NAME": "ev", "CANCEL_EXCHANGE_NAME": "cx",
		"PUBLISH_CONFIRM_TIMEOUT_MS": "1234", "CONNECTION_READY_TIMEOUT_MS": "2345", "MAX_OUTSTANDING_CONFIRMS": "17",
		"STREAM_MAX_BUFFERED_CHUNKS": "9", "STREAM_MAX_BUFFERED_BYTES": "999", "STREAM_MAX_TOTAL_BUFFERED_BYTES": "500",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	c := ConfigFromEnv()
	if c.CallbacksExchange != "cb" || c.EventsExchange != "ev" || c.CancelExchange != "cx" ||
		c.PublishConfirmTimeout != 1234*time.Millisecond || c.ConnectionReadyTimeout != 2345*time.Millisecond ||
		c.MaxOutstandingConfirms != 17 || c.StreamMaxBufferedChunks != 9 || c.StreamMaxBufferedBytes != 999 ||
		c.StreamMaxTotalBufferedBytes != 500 {
		t.Fatalf("%+v", c)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("every value the other ports accept must validate: %v", err)
	}
}

// ---- connection lifecycle ---------------------------------------------------------

func TestReconnectionEventsCarryAttemptAndDelay(t *testing.T) {
	b := fakebroker.New()
	events := make(chan ConnectionEvent, 32)
	bus := dialTest(t, b, fastConfig(), WithConnectionObserver(func(e ConnectionEvent) { events <- e }))
	_ = bus
	b.SetDialFault(errors.New("down"))
	b.KillConnections()
	var reconnecting []ConnectionEvent
	for e := range events {
		if e.Kind == EventReconnecting {
			reconnecting = append(reconnecting, e)
			if len(reconnecting) == 2 {
				b.SetDialFault(nil)
			}
		}
		if e.Kind == EventReconnected {
			if e.Attempt < 2 || e.Attempt != len(reconnecting) {
				t.Fatalf("reconnected on attempt %d after %d scheduled attempts", e.Attempt, len(reconnecting))
			}
			break
		}
	}
	for i, e := range reconnecting {
		if e.Attempt != i+1 || e.Delay <= 0 {
			t.Fatalf("reconnecting event %d: %+v", i, e)
		}
	}
}

func TestGivesUpWhenRestorationKeepsFailing(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.Reconnect.MaxRetries = 2
	bus := dialTest(t, b, cfg)
	startCalc(t, bus, newCalcImpl(), WithRetry(RetryPolicy{MaxRetries: 1, Delay: time.Second}))
	// Every restore now fails: the retry queue was re-declared elsewhere with
	// another TTL.
	other := b.DialPeer()
	ch, _ := other.Channel()
	b.DeleteQueue("Test.Calc.Retry")
	_, _ = ch.QueueDeclare("Test.Calc.Retry", true, false, false, false, amqp.Table{"x-message-ttl": int32(1)})
	b.KillConnections()
	select {
	case <-bus.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("restoration that keeps failing must exhaust the retry budget")
	}
	if !errors.Is(bus.Err(), ErrNotReady) {
		t.Fatalf("err %v", bus.Err())
	}
}

func TestParkedCallIsFailedByClose(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	b.SetDialFault(errors.New("down"))
	b.KillConnections()
	eventually(t, "not ready", func() bool { _, _, ok := bus.sess.current(); return !ok })
	errs := make(chan error, 1)
	go func() {
		errs <- NewClient(bus, "Test.Calc").Invoke(context.Background(), "add", &testpb.AddRequest{}, &testpb.AddResponse{})
	}()
	time.Sleep(20 * time.Millisecond)
	_ = bus.Close()
	if err := recvWithin(t, errs, 2*time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("a call parked on readiness must fail with ErrClosed, got %v", err)
	}
}

type relayCalc struct {
	*calcImpl
	bus *Bus
}

func (c *relayCalc) Add(ctx context.Context, in *testpb.AddRequest) (*testpb.AddResponse, error) {
	if err := c.bus.PublishEvent(ctx, &testpb.Ping{Id: "from-handler"}); err != nil {
		return nil, err
	}
	return &testpb.AddResponse{Sum: in.A + in.B}, nil
}

func TestHandlerPublishesAfterAReconnection(t *testing.T) {
	b := fakebroker.New()
	obs, wait := reconnected(t)
	bus := dialTest(t, b, fastConfig(), obs)
	startCalc(t, bus, &relayCalc{calcImpl: newCalcImpl(), bus: bus})
	got := newReceived[*testpb.Ping]()
	l, _ := bus.NewEventListener("")
	t.Cleanup(func() { _ = l.Close() })
	_ = Subscribe(testCtx(t), l, got.handler(nil))
	_ = l.Start(testCtx(t))
	b.KillConnections()
	wait()
	if _, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{A: 1}); err != nil {
		t.Fatal(err)
	}
	recvWithin(t, got.ch, 2*time.Second)
}

func TestPanickingHandlerLeaksNothing(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), noRetryCfg()...)
	_, _ = newCalcClient(bus).Fail(testCtx(t), &testpb.FailRequest{Mode: "panic", Message: "x"})
	eventually(t, "idle", func() bool { return bus.InFlight() == 0 })
}

func TestEventListenerUsesTheDefaultPrefetch(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("P.Events")
	t.Cleanup(func() { _ = l.Close() })
	var running, peak atomic.Int32
	done := make(chan struct{}, 4)
	_ = Subscribe(testCtx(t), l, func(context.Context, *testpb.Ping, EventInfo) error {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		done <- struct{}{}
		return nil
	})
	_ = l.Start(testCtx(t))
	for range 4 {
		_ = bus.PublishEvent(testCtx(t), &testpb.Ping{})
	}
	for range 4 {
		recvWithin(t, done, 2*time.Second)
	}
	if peak.Load() != 1 {
		t.Fatalf("an unconfigured listener handles DEFAULT_PREFETCH (1) at a time, saw %d", peak.Load())
	}
}

// ---- the publish / deadline race ---------------------------------------------------

func TestDeadlineDuringTheConfirmWaitIsAnRPCTimeout(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", adder)
	bus := dialTest(t, b, fastConfig())
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Drop })
	err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{}, WithTimeout(30*time.Millisecond))
	if !errors.Is(err, ErrRPCTimeout) || !IsCode(err, CodeRPCTimeout) {
		t.Fatalf("got %v", err)
	}
}

func TestPublishFailureBeatsTheDeadline(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	b.SetConfirmDelay(func(fakebroker.Published) time.Duration { return 0 })
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Nack })
	err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{}, WithTimeout(time.Second))
	var pe *PublishError
	if !errors.Is(err, ErrPublishNacked) || !errors.As(err, &pe) || pe.MessageID == "" {
		t.Fatalf("the specific publish failure, with its message id, must win: %v", err)
	}
}

func TestDisconnectWhileTheConfirmIsPending(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Drop })
	errs := make(chan error, 1)
	go func() {
		errs <- NewClient(bus, "Test.Calc").Invoke(context.Background(), "add", &testpb.AddRequest{}, &testpb.AddResponse{})
	}()
	eventually(t, "published", func() bool { return len(b.OpsOf("publish")) == 1 })
	b.SetConfirmPolicy(nil)
	b.KillConnections()
	err := recvWithin(t, errs, 3*time.Second)
	var pe *PublishError
	if !(errors.Is(err, ErrDisconnected) || errors.As(err, &pe) && pe.Ambiguous()) {
		t.Fatalf("an outcome the caller can recognise as ambiguous: %v", err)
	}
}

func TestInTimeHandlerIsNotPenalised(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), WithProcessingTimeout(500*time.Millisecond))
	if out, err := newCalcClient(bus).Slow(testCtx(t), &testpb.SlowRequest{Ms: 50}); err != nil || out.Sum != 50 {
		t.Fatalf("%v %v", out, err)
	}
}

func TestEventMessageIDIsValidated(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	before := len(b.OpsOf("publish"))
	if err := bus.PublishEvent(testCtx(t), &testpb.Ping{}, WithMessageID(" ")); !errors.Is(err, ErrInvalidMessageID) {
		t.Fatalf("got %v", err)
	}
	if len(b.OpsOf("publish")) != before {
		t.Fatal("nothing must be published")
	}
}

func TestDeliveryErrorsPassThroughInvoke(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.PublishConfirmTimeout = 30 * time.Millisecond
	bus := dialTest(t, b, cfg)
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Drop })
	err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{}, WithMessageID("dedupe-me"))
	var pe *PublishError
	if !errors.Is(err, ErrPublishConfirmTimeout) || !errors.As(err, &pe) || pe.MessageID != "dedupe-me" || !pe.Ambiguous() {
		t.Fatalf("the error keeps its identity and message id: %v", err)
	}
}

func TestLocalEncodeFailureIsInvalidRequest(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "echo", &testpb.Order{Id: "\xff"}, &testpb.Order{})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("got %v", err)
	}
	err = NewClient(bus, "Test.Calc").Invoke(testCtx(t), "echo", &testpb.Order{Amount: overlong()}, &testpb.Order{})
	if !errors.Is(err, ErrInvalidRequest) || !errors.Is(err, pbtypes.ErrBigintRange) {
		t.Fatalf("an out-of-range bigint must not be sent: %v", err)
	}
}

func TestOutOfRangeBigintsAreNeverSent(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	if err := bus.PublishEvent(testCtx(t), &testpb.OrderCreated{Amount: overlong()}); !errors.Is(err, pbtypes.ErrBigintRange) {
		t.Fatalf("event: %v", err)
	}
	startCalc(t, bus, &badReply{calcImpl: newCalcImpl()}, noRetryCfg()...)
	_, err := newCalcClient(bus).Echo(testCtx(t), &testpb.Order{})
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("a reply that cannot be encoded fails the call: %v", err)
	}
}

type badReply struct{ *calcImpl }

func (badReply) Echo(context.Context, *testpb.Order) (*testpb.Order, error) {
	return &testpb.Order{Amount: overlong()}, nil
}

// ---- settlement --------------------------------------------------------------

func TestStreamFramesPrecedeTheAck(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	if _, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 3})); err != nil {
		t.Fatal(err)
	}
	eventually(t, "ack", func() bool { return len(b.OpsOf("ack")) == 1 })
	ops := b.Ops()
	ack := slices.IndexFunc(ops, func(o fakebroker.Op) bool { return o.Kind == "ack" })
	for i, o := range ops {
		if o.Kind == "publish" && o.Exchange == "proto.bus.callback" && i > ack {
			t.Fatal("every frame is published before the request is acknowledged")
		}
	}
}

func refuseErrorReplies(b *fakebroker.Broker) {
	b.SetConfirmPolicy(func(p fakebroker.Published) fakebroker.ConfirmAction {
		if p.Exchange == "proto.bus.callback" {
			return fakebroker.Nack
		}
		return fakebroker.Ack
	})
}

func TestSettlementSurvivesAFailedErrorReply(t *testing.T) {
	t.Run("still dead-letters", func(t *testing.T) {
		b := fakebroker.New()
		bus := dialTest(t, b, fastConfig())
		startCalc(t, bus, newCalcImpl(), fastRetry(1))
		peer := newRawPeer(t, b)
		refuseErrorReplies(b)
		peer.send("REQUEST.Test.Calc.fail", amqp.Publishing{Body: peer.request("Test.Calc.fail", &testpb.FailRequest{Mode: "unhandled"})})
		eventually(t, "dead-lettered", func() bool { return b.QueueDepth("Test.Calc.DLQ") == 1 })
		eventually(t, "acked", func() bool { return len(b.OpsOf("ack")) == 2 })
	})
	t.Run("still rejects without retries", func(t *testing.T) {
		b := fakebroker.New()
		bus := dialTest(t, b, fastConfig())
		startCalc(t, bus, newCalcImpl(), noRetryCfg()...)
		peer := newRawPeer(t, b)
		refuseErrorReplies(b)
		peer.send("REQUEST.Test.Calc.fail", amqp.Publishing{Body: peer.request("Test.Calc.fail", &testpb.FailRequest{Mode: "unhandled"})})
		eventually(t, "rejected", func() bool { return len(b.OpsOf("reject")) == 1 })
		if len(b.OpsOf("nack")) != 0 {
			t.Fatal("a failed error reply must not requeue the message")
		}
	})
}

func TestRepublishedPropertiesAreExact(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), fastRetry(1))
	peer := newRawPeer(t, b)
	peer.send("REQUEST.Test.Calc.fail", amqp.Publishing{
		Body: peer.request("Test.Calc.fail", &testpb.FailRequest{Mode: "unhandled"}), Priority: 2, UserId: "guest",
	})
	eventually(t, "dead-lettered", func() bool { return b.QueueDepth("Test.Calc.DLQ") == 1 })
	dl := b.Messages("Test.Calc.DLQ")[0]
	if dl.Priority != 2 {
		t.Fatal("priority is carried onto the DLQ")
	}
	if dl.UserId != "" {
		t.Fatal("userId is dropped: the broker validates it against the republishing user")
	}
	if dl.ContentEncoding != "" || dl.Type != "" || dl.AppId != "" || dl.Expiration != "" {
		t.Fatalf("a hop gains no property the original lacked: %+v", dl)
	}

	// No priority on the hop when the original had none.
	peer.send("REQUEST.Test.Calc.fail", amqp.Publishing{Body: peer.request("Test.Calc.fail", &testpb.FailRequest{Mode: "unhandled"})})
	eventually(t, "second dead-lettered", func() bool { return b.QueueDepth("Test.Calc.DLQ") == 2 })
	if p := b.Messages("Test.Calc.DLQ")[1].Priority; p != 0 {
		t.Fatalf("priority %d appeared from nowhere", p)
	}
}

// ---- dispatch -------------------------------------------------------------------

func TestDispatchWithoutARoutingKeyStillValidatesTheBody(t *testing.T) {
	// A delivery with no routing key (a shovel to the default exchange) is
	// served if, and only if, its body names a method of this contract.
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	svc := startCalc(t, bus, newCalcImpl())
	peer := newRawPeer(t, b)
	ok := svc.handle(testCtx(t), &amqp.Delivery{Body: peer.request("Test.Calc.add", &testpb.AddRequest{A: 1, B: 2})}, &deliveryControl{disarmed: make(chan struct{})})
	if ok.reply == nil {
		t.Fatal("no reply")
	}
	if resp, _ := wire.DecodeResponse(ok.reply); resp.Result == nil {
		t.Fatalf("a valid body is served: %+v", resp.Error)
	}
	bad := svc.handle(testCtx(t), &amqp.Delivery{Body: peer.request("Test.Other.add", &testpb.AddRequest{})}, &deliveryControl{disarmed: make(chan struct{})})
	if resp, _ := wire.DecodeResponse(bad.reply); resp.Error == nil || resp.Error.Code != CodeProtocol {
		t.Fatal("a body naming another service is refused even without a routing key")
	}
}

const dottedProto = `syntax = "proto3";
package acme.orders.v1;
service Orders { rpc place(Order) returns (Placed); rpc watch(Order) returns (stream Placed); }
message Order { string id = 1; }
message Placed { string id = 1; }
`

func TestDottedPackages(t *testing.T) {
	res, err := protoload.Parse(context.Background(), map[string]string{"orders.proto": dottedProto})
	if err != nil {
		t.Fatal(err)
	}
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig(), WithRegistry(res.Files, res.Types))
	placed, _ := res.Types.FindMessageByName("acme.orders.v1.Placed")
	reply := func(req proto.Message) proto.Message {
		m := placed.New()
		m.Set(m.Descriptor().Fields().ByName("id"), req.ProtoReflect().Get(req.ProtoReflect().Descriptor().Fields().ByName("id")))
		return m.Interface()
	}
	svc, err := bus.RegisterDynamic("acme.orders.v1.Orders", DynamicHandlers{
		Unary: map[string]DynamicHandler{"place": func(_ context.Context, req proto.Message) (proto.Message, error) { return reply(req), nil }},
		Streams: map[string]DynamicStreamHandler{"watch": func(_ context.Context, req proto.Message, send func(proto.Message) error) error {
			return send(reply(req))
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if keys := b.Bindings("proto.bus", "acme.orders.v1.Orders"); len(keys) != 1 || keys[0] != "REQUEST.acme.orders.v1.Orders.*" {
		t.Fatalf("bindings %v", keys)
	}
	c, err := bus.ResolveClient("acme.orders.v1.Orders")
	if err != nil {
		t.Fatal(err)
	}
	req, _ := c.NewRequest("place")
	req.ProtoReflect().Set(req.ProtoReflect().Descriptor().Fields().ByName("id"), protoreflect.ValueOfString("o-1"))
	out, err := c.Call(testCtx(t), "place", req)
	if err != nil || out.ProtoReflect().Get(out.ProtoReflect().Descriptor().Fields().ByName("id")).String() != "o-1" {
		t.Fatalf("%v %v", out, err)
	}
	n := 0
	for _, err := range c.CallStream(testCtx(t), "watch", req) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 1 {
		t.Fatalf("stream yielded %d", n)
	}
	if _, err := c.Call(testCtx(t), "nope", req); err == nil {
		t.Fatal("an unknown method is named, not misparsed")
	}
}

// ---- logging: no payload, header or credential ever reaches the log ------------

const secret = "SECRET-PAYLOAD-0451"

func TestNothingFromAPayloadReachesTheLog(t *testing.T) {
	b := fakebroker.New()
	var buf syncBuffer
	bus := dialTest(t, b, fastConfig(), WithLogger(slogTo(&buf)))
	svc := startCalc(t, bus, newCalcImpl(), noRetryCfg()...)
	peer := newRawPeer(t, b)

	// An unhandled message: a method nobody implements.
	cid := peer.send("REQUEST.Test.Calc.missing", amqp.Publishing{Body: peer.request("Test.Calc.missing", &testpb.AddRequest{}), Headers: amqp.Table{"x-secret": secret}})
	peer.await(cid, 1)
	// A failing handler whose request carries the secret.
	_, _ = newCalcClient(bus).Fail(testCtx(t), &testpb.FailRequest{Mode: "handled", Code: "C", Message: "fine"}, WithActor(secret))
	_, _ = newCalcClient(bus).Echo(testCtx(t), &testpb.Order{Id: secret, Tags: []string{secret}})
	// An event nobody handles, and one whose handler fails.
	_ = Subscribe(testCtx(t), svc.Events(), func(context.Context, *testpb.OrderCreated, EventInfo) error { return errors.New("handler failed") })
	_ = bus.PublishEvent(testCtx(t), &testpb.OrderCreated{Id: secret})
	_ = bus.PublishEvent(testCtx(t), &testpb.Ping{Id: secret}, WithTopic("EVENT.Test.OrderCreated"))
	time.Sleep(50 * time.Millisecond)
	if strings.Contains(buf.String(), secret) {
		t.Fatalf("payload, header or actor text reached the log:\n%s", buf.String())
	}
}

func TestComponentIsStampedOnAUserLogger(t *testing.T) {
	b := fakebroker.New()
	var buf syncBuffer
	dialTest(t, b, fastConfig(), WithLogger(slogTo(&buf)))
	if !strings.Contains(buf.String(), "component=protobus") {
		t.Fatalf("%s", buf.String())
	}
}

// ---- shutdown -------------------------------------------------------------------

func TestShutdownTwiceAndWhileDisconnected(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	b.SetDialFault(errors.New("down"))
	b.KillConnections()
	if err := bus.Shutdown(testCtx(t)); err != nil {
		t.Fatalf("shutdown while disconnected: %v", err)
	}
	if err := bus.Shutdown(testCtx(t)); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

// ---- streaming ------------------------------------------------------------------

func TestStreamBookkeepingIsReleased(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	c := newCalcClient(bus)
	_, _ = collect(t, c.Count(testCtx(t), &testpb.CountRequest{N: 3}))
	for _, err := range c.Count(testCtx(t), &testpb.CountRequest{N: 100, DelayMs: 2}) {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	eventually(t, "server side released", func() bool { return bus.cancels.size() == 0 })
	bus.dispatcher.mu.Lock()
	defer bus.dispatcher.mu.Unlock()
	if len(bus.dispatcher.streams) != 0 || len(bus.dispatcher.calls) != 0 {
		t.Fatal("finished streams must leave nothing behind")
	}
}

func TestStreamWithACancelledContextPublishesNothing(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := collect(t, newCalcClient(bus).Count(ctx, &testpb.CountRequest{N: 1}))
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, op := range b.OpsOf("publish") {
		if op.Key == "REQUEST.Test.Calc.count" {
			t.Fatal("nothing must be published")
		}
	}
}

func TestCancelOfAnUnknownStream(t *testing.T) {
	if newCancelRegistry().cancel("never-seen") {
		t.Fatal("cancelling an unknown stream reports false")
	}
}

func TestServiceWorksWhenTheCancelExchangeIsUnavailable(t *testing.T) {
	b := fakebroker.New()
	// Someone declared the cancel exchange with another type: declaring it
	// fails, and the service must run on without cancellation.
	peer := b.DialPeer()
	ch, _ := peer.Channel()
	_ = ch.ExchangeDeclare("proto.bus.cancel", "topic", true, false, false, false, nil)
	_ = peer.Close()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	if out, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{A: 1, B: 1}); err != nil || out.Sum != 2 {
		t.Fatalf("%v %v", out, err)
	}
}

func TestExactlyOneCancelHoweverManyTriggers(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl)
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	for _, err := range newCalcClient(bus).Count(ctx, &testpb.CountRequest{N: 1000, DelayMs: 2}, WithIdleTimeout(time.Second)) {
		if err != nil {
			break
		}
		if n++; n == 2 {
			cancel() // the context ends AND the loop breaks
			break
		}
	}
	cancel()
	recvWithin(t, impl.ended, 3*time.Second)
	cancels := 0
	for _, op := range b.OpsOf("publish") {
		if op.Exchange == "proto.bus.cancel" {
			cancels++
		}
	}
	if cancels != 1 {
		t.Fatalf("%d cancel notices", cancels)
	}
}

func TestStreamPerCallByteBound(t *testing.T) {
	b := fakebroker.New()
	rawStreamer(t, b, func(d amqp.Delivery, m string) []amqp.Publishing {
		var out []amqp.Publishing
		for i := range int32(10) {
			out = append(out, frame(d, m, i, amqp.Table{headerSeq: i, headerFinal: i == 9}))
		}
		return out
	})
	cfg := fastConfig()
	one := int64(len(resultReply(amqp.Delivery{}, "Test.Calc.count", &testpb.CountChunk{I: 1}).Body))
	cfg.StreamMaxBufferedBytes = one * 3
	bus := dialTest(t, b, cfg)
	var err error
	for _, e := range newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{}) {
		if e != nil {
			err = e
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !errors.Is(err, ErrStreamBackpressure) {
		t.Fatalf("got %v", err)
	}
}

func TestTimedOutStreamReturnsItsBytes(t *testing.T) {
	b := fakebroker.New()
	rawStreamer(t, b, func(d amqp.Delivery, m string) []amqp.Publishing {
		return []amqp.Publishing{frame(d, m, 0, amqp.Table{headerSeq: int8(0), headerFinal: false})}
	})
	bus := dialTest(t, b, fastConfig())
	for _, err := range newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{}, WithIdleTimeout(50*time.Millisecond)) {
		if err != nil {
			if !errors.Is(err, ErrStreamTimeout) {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(200 * time.Millisecond) // hold the chunk past the idle timeout
	}
	if n := bus.dispatcher.bufferedBytes.Load(); n != 0 {
		t.Fatalf("%d bytes not returned to the allowance", n)
	}
}

func TestSingleChunkStream(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	got, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 1}))
	if err != nil || !slices.Equal(got, []int32{0}) {
		t.Fatalf("%v %v", got, err)
	}
}

// ---- priority -------------------------------------------------------------------

func TestQueueArgumentsSurviveReconnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []ServiceOption
		want amqp.Table
	}{
		{"priority", []ServiceOption{WithMaxPriority(2)}, amqp.Table{"x-max-priority": int32(2)}},
		{"none", nil, amqp.Table{}},
		{"ttl only", []ServiceOption{WithRetry(RetryPolicy{MessageTTL: time.Minute})}, amqp.Table{"x-message-ttl": int32(60000)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := fakebroker.New()
			obs, wait := reconnected(t)
			bus := dialTest(t, b, fastConfig(), obs)
			startCalc(t, bus, newCalcImpl(), tc.opts...)
			b.Restart() // durable queues keep their arguments; redeclaring must match
			wait()
			info, _ := b.Queue("Test.Calc")
			if len(info.Args) != len(tc.want) || info.Consumers != 1 {
				t.Fatalf("after reconnecting: %+v", info)
			}
			if dlq, ok := b.Queue("Test.Calc.DLQ"); ok && len(dlq.Args) != 0 {
				t.Fatal("the DLQ never gets arguments")
			}
		})
	}
}

func TestMaxPriorityAcceptsItsRange(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	for _, p := range []uint8{1, 2, 10, 255} {
		if _, err := bus.Register(&calcServiceDesc, newCalcImpl(), WithMaxPriority(p)); err != nil {
			t.Errorf("%d: %v", p, err)
		}
	}
}

func TestPriorityOnFireAndForget(t *testing.T) {
	b := fakebroker.New()
	r := newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", func(amqp.Delivery, wire.Request) []amqp.Publishing { return nil })
	bus := dialTest(t, b, fastConfig())
	if err := NewClient(bus, "Test.Calc").Invoke(testCtx(t), "add", &testpb.AddRequest{}, &testpb.AddResponse{}, NoReply(), WithPriority(2)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "delivered", func() bool { return len(r.deliveries()) == 1 })
	if r.deliveries()[0].Priority != 2 {
		t.Fatal("priority")
	}
}

func TestEventsQueueGetsNoPriority(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	svc, _ := bus.Register(&calcServiceDesc, newCalcImpl(), WithMaxPriority(2))
	_ = Subscribe(testCtx(t), svc.Events(), func(context.Context, *testpb.Ping, EventInfo) error { return nil })
	_ = svc.Start(testCtx(t))
	if info, _ := b.Queue("Test.Calc.Events"); len(info.Args) != 0 {
		t.Fatalf("events queue args %v", info.Args)
	}
}

func TestAddingPriorityToAnExistingQueueFailsUntilItIsDeleted(t *testing.T) {
	b := fakebroker.New()
	startCalc(t, dialTest(t, b, fastConfig()), newCalcImpl())
	svc, _ := dialTest(t, b, fastConfig()).Register(&calcServiceDesc, newCalcImpl(), WithMaxPriority(2))
	err := svc.Start(testCtx(t))
	var ae *amqp.Error
	if !errors.As(err, &ae) || ae.Code != amqp.PreconditionFailed {
		t.Fatalf("got %v", err)
	}
}

// ---- events -------------------------------------------------------------------

func TestEventRetryRerunsEveryMatchedHandlerAndKeepsTheID(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("R.Events", WithEventRetry(EventRetryPolicy{MaxRetries: 1, Delay: 10 * time.Millisecond}))
	t.Cleanup(func() { _ = l.Close() })
	var mu sync.Mutex
	runs := map[string]int{}
	ids := map[string]bool{}
	record := func(name string, fail bool) func(context.Context, *testpb.Ping, EventInfo) error {
		return func(_ context.Context, _ *testpb.Ping, info EventInfo) error {
			mu.Lock()
			runs[name]++
			ids[info.MessageID] = true
			mu.Unlock()
			if fail {
				return errors.New("always")
			}
			return nil
		}
	}
	_ = Subscribe(testCtx(t), l, record("ok", false))
	_ = Subscribe(testCtx(t), l, record("bad", true))
	_ = l.Start(testCtx(t))
	_ = bus.PublishEvent(testCtx(t), &testpb.Ping{}, WithMessageID("evt-1"))
	eventually(t, "dead-lettered", func() bool { return b.QueueDepth("R.Events.DLQ") == 1 })
	mu.Lock()
	defer mu.Unlock()
	if runs["ok"] != 2 || runs["bad"] != 2 {
		t.Fatalf("a retried event re-runs every matching handler: %v", runs)
	}
	if len(ids) != 1 || !ids["evt-1"] || b.Messages("R.Events.DLQ")[0].MessageId != "evt-1" {
		t.Fatalf("one message id across every hop: %v", ids)
	}
	if b.QueueDepth("R.Events") != 0 || b.QueueDepth("R.Events.Retry") != 0 {
		t.Fatal("nothing is left parked")
	}
}

func TestBrokerCancelledConsumerIsRebuilt(t *testing.T) {
	// The Go analogue of amqplib's null delivery: an operator deletes the
	// queue and the broker cancels the consumer. The consumer is rebuilt.
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	b.DeleteQueue("Test.Calc")
	eventually(t, "rebuilt", func() bool { info, ok := b.Queue("Test.Calc"); return ok && info.Consumers == 1 })
	if _, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{}); err != nil {
		t.Fatal(err)
	}
}
