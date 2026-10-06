package protobus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
	"github.com/ArielLaub/protobus-go/v2/internal/transport"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
)

// Regression tests for the correctness review of 1b13034: retry routing,
// publish cancellation and capacity, handler concurrency under timeouts, and
// stream cancellation.

// ---- retries keep the route the broker delivered on ---------------------------

func rawEvent(t *testing.T, typ, topic string) []byte {
	t.Helper()
	return wire.AppendEvent(nil, wire.Event{Type: typ, Topic: topic})
}

func TestEventRetryIgnoresAPublisherSuppliedOriginalRoutingKey(t *testing.T) {
	for name, forged := range map[string]any{"string": "ADMIN.reset", "bytes": []byte("ADMIN.reset")} {
		t.Run(name, func(t *testing.T) {
			b := fakebroker.New()
			bus := dialTest(t, b, fastConfig())
			l, err := bus.NewEventListener("Sub.Events", WithEventRetry(EventRetryPolicy{MaxRetries: 2, Delay: 10 * time.Millisecond}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			var publicAttempts atomic.Int32
			public := newReceived[*testpb.OrderCreated]()
			admin := newReceived[*testpb.OrderCreated]()
			_ = Subscribe(testCtx(t), l, public.handler(func(*testpb.OrderCreated) error {
				if publicAttempts.Add(1) == 1 {
					return errors.New("transient")
				}
				return nil
			}), WithTopic("PUBLIC.#"))
			_ = Subscribe(testCtx(t), l, admin.handler(nil), WithTopic("ADMIN.#"))
			if err := l.Start(testCtx(t)); err != nil {
				t.Fatal(err)
			}

			peerConn := b.DialPeer()
			t.Cleanup(func() { _ = peerConn.Close() })
			peer, _ := peerConn.Channel()
			err = peer.PublishWithContext(context.Background(), "proto.bus.events", "PUBLIC.hello", false, false, amqp.Publishing{
				MessageId: "m1",
				Headers:   amqp.Table{headerOriginalKey: forged},
				Body:      rawEvent(t, "Test.OrderCreated", "PUBLIC.hello"),
			})
			if err != nil {
				t.Fatal(err)
			}
			recvWithin(t, public.ch, 2*time.Second)
			recvWithin(t, public.ch, 2*time.Second) // the retry, on its own route
			time.Sleep(50 * time.Millisecond)
			if admin.count() != 0 {
				t.Fatal("a forged x-original-routing-key redirected the retry to another subscription")
			}
			public.mu.Lock()
			info := public.info[1]
			public.mu.Unlock()
			if info.RoutingKey != "PUBLIC.hello" || info.Attempt != 1 {
				t.Fatalf("retry %+v", info)
			}
			for _, op := range b.OpsOf("publish") {
				if op.Exchange == "Sub.Events.Retry.Exchange" {
					if op.Key != "PUBLIC.hello" || op.Msg.Headers[headerOriginalKey] != "PUBLIC.hello" {
						t.Fatalf("the retry copy must carry the delivered route: key %s, header %v", op.Key, op.Msg.Headers[headerOriginalKey])
					}
				}
			}
		})
	}
}

func TestRequestRetryCannotCrossIntoAnotherService(t *testing.T) {
	for name, forged := range map[string]any{"string": "REQUEST.Other.Svc.wipe", "bytes": []byte("REQUEST.Other.Svc.wipe")} {
		t.Run(name, func(t *testing.T) {
			b := fakebroker.New()
			other := newResponder(t, b, "Other.Svc", "REQUEST.Other.Svc.*", func(amqp.Delivery, wire.Request) []amqp.Publishing { return nil })
			bus := dialTest(t, b, fastConfig())
			startCalc(t, bus, newCalcImpl(), fastRetry(3))
			p := newRawPeer(t, b)
			cid := p.send("REQUEST.Test.Calc.fail", amqp.Publishing{
				MessageId: "m1",
				Headers:   amqp.Table{headerOriginalKey: forged, "CC": []any{"REQUEST.Other.Svc.wipe"}, "BCC": []any{"REQUEST.Other.Svc.wipe"}},
				Body:      p.request("Test.Calc.fail", &testpb.FailRequest{Mode: "transient", Message: "boom", SucceedAfter: 1}),
			})
			reply := p.await(cid, 1)[0]
			if resp, err := wire.DecodeResponse(reply.Body); err != nil || resp.Error != nil {
				t.Fatalf("the retry must reach Test.Calc again and succeed: %v %+v", err, resp.Error)
			}
			if n := len(other.deliveries()); n != 0 {
				t.Fatalf("a forged header delivered the retry to another service (%d)", n)
			}
			for _, op := range b.OpsOf("publish") {
				if op.Exchange != "Test.Calc.Retry.Exchange" {
					continue
				}
				h := op.Msg.Headers
				if op.Key != "REQUEST.Test.Calc.fail" || h[headerOriginalKey] != "REQUEST.Test.Calc.fail" {
					t.Fatalf("retry routed on %s with header %v", op.Key, h[headerOriginalKey])
				}
				if _, ok := h["CC"]; ok {
					t.Fatal("a republished copy must not carry publisher-chosen CC routing")
				}
				if _, ok := h["BCC"]; ok {
					t.Fatal("a republished copy must not carry publisher-chosen BCC routing")
				}
			}
		})
	}
}

func TestDeadLetterRecordsTheDeliveredRoute(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), fastRetry(1))
	p := newRawPeer(t, b)
	cid := p.send("REQUEST.Test.Calc.fail", amqp.Publishing{
		Headers: amqp.Table{headerOriginalKey: "REQUEST.Other.Svc.wipe", "CC": []any{"Other.Svc"}},
		Body:    p.request("Test.Calc.fail", &testpb.FailRequest{Message: "always"}),
	})
	p.await(cid, 1)
	eventually(t, "dead-lettered", func() bool { return b.QueueDepth("Test.Calc.DLQ") == 1 })
	for _, op := range b.OpsOf("publish") {
		if op.Key == "Test.Calc.DLQ" {
			if op.Msg.Headers[headerOriginalKey] != "REQUEST.Test.Calc.fail" {
				t.Fatalf("the DLQ copy records %v", op.Msg.Headers[headerOriginalKey])
			}
			if _, ok := op.Msg.Headers["CC"]; ok {
				t.Fatal("the DLQ copy must not carry publisher-chosen CC routing")
			}
		}
	}
}

// ---- publish cancellation and capacity through the Bus ---------------------------

// gatedDialer wraps every channel the bus opens in a gatedChannel sharing one
// set of gates.
type gatedDialer struct {
	b       *fakebroker.Broker
	mu      sync.Mutex
	gates   map[string]chan struct{}
	entered chan string
}

type gatedConn struct {
	transport.Conn
	d *gatedDialer
}

func (c gatedConn) Channel() (transport.Channel, error) {
	ch, err := c.Conn.Channel()
	if err != nil {
		return nil, err
	}
	return &sharedGateChannel{Channel: ch, d: c.d}, nil
}

type sharedGateChannel struct {
	transport.Channel
	d *gatedDialer
}

func (g *sharedGateChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.d.mu.Lock()
	c := g.d.gates[msg.MessageId]
	g.d.mu.Unlock()
	if c != nil {
		g.d.entered <- msg.MessageId
		<-c
	}
	return g.Channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (d *gatedDialer) dial(ctx context.Context, url string, cfg amqp.Config) (transport.Conn, error) {
	c, err := d.b.Dial(ctx, url, cfg)
	if err != nil {
		return nil, err
	}
	return gatedConn{Conn: c, d: d}, nil
}

func (d *gatedDialer) gate(id string) func() {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := make(chan struct{})
	d.gates[id] = c
	return sync.OnceFunc(func() { close(c) })
}

func TestInvokeDeadlineCoversAStuckSendPath(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", adder)
	gd := &gatedDialer{b: b, gates: map[string]chan struct{}{}, entered: make(chan string, 4)}
	bus := dialTest(t, b, fastConfig(), withDialer(gd.dial))
	c := newCalcClient(bus)
	release := gd.gate("blocker")
	defer release()
	go func() {
		_, _ = c.Add(context.Background(), &testpb.AddRequest{}, WithMessageID("blocker"), WithTimeout(5*time.Second))
	}()
	recvWithin(t, gd.entered, 2*time.Second)

	start := time.Now()
	_, err := c.Add(context.Background(), &testpb.AddRequest{A: 1}, WithMessageID("waiting"), WithTimeout(50*time.Millisecond))
	if !errors.Is(err, ErrRPCTimeout) || time.Since(start) > time.Second {
		t.Fatalf("the call's deadline must bound the wait for the send path: %v after %v", err, time.Since(start))
	}
	release()
	time.Sleep(30 * time.Millisecond)
	for _, op := range b.OpsOf("publish") {
		if op.MessageID == "waiting" {
			t.Fatal("a call that timed out before it was sent must never be transmitted")
		}
	}
}

func TestDispatcherRecoversFromAChannelThatStopsConfirming(t *testing.T) {
	b := fakebroker.New()
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", adder)
	cfg := fastConfig()
	cfg.MaxOutstandingConfirms = 1
	cfg.PublishConfirmTimeout = 30 * time.Millisecond
	bus := dialTest(t, b, cfg)
	c := newCalcClient(bus)
	var drop atomic.Bool
	drop.Store(true)
	b.SetConfirmPolicy(func(p fakebroker.Published) fakebroker.ConfirmAction {
		if p.Exchange == "proto.bus" && drop.Load() {
			return fakebroker.Drop
		}
		return fakebroker.Ack
	})
	_, err := c.Add(testCtx(t), &testpb.AddRequest{})
	var pe *PublishError
	if !errors.As(err, &pe) || !pe.Ambiguous() {
		t.Fatalf("an unconfirmed request is ambiguous: %v", err)
	}
	_, err = c.Add(testCtx(t), &testpb.AddRequest{})
	if err == nil || errors.As(err, &pe) {
		t.Fatalf("with its capacity held, the channel sends nothing: %v", err)
	}
	if n := len(b.OpsOf("publish")); n > 2 {
		t.Fatalf("unconfirmed publishes must not pile up: %d", n)
	}
	drop.Store(false)
	eventually(t, "a fresh channel serves calls", func() bool {
		r, err := c.Add(testCtx(t), &testpb.AddRequest{A: 2, B: 3})
		return err == nil && r.Sum == 5
	})
}

// ---- handler concurrency survives processing timeouts -------------------------------

func TestTimedOutHandlersKeepTheirConcurrencySlot(t *testing.T) {
	for name, opts := range map[string][]ServiceOption{
		"early ack": {WithEarlyAck()},
		"late ack":  noRetryCfg(),
	} {
		t.Run(name, func(t *testing.T) {
			b := fakebroker.New()
			bus := dialTest(t, b, fastConfig())
			impl := newCalcImpl()
			opts := append([]ServiceOption{WithMaxConcurrent(1), WithProcessingTimeout(10 * time.Millisecond)}, opts...)
			svc := startCalc(t, bus, impl, opts...)
			p := newRawPeer(t, b)
			var cids []string
			for range 4 {
				cids = append(cids, p.send("REQUEST.Test.Calc.slow", amqp.Publishing{
					Body: p.request("Test.Calc.slow", &testpb.SlowRequest{Ms: 100, IgnoreCancel: true}),
				}))
			}
			for range 4 {
				recvWithin(t, impl.slowDone, 5*time.Second)
			}
			if m := impl.maxSeen.Load(); m != 1 {
				t.Fatalf("WithMaxConcurrent(1) ran %d handlers at once", m)
			}
			// Every caller still hears of its timeout.
			for _, cid := range cids {
				if e := decodeErrorReply(t, p.await(cid, 1)[0].Body); e.Code != CodeProcessingTimeout {
					t.Fatalf("got %+v", e)
				}
			}
			eventually(t, "every permit returned", func() bool { return len(svc.requests.sem) == 0 })
		})
	}
}

func TestTheTimeoutIsReportedWhileTheHandlerStillRuns(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl, WithEarlyAck(), WithMaxConcurrent(1), WithProcessingTimeout(10*time.Millisecond))
	p := newRawPeer(t, b)
	cid := p.send("REQUEST.Test.Calc.slow", amqp.Publishing{Body: p.request("Test.Calc.slow", &testpb.SlowRequest{Ms: 300, IgnoreCancel: true})})
	p.await(cid, 1) // within await's deadline, long before the handler returns
	if impl.running.Load() != 1 {
		t.Fatal("the reply came before the timeout or after the handler")
	}
	recvWithin(t, impl.slowDone, 2*time.Second)
}

func TestPermitsReturnAfterPanicsAndNormalReturns(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	svc := startCalc(t, bus, newCalcImpl(), WithEarlyAck(), WithMaxConcurrent(2))
	p := newRawPeer(t, b)
	var cids []string
	for i := range 6 {
		mode := "handled"
		if i%2 == 0 {
			mode = "panic"
		}
		cids = append(cids, p.send("REQUEST.Test.Calc.fail", amqp.Publishing{Body: p.request("Test.Calc.fail", &testpb.FailRequest{Mode: mode, Message: "x"})}))
	}
	cids = append(cids, p.send("REQUEST.Test.Calc.add", amqp.Publishing{Body: p.request("Test.Calc.add", &testpb.AddRequest{A: 1})}))
	for _, cid := range cids {
		p.await(cid, 1)
	}
	eventually(t, "every permit returned", func() bool { return len(svc.requests.sem) == 0 })
}

func TestCloseWithAnOverrunningHandlerReleasesItsPermitOnce(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	svc := startCalc(t, bus, impl, WithEarlyAck(), WithMaxConcurrent(1), WithProcessingTimeout(10*time.Millisecond))
	p := newRawPeer(t, b)
	p.send("REQUEST.Test.Calc.slow", amqp.Publishing{Body: p.request("Test.Calc.slow", &testpb.SlowRequest{Ms: 80, IgnoreCancel: true})})
	eventually(t, "running", func() bool { return impl.running.Load() == 1 })
	b.KillConnections() // reconnects; the handler is still running
	recvWithin(t, impl.slowDone, 2*time.Second)
	eventually(t, "permit returned", func() bool { return len(svc.requests.sem) == 0 })
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
	if n := len(svc.requests.sem); n != 0 {
		t.Fatalf("permits after close: %d", n)
	}
}

// ---- cancel registration lifecycle ---------------------------------------------

func newTestControl(r *cancelRegistry) *deliveryControl {
	_, cancel := context.WithCancelCause(context.Background())
	return &deliveryControl{disarmed: make(chan struct{}), cancels: r, id: "cid", cancel: cancel}
}

func TestCancellableAfterReleaseRegistersNothing(t *testing.T) {
	r := newCancelRegistry()
	ctl := newTestControl(r)
	ctl.release() // the processing timeout won
	ctl.disarmTimeout()
	ctl.cancellable() // the handler reaches its streaming setup late
	if n := r.size(); n != 0 {
		t.Fatalf("a registration after release leaked: %d", n)
	}
	ctl.release()
	if n := r.size(); n != 0 {
		t.Fatalf("size %d", n)
	}
}

func TestReleaseAfterCancellableRemovesTheRegistration(t *testing.T) {
	r := newCancelRegistry()
	ctl := newTestControl(r)
	ctl.cancellable()
	ctl.cancellable()
	if r.size() != 1 {
		t.Fatal("registered once")
	}
	ctl.release()
	ctl.release()
	if n := r.size(); n != 0 {
		t.Fatalf("size %d", n)
	}
}

func TestConcurrentCancellableAndRelease(t *testing.T) {
	r := newCancelRegistry()
	for range 500 {
		ctl := newTestControl(r)
		var wg sync.WaitGroup
		wg.Go(ctl.cancellable)
		wg.Go(ctl.release)
		wg.Wait()
		ctl.release() // what process does when the handler finally exits
	}
	if n := r.size(); n != 0 {
		t.Fatalf("%d registrations leaked", n)
	}
}

// ---- stream iteration observes cancellation -------------------------------------

func bufferedFrames(n int, final bool) func(d amqp.Delivery, m string) []amqp.Publishing {
	return func(d amqp.Delivery, m string) []amqp.Publishing {
		var out []amqp.Publishing
		for i := range int32(n) {
			out = append(out, frame(d, m, i, amqp.Table{headerSeq: i, headerFinal: final && int(i) == n-1}))
		}
		return out
	}
}

func cancelNotices(b *fakebroker.Broker) int {
	n := 0
	for _, op := range b.OpsOf("publish") {
		if op.Exchange == "proto.bus.cancel" {
			n++
		}
	}
	return n
}

func TestStreamCancellationWinsOverBufferedChunks(t *testing.T) {
	for name, final := range map[string]bool{"final frame buffered": true, "producer still running": false} {
		t.Run(name, func(t *testing.T) {
			b := fakebroker.New()
			rawStreamer(t, b, bufferedFrames(10, final))
			bus := dialTest(t, b, fastConfig())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var got []int32
			var lastErr error
			for chunk, err := range newCalcClient(bus).Count(ctx, &testpb.CountRequest{}) {
				if err != nil {
					lastErr = err
					break
				}
				got = append(got, chunk.I)
				if len(got) == 1 {
					// Every frame is buffered before the caller cancels.
					eventually(t, "frames buffered", func() bool {
						bus.dispatcher.mu.Lock()
						defer bus.dispatcher.mu.Unlock()
						for _, s := range bus.dispatcher.streams {
							s.mu.Lock()
							n := len(s.chunks)
							s.mu.Unlock()
							return n == 9
						}
						return false
					})
					cancel()
				}
			}
			if len(got) != 1 || !errors.Is(lastErr, context.Canceled) {
				t.Fatalf("after cancelling, nothing more is yielded and the stream ends with the context's error: %v %v", got, lastErr)
			}
			if n := bus.dispatcher.bufferedBytes.Load(); n != 0 {
				t.Fatalf("%d buffered bytes not released", n)
			}
			bus.dispatcher.mu.Lock()
			streams := len(bus.dispatcher.streams)
			bus.dispatcher.mu.Unlock()
			if streams != 0 {
				t.Fatal("the stream must be unregistered")
			}
			want := 1
			if final {
				want = 0 // the producer had already finished
			}
			time.Sleep(20 * time.Millisecond)
			if n := cancelNotices(b); n != want {
				t.Fatalf("cancel notices %d, want %d", n, want)
			}
		})
	}
}

func TestStreamBreakWithABufferedBacklog(t *testing.T) {
	b := fakebroker.New()
	rawStreamer(t, b, bufferedFrames(10, false))
	bus := dialTest(t, b, fastConfig())
	for range newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{}) {
		eventually(t, "backlog", func() bool { return bus.dispatcher.bufferedBytes.Load() > 0 })
		break
	}
	if n := bus.dispatcher.bufferedBytes.Load(); n != 0 {
		t.Fatalf("%d buffered bytes not released", n)
	}
	time.Sleep(20 * time.Millisecond)
	if n := cancelNotices(b); n != 1 {
		t.Fatalf("cancel notices %d", n)
	}
}

func TestStreamCompletedBeforeCancellationEndsCleanly(t *testing.T) {
	// Cancelling after the iterator has returned changes nothing.
	b := fakebroker.New()
	rawStreamer(t, b, bufferedFrames(3, true))
	bus := dialTest(t, b, fastConfig())
	ctx, cancel := context.WithCancel(context.Background())
	got, err := collect(t, newCalcClient(bus).Count(ctx, &testpb.CountRequest{}))
	cancel()
	if err != nil || len(got) != 3 {
		t.Fatalf("got %v %v", got, err)
	}
	time.Sleep(20 * time.Millisecond)
	if n := cancelNotices(b); n != 0 {
		t.Fatalf("a completed stream sends no cancel notice: %d", n)
	}
}
