package protobus

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
)

func reconnected(t *testing.T) (DialOption, func()) {
	t.Helper()
	ch := make(chan ConnectionEvent, 64)
	return WithConnectionObserver(func(e ConnectionEvent) { ch <- e }), func() {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case e := <-ch:
				if e.Kind == EventReconnected {
					return
				}
			case <-deadline:
				t.Fatal("no reconnection")
			}
		}
	}
}

func TestServiceServesAgainAfterAnOutage(t *testing.T) {
	b := fakebroker.New()
	obs, wait := reconnected(t)
	bus := dialTest(t, b, fastConfig(), obs)
	svc := startCalc(t, bus, newCalcImpl())
	got := newReceived[*testpb.Ping]()
	_ = Subscribe(testCtx(t), svc.Events(), got.handler(nil))
	c := newCalcClient(bus)

	b.KillConnections()
	wait()
	if out, err := c.Add(testCtx(t), &testpb.AddRequest{A: 5, B: 5}); err != nil || out.Sum != 10 {
		t.Fatalf("after reconnecting: %v %v", out, err)
	}
	if err := bus.PublishEvent(testCtx(t), &testpb.Ping{Id: "after"}); err != nil {
		t.Fatal(err)
	}
	recvWithin(t, got.ch, 2*time.Second)
}

func TestServiceSurvivesABrokerRestart(t *testing.T) {
	// Non-durable state vanishes: the private reply queue comes back under a
	// new name, the durable service queue keeps its persistent backlog.
	b := fakebroker.New()
	obs, wait := reconnected(t)
	bus := dialTest(t, b, fastConfig(), obs)
	startCalc(t, bus, newCalcImpl())
	oldReply := bus.dispatcher.replyQueue
	b.Restart()
	wait()
	if bus.dispatcher.replyQueue == oldReply || !b.HasQueue(bus.dispatcher.replyQueue) {
		t.Fatal("a fresh reply queue must be declared")
	}
	if out, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{A: 1, B: 2}); err != nil || out.Sum != 3 {
		t.Fatalf("after a restart: %v %v", out, err)
	}
}

func TestStopConsumingIsNotUndoneByAReconnection(t *testing.T) {
	b := fakebroker.New()
	obs, wait := reconnected(t)
	bus := dialTest(t, b, fastConfig(), obs)
	svc := startCalc(t, bus, newCalcImpl())
	b.SetDialFault(errors.New("down"))
	b.KillConnections()
	_ = svc.StopConsuming(context.Background())
	b.SetDialFault(nil)
	wait()
	time.Sleep(20 * time.Millisecond)
	if info, _ := b.Queue("Test.Calc"); info.Consumers != 0 {
		t.Fatalf("a stopped service came back to life with %d consumers", info.Consumers)
	}
}

func TestShutdownDrainsInFlightWork(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl)
	c := newCalcClient(bus)
	res := make(chan error, 1)
	go func() {
		out, err := c.Slow(context.Background(), &testpb.SlowRequest{Ms: 150})
		if err == nil && out.Sum != 150 {
			err = errors.New("wrong result")
		}
		res <- err
	}()
	eventually(t, "in flight", func() bool { return bus.InFlight() == 1 })
	start := time.Now()
	if err := bus.Shutdown(context.Background()); err != nil {
		t.Fatalf("a drain that completes in time is clean: %v", err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("shutdown must wait for in-flight work")
	}
	// The caller is on the same bus, so it is closed now; the reply was sent
	// before the ack, which the operation log shows.
	ops := b.Ops()
	reply := slices.IndexFunc(ops, func(o fakebroker.Op) bool { return o.Kind == "publish" && o.Exchange == "proto.bus.callback" })
	ack := slices.IndexFunc(ops, func(o fakebroker.Op) bool { return o.Kind == "ack" })
	if reply < 0 || ack < reply {
		t.Fatal("the in-flight request must be answered and acknowledged before close")
	}
	<-res
}

func TestShutdownGivesUpAtTheDeadline(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.ShutdownDrainTimeout = 50 * time.Millisecond
	bus := dialTest(t, b, cfg)
	impl := newCalcImpl()
	startCalc(t, bus, impl)
	go func() {
		_, _ = newCalcClient(bus).Slow(context.Background(), &testpb.SlowRequest{Ms: 400, IgnoreCancel: true})
	}()
	eventually(t, "in flight", func() bool { return bus.InFlight() == 1 })
	err := bus.Shutdown(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	// Unsettled work goes back to the queue for another replica.
	eventually(t, "requeued", func() bool { return b.QueueDepth("Test.Calc") == 1 })
	recvWithin(t, impl.slowDone, 2*time.Second)
}

func TestRunShutsDownWhenItsContextEnds(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	svc, _ := bus.Register(&calcServiceDesc, newCalcImpl())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, bus, svc) }()
	eventually(t, "consuming", func() bool { q, _ := b.Queue("Test.Calc"); return q.Consumers == 1 })
	cancel()
	if err := recvWithin(t, done, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(bus.Err(), ErrClosed) {
		t.Fatal("Run closes the bus")
	}
}

func TestRunReturnsWhenTheBusGivesUp(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.Reconnect.MaxRetries = 2
	bus := dialTest(t, b, cfg)
	svc, _ := bus.Register(&calcServiceDesc, newCalcImpl())
	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), bus, svc) }()
	eventually(t, "consuming", func() bool { q, _ := b.Queue("Test.Calc"); return q.Consumers == 1 })
	b.SetDialFault(errors.New("gone"))
	b.KillConnections()
	if err := recvWithin(t, done, 3*time.Second); !errors.Is(err, ErrNotReady) {
		t.Fatalf("got %v", err)
	}
}

func TestRunReportsAStartFailure(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	svc, _ := bus.Register(&calcServiceDesc, newCalcImpl(), WithRetry(RetryPolicy{MaxRetries: 1, Delay: time.Second}))
	other := dialTest(t, b, fastConfig())
	_ = startCalc(t, other, newCalcImpl(), WithRetry(RetryPolicy{MaxRetries: 1, Delay: 2 * time.Second}))
	if err := Run(context.Background(), bus, svc); !errors.Is(err, ErrRetryQueueMismatch) {
		t.Fatalf("got %v", err)
	}
}

func TestConsumerRecoversADeadChannelOnALiveConnection(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	svc := startCalc(t, bus, newCalcImpl())
	svc.requests.mu.Lock()
	ch := svc.requests.ch
	svc.requests.mu.Unlock()
	_ = ch.Close() // the channel dies; the connection lives
	eventually(t, "rebuilt", func() bool {
		svc.requests.mu.Lock()
		defer svc.requests.mu.Unlock()
		return svc.requests.ch != ch && svc.requests.tag != ""
	})
	if out, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{A: 2, B: 3}); err != nil || out.Sum != 5 {
		t.Fatalf("after recovery: %v %v", out, err)
	}
}

func TestDispatcherRecoversADeadReplyConsumer(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	bus.dispatcher.mu.Lock()
	ch := bus.dispatcher.consumeCh
	bus.dispatcher.mu.Unlock()
	_ = ch.Close()
	eventually(t, "rebuilt", func() bool {
		bus.dispatcher.mu.Lock()
		defer bus.dispatcher.mu.Unlock()
		return bus.dispatcher.consumeCh != ch && bus.dispatcher.consumeCh != nil
	})
	if out, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{A: 2, B: 3}); err != nil || out.Sum != 5 {
		t.Fatalf("after recovery: %v %v", out, err)
	}
}

func TestCancelListenerSurvivesAReconnection(t *testing.T) {
	b := fakebroker.New()
	obs, wait := reconnected(t)
	bus := dialTest(t, b, fastConfig(), obs)
	impl := newCalcImpl()
	startCalc(t, bus, impl)
	b.KillConnections()
	wait()
	n := 0
	for _, err := range newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 1000, DelayMs: 5}) {
		if err != nil {
			t.Fatal(err)
		}
		if n++; n == 2 {
			break
		}
	}
	if end := recvWithin(t, impl.ended, 3*time.Second); !end.cancelled {
		t.Fatalf("cancellation must still reach the producer after a reconnection: %+v", end)
	}
}

func TestCloseIsIdempotentAndFailsPendingCalls(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	errs := make(chan error, 1)
	go func() {
		_, err := newCalcClient(bus).Slow(context.Background(), &testpb.SlowRequest{Ms: 2000})
		errs <- err
	}()
	eventually(t, "in flight", func() bool { return bus.InFlight() == 1 })
	_ = bus.Close()
	_ = bus.Close()
	if err := recvWithin(t, errs, 2*time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
}
