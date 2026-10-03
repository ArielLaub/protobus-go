package protobus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

// Regression tests for defects found in review. Each names the failure it
// pins down.

func bareBus() *Bus {
	cfg := fastConfig()
	b := &Bus{cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil)), cancels: newCancelRegistry()}
	b.sess = newSession(nil, "", cfg, b.log, nil)
	b.dispatcher = newDispatcher(b)
	return b
}

func TestStreamFailedAfterItsFinalFrameKeepsItsData(t *testing.T) {
	// A connection loss after the producer finished must not turn a complete
	// stream the caller has not yet drained into a short "complete" one.
	b := bareBus()
	defer b.sess.cancel()
	s := b.dispatcher.newStream("s1")
	s.push(&amqp.Delivery{Body: []byte("a"), Headers: amqp.Table{headerSeq: int64(0)}})
	s.push(&amqp.Delivery{Body: []byte("b"), Headers: amqp.Table{headerSeq: int64(1), headerFinal: true}})
	s.fail(ErrDisconnected)
	var got []string
	for {
		chunk, done, err := s.next()
		if err != nil {
			t.Fatalf("a complete stream must not fail late: %v", err)
		}
		if chunk != nil {
			got = append(got, string(chunk))
			continue
		}
		if done {
			break
		}
	}
	if fmt.Sprint(got) != "[a b]" {
		t.Fatalf("got %v", got)
	}
}

func TestStreamPushAfterReleaseDoesNotLeakTheAllowance(t *testing.T) {
	b := bareBus()
	defer b.sess.cancel()
	s := b.dispatcher.newStream("s1")
	s.release()
	s.push(&amqp.Delivery{Body: make([]byte, 1000), Headers: amqp.Table{headerSeq: int64(0)}})
	if n := b.dispatcher.bufferedBytes.Load(); n != 0 {
		t.Fatalf("%d bytes leaked from the process-wide allowance", n)
	}
}

func TestConnectionLostNeverBlocksOnAStaleCall(t *testing.T) {
	// A call notified once and re-registered (its publish was retried) must
	// not block the supervisor when the connection drops again.
	b := bareBus()
	defer b.sess.cancel()
	d := b.dispatcher
	call := newPendingCall()
	d.calls["x"] = call
	d.connectionLost(nil)
	d.calls["x"] = call
	done := make(chan struct{})
	go func() { d.connectionLost(nil); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connectionLost blocked: the supervisor would never reconnect")
	}
}

type failingConn struct{}

func (failingConn) Channel() (transport.Channel, error)             { return nil, errors.New("no channel") }
func (failingConn) NotifyClose(c chan *amqp.Error) chan *amqp.Error { return c }
func (failingConn) Close() error                                    { return nil }
func (failingConn) IsClosed() bool                                  { return false }

type deadChannel struct{ transport.Channel }

func (deadChannel) Close() error { return nil }

func TestReplyQueueRecoveryKeepsTryingAfterAFailure(t *testing.T) {
	b := bareBus()
	defer b.sess.cancel()
	d := b.dispatcher
	b.sess.conn = failingConn{}
	b.sess.markReady()
	dead := &deadChannel{}
	d.consumeCh, d.replyQueue = dead, "amq.gen-old"
	gen := d.replyGen
	if retry, _ := d.tryRecoverReplies(gen); !retry {
		t.Fatal("a failed rebuild must ask to be retried")
	}
	if retry, _ := d.tryRecoverReplies(gen); !retry {
		t.Fatal("the second attempt must not mistake its own cleanup for someone else's repair")
	}
}

type unhashable struct{ fields []string }

func (u unhashable) Error() string { return fmt.Sprint(u.fields) }

func TestErrorCodeOfAnUnhashableError(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", unhashable{fields: []string{"a"}})
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ErrorCode panicked: %v", r)
		}
	}()
	if code := ErrorCode(err); code != "" {
		t.Fatalf("code %q", code)
	}
	_ = safeErrorSummary(err)
}

func TestEventHandlerReturningAnUnhashableErrorDoesNotCrash(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("U.Events")
	t.Cleanup(func() { _ = l.Close() })
	ran := make(chan struct{}, 1)
	_ = Subscribe(testCtx(t), l, func(context.Context, *testpb.Ping, EventInfo) error {
		ran <- struct{}{}
		return unhashable{fields: []string{"x"}}
	})
	_ = l.Start(testCtx(t))
	_ = bus.PublishEvent(testCtx(t), &testpb.Ping{})
	recvWithin(t, ran, 2*time.Second)
	eventually(t, "rejected", func() bool { return len(b.OpsOf("reject")) == 1 })
}

func TestFailedStartCanBeRetried(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	// Occupy the retry queue with other arguments so Start fails.
	_ = startCalc(t, dialTest(t, b, fastConfig()), newCalcImpl(), WithRetry(RetryPolicy{MaxRetries: 1, Delay: time.Second}))
	svc, _ := bus.Register(&calcServiceDesc, newCalcImpl(), WithRetry(RetryPolicy{MaxRetries: 1, Delay: 2 * time.Second}))
	if err := svc.Start(testCtx(t)); !errors.Is(err, ErrRetryQueueMismatch) {
		t.Fatalf("got %v", err)
	}
	if info, _ := b.Queue("Test.Calc"); info.Consumers != 1 {
		t.Fatalf("a failed Start must not leave its consumer running (consumers %d)", info.Consumers)
	}
	l, _ := bus.NewEventListener("")
	b.SetDialFault(nil)
	if err := l.Start(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
}

func TestClosedListenerIsNotRestoredAfterAReconnection(t *testing.T) {
	b := fakebroker.New()
	obs, wait := reconnected(t)
	bus := dialTest(t, b, fastConfig(), obs)
	l, _ := bus.NewEventListener("Gone.Events")
	_ = Subscribe(testCtx(t), l, func(context.Context, *testpb.Ping, EventInfo) error { return nil })
	_ = l.Start(testCtx(t))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	b.KillConnections()
	wait()
	time.Sleep(20 * time.Millisecond)
	if info, _ := b.Queue("Gone.Events"); info.Consumers != 0 {
		t.Fatal("a closed listener came back after the reconnection")
	}
	l.consumer.mu.Lock()
	defer l.consumer.mu.Unlock()
	if l.consumer.ch != nil {
		t.Fatal("a closed listener must not hold a channel")
	}
}

func TestShutdownStopsStandaloneListeners(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	l, _ := bus.NewEventListener("Solo.Events")
	_ = Subscribe(testCtx(t), l, func(context.Context, *testpb.Ping, EventInfo) error { return nil })
	_ = l.Start(testCtx(t))
	if err := bus.Shutdown(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if info, _ := b.Queue("Solo.Events"); info.Consumers != 0 {
		t.Fatal("Shutdown must stop every listener, not only services")
	}
}

func TestEarlyAckFailureSkipsTheHandler(t *testing.T) {
	// A failed early ack means the broker will redeliver: running the handler
	// as well breaks the at-most-once promise of early ack.
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := &countingCalc{calcImpl: newCalcImpl()}
	svc := startCalc(t, bus, impl, WithEarlyAck())
	c := svc.requests
	c.mu.Lock()
	pub := c.pub
	c.mu.Unlock()
	d := &amqp.Delivery{Acknowledger: failingAcker{}, RoutingKey: "REQUEST.Test.Calc.add", Body: (&rawPeer{}).request("Test.Calc.add", &testpb.AddRequest{})}
	bus.deliveries.add()
	c.process(d, pub)
	bus.deliveries.done()
	if impl.adds.Load() != 0 {
		t.Fatal("the handler ran although the early ack failed")
	}
}

type failingAcker struct{}

func (failingAcker) Ack(uint64, bool) error        { return amqp.ErrClosed }
func (failingAcker) Nack(uint64, bool, bool) error { return amqp.ErrClosed }
func (failingAcker) Reject(uint64, bool) error     { return amqp.ErrClosed }

type countingCalc struct {
	*calcImpl
	adds atomicCounter
}

func (c *countingCalc) Add(ctx context.Context, in *testpb.AddRequest) (*testpb.AddResponse, error) {
	c.adds.Add(1)
	return c.calcImpl.Add(ctx, in)
}
