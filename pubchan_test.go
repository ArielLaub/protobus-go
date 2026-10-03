package protobus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

func newTestPubChannel(t *testing.T, b *fakebroker.Broker, cfg Config) (*pubChannel, transport.Conn) {
	t.Helper()
	conn, err := b.Dial(context.Background(), "x", amqp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	pc, err := newPubChannel(ch, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pc.close)
	setup, _ := conn.Channel()
	_ = setup.ExchangeDeclare("ex", "topic", true, false, false, false, nil)
	_, _ = setup.QueueDeclare("q", true, false, false, false, nil)
	_ = setup.QueueBind("q", "k", "ex", false, nil)
	_ = setup.Close()
	return pc, conn
}

func pub(id string) amqp.Publishing { return amqp.Publishing{MessageId: id, Body: []byte("x")} }

func TestPubChannelResolvesOnConfirm(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	if err := pc.publish(testCtx(t), "ex", "k", false, pub("m1")); err != nil {
		t.Fatal(err)
	}
	if b.QueueDepth("q") != 1 {
		t.Fatal("message must be routed")
	}
}

func TestPubChannelUsesAConfirmChannel(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Nack })
	err := pc.publish(testCtx(t), "ex", "k", false, pub("m1"))
	if !errors.Is(err, ErrPublishNacked) {
		t.Fatalf("a nack must surface; got %v", err)
	}
	var pe *PublishError
	if !errors.As(err, &pe) || pe.MessageID != "m1" || pe.Ambiguous() {
		t.Fatalf("unexpected %+v", pe)
	}
}

func TestPubChannelMandatoryUnroutable(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	err := pc.publish(testCtx(t), "ex", "nowhere", true, pub("m1"))
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("got %v, want ErrUnroutable", err)
	}
	// Not mandatory: an unroutable event is normal and succeeds.
	if err := pc.publish(testCtx(t), "ex", "nowhere", false, pub("m2")); err != nil {
		t.Fatalf("non-mandatory unroutable publish: %v", err)
	}
}

func TestPubChannelStaleReturnDoesNotFailALaterPublish(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	_ = pc.publish(testCtx(t), "ex", "nowhere", true, pub("same"))
	// The same id, reused by a caller-driven republish, now routes.
	if err := pc.publish(testCtx(t), "ex", "k", true, pub("same")); err != nil {
		t.Fatalf("a return for an earlier publish must not fail this one: %v", err)
	}
}

func TestPubChannelMintsAMessageID(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	_ = pc.publish(testCtx(t), "ex", "k", false, amqp.Publishing{})
	_ = pc.publish(testCtx(t), "ex", "k", false, amqp.Publishing{})
	_ = pc.publish(testCtx(t), "ex", "k", false, pub("mine"))
	ops := b.OpsOf("publish")
	if len(ops) != 3 || ops[0].MessageID == "" || ops[0].MessageID == ops[1].MessageID {
		t.Fatalf("every publish carries a fresh id when none is given: %+v", ops)
	}
	if ops[2].MessageID != "mine" {
		t.Fatalf("a caller-supplied id is kept, got %q", ops[2].MessageID)
	}
}

func TestPubChannelConfirmTimeoutIsAmbiguous(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.PublishConfirmTimeout = 30 * time.Millisecond
	pc, _ := newTestPubChannel(t, b, cfg)
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Drop })
	err := pc.publish(testCtx(t), "ex", "k", false, pub("m1"))
	var pe *PublishError
	if !errors.As(err, &pe) || !errors.Is(err, ErrPublishConfirmTimeout) || !pe.Ambiguous() {
		t.Fatalf("got %v", err)
	}
}

func TestPubChannelContextEndWhileAwaitingConfirmIsAmbiguous(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Drop })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := pc.publish(ctx, "ex", "k", false, pub("m1"))
	var pe *PublishError
	if !errors.As(err, &pe) || !errors.Is(err, context.DeadlineExceeded) || !pe.Ambiguous() {
		t.Fatalf("a deadline during the confirm wait leaves the outcome unknown: %v", err)
	}
}

func TestPubChannelFailsPendingPublishesWhenTheChannelCloses(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Drop })
	errs := make(chan error, 1)
	go func() { errs <- pc.publish(context.Background(), "ex", "k", false, pub("m1")) }()
	eventually(t, "publish sent", func() bool { return len(b.OpsOf("publish")) == 1 })
	b.KillConnections()
	err := recvWithin(t, errs, 2*time.Second)
	var pe *PublishError
	if !errors.As(err, &pe) || !errors.Is(err, ErrChannelClosed) || !pe.Ambiguous() {
		t.Fatalf("got %v", err)
	}
	select {
	case <-pc.closedCh():
	case <-time.After(time.Second):
		t.Fatal("the publisher must observe its channel closing")
	}
}

func TestPubChannelPublishAfterCloseIsNotSent(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	b.KillConnections()
	<-pc.closedCh()
	err := pc.publish(testCtx(t), "ex", "k", false, pub("m1"))
	if !errors.Is(err, errChannelGone) {
		t.Fatalf("a publish on a dead channel was never sent; got %v", err)
	}
	var pe *PublishError
	if errors.As(err, &pe) {
		t.Fatal("an unsent publish is not an ambiguous PublishError")
	}
}

func TestPubChannelBoundsOutstandingConfirms(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.MaxOutstandingConfirms = 2
	pc, _ := newTestPubChannel(t, b, cfg)

	var mu sync.Mutex
	hold := true
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction {
		mu.Lock()
		defer mu.Unlock()
		if hold {
			return fakebroker.Drop
		}
		return fakebroker.Ack
	})
	for i := range 2 {
		go func() { _ = pc.publish(context.Background(), "ex", "k", false, pub(string(rune('a'+i)))) }()
	}
	eventually(t, "two in flight", func() bool { return len(b.OpsOf("publish")) == 2 })

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err := pc.publish(ctx, "ex", "k", false, pub("third"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a third publish must wait for a slot; got %v", err)
	}
	var pe *PublishError
	if errors.As(err, &pe) {
		t.Fatal("waiting for a slot sends nothing, so the outcome is not ambiguous")
	}
	if n := len(b.OpsOf("publish")); n != 2 {
		t.Fatalf("the third publish must not reach the broker; publishes=%d", n)
	}
	mu.Lock()
	hold = false
	mu.Unlock()
}

func TestPubChannelConcurrentPublishes(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for range 200 {
		wg.Go(func() {
			if err := pc.publish(context.Background(), "ex", "k", true, amqp.Publishing{}); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if b.QueueDepth("q") != 200 {
		t.Fatalf("depth %d", b.QueueDepth("q"))
	}
}
