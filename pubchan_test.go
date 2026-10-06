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

func TestPubChannelAttributesAReturnToItsDestination(t *testing.T) {
	// Two publishes share a message id (a caller-driven republish, or a retry
	// and a dead-letter copy of one delivery): only the one that went nowhere
	// is unroutable, however the confirms interleave.
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	b.SetConfirmDelay(func(p fakebroker.Published) time.Duration {
		if p.Key == "k" {
			return 100 * time.Millisecond // the routed publish's confirm is late (a slow fsync)
		}
		return 0
	})
	routed := make(chan error, 1)
	go func() { routed <- pc.publish(context.Background(), "ex", "k", true, pub("same")) }()
	eventually(t, "first publish sent", func() bool { return len(b.OpsOf("publish")) == 1 })
	lost := pc.publish(testCtx(t), "ex", "nowhere", true, pub("same"))
	if !errors.Is(lost, ErrUnroutable) {
		t.Fatalf("the unroutable copy: %v", lost)
	}
	if err := recvWithin(t, routed, 2*time.Second); err != nil {
		t.Fatalf("the routed copy must not be blamed for the other's return: %v", err)
	}
}

// ---- cancellation, capacity and return attribution --------------------------

// gatedChannel blocks chosen publishes inside PublishWithContext, as a
// transport write stuck on a full socket does. amqp091 does not interrupt
// such a write when the context ends, so neither does this.
type gatedChannel struct {
	transport.Channel
	mu      sync.Mutex
	gates   map[string]chan struct{} // message id -> released when closed
	entered chan string
}

func (g *gatedChannel) gate(id string) (release func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := make(chan struct{})
	g.gates[id] = c
	return sync.OnceFunc(func() { close(c) })
}

func (g *gatedChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	c := g.gates[msg.MessageId]
	g.mu.Unlock()
	if c != nil {
		g.entered <- msg.MessageId
		<-c
	}
	return g.Channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func newGatedPubChannel(t *testing.T, b *fakebroker.Broker, cfg Config) (*pubChannel, *gatedChannel) {
	t.Helper()
	conn, err := b.Dial(context.Background(), "x", amqp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	raw, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	g := &gatedChannel{Channel: raw, gates: map[string]chan struct{}{}, entered: make(chan string, 16)}
	pc, err := newPubChannel(g, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pc.close)
	setup, _ := conn.Channel()
	_ = setup.ExchangeDeclare("ex", "topic", true, false, false, false, nil)
	_, _ = setup.QueueDeclare("q", true, false, false, false, nil)
	_ = setup.QueueBind("q", "k", "ex", false, nil)
	_ = setup.Close()
	return pc, g
}

func publishedIDs(b *fakebroker.Broker) []string {
	var ids []string
	for _, op := range b.OpsOf("publish") {
		ids = append(ids, op.MessageID)
	}
	return ids
}

// unresolved is how many publishes hold confirm capacity.
func (p *pubChannel) unresolved() int { return len(p.slots) }

func isDefinite(err error) bool {
	var pe *PublishError
	return !errors.As(err, &pe)
}

func TestPubChannelCancelBeforeAdmissionSendsNothing(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.MaxOutstandingConfirms = 1
	pc, _ := newTestPubChannel(t, b, cfg)
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Hold })
	first := make(chan error, 1)
	go func() { first <- pc.publish(context.Background(), "ex", "k", false, pub("first")) }()
	eventually(t, "first sent", func() bool { return len(b.OpsOf("publish")) == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() { errs <- pc.publish(ctx, "ex", "k", false, pub("second")) }()
	cancel()
	err := recvWithin(t, errs, time.Second)
	if !errors.Is(err, context.Canceled) || !isDefinite(err) {
		t.Fatalf("a publish cancelled before admission was never sent: %v", err)
	}
	b.ReleaseHeld()
	if err := recvWithin(t, first, time.Second); err != nil {
		t.Fatal(err)
	}
	if ids := publishedIDs(b); len(ids) != 1 {
		t.Fatalf("only the first publish may reach the broker: %v", ids)
	}
	eventually(t, "capacity released", func() bool { return pc.unresolved() == 0 })
}

func TestPubChannelCancelWhileAwaitingTheSendPathSendsNothing(t *testing.T) {
	b := fakebroker.New()
	pc, g := newGatedPubChannel(t, b, fastConfig())
	release := g.gate("first")
	defer release()
	first := make(chan error, 1)
	go func() { first <- pc.publish(context.Background(), "ex", "k", false, pub("first")) }()
	recvWithin(t, g.entered, time.Second) // the first write is stuck

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() { errs <- pc.publish(ctx, "ex", "k", false, pub("second")) }()
	eventually(t, "second admitted", func() bool { return pc.unresolved() == 2 })
	cancel()
	err := recvWithin(t, errs, time.Second)
	if !errors.Is(err, context.Canceled) || !isDefinite(err) {
		t.Fatalf("a publish cancelled while waiting for the send path was never sent: %v", err)
	}
	release()
	if err := recvWithin(t, first, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if ids := publishedIDs(b); len(ids) != 1 || ids[0] != "first" {
		t.Fatalf("the cancelled publish must never be transmitted: %v", ids)
	}
	eventually(t, "capacity released", func() bool { return pc.unresolved() == 0 })
}

func TestPubChannelCancelDuringABlockedWriteIsAmbiguousAndKeepsCapacity(t *testing.T) {
	b := fakebroker.New()
	pc, g := newGatedPubChannel(t, b, fastConfig())
	release := g.gate("stuck")
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() { errs <- pc.publish(ctx, "ex", "k", false, pub("stuck")) }()
	recvWithin(t, g.entered, time.Second)
	cancel()
	err := recvWithin(t, errs, time.Second)
	var pe *PublishError
	if !errors.As(err, &pe) || !pe.Ambiguous() || !errors.Is(err, context.Canceled) {
		t.Fatalf("a caller giving up mid-write cannot know whether it was sent: %v", err)
	}
	if pc.unresolved() != 1 {
		t.Fatalf("the abandoned write still holds its capacity; unresolved=%d", pc.unresolved())
	}
	release()
	eventually(t, "write settled by its confirm", func() bool { return pc.unresolved() == 0 })
	if ids := publishedIDs(b); len(ids) != 1 {
		t.Fatalf("published %v", ids)
	}
}

func TestPubChannelSendPathWaitIsBoundedByTheDeadline(t *testing.T) {
	// The RPC path passes its deadline as the context: it must cover the wait
	// for the send path, not only the wait for a confirm.
	b := fakebroker.New()
	pc, g := newGatedPubChannel(t, b, fastConfig())
	release := g.gate("first")
	defer release()
	go func() { _ = pc.publish(context.Background(), "ex", "k", false, pub("first")) }()
	recvWithin(t, g.entered, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := pc.publish(ctx, "ex", "k", false, pub("second"))
	if !errors.Is(err, context.DeadlineExceeded) || !isDefinite(err) || time.Since(start) > time.Second {
		t.Fatalf("got %v after %v", err, time.Since(start))
	}
}

func TestPubChannelSendPathWaitIsBoundedWithoutADeadline(t *testing.T) {
	// Settlement publishes run on a background context; the confirm timeout
	// bounds their wait for the send path too, as a definite failure.
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.PublishConfirmTimeout = 40 * time.Millisecond
	pc, g := newGatedPubChannel(t, b, cfg)
	release := g.gate("first")
	defer release()
	go func() { _ = pc.publish(context.Background(), "ex", "k", false, pub("first")) }()
	recvWithin(t, g.entered, time.Second)
	errs := make(chan error, 1)
	go func() { errs <- pc.publish(context.Background(), "ex", "k", false, pub("second")) }()
	err := recvWithin(t, errs, time.Second)
	if err == nil || !isDefinite(err) {
		t.Fatalf("the send path never freed up: a definite failure, got %v", err)
	}
	release()
	time.Sleep(20 * time.Millisecond)
	if ids := publishedIDs(b); len(ids) != 1 {
		t.Fatalf("the timed-out publish must never be transmitted: %v", ids)
	}
}

func TestPubChannelTimeoutsDoNotReleaseConfirmCapacity(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.MaxOutstandingConfirms = 1
	cfg.PublishConfirmTimeout = 20 * time.Millisecond
	pc, _ := newTestPubChannel(t, b, cfg)
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Drop })
	for i := range 10 {
		_ = pc.publish(context.Background(), "ex", "k", false, pub(string(rune('a'+i))))
	}
	if n := len(b.OpsOf("publish")); n > cfg.MaxOutstandingConfirms {
		t.Fatalf("%d publishes sent with none confirmed; the bound is %d", n, cfg.MaxOutstandingConfirms)
	}
}

func TestPubChannelCancellationsDoNotReleaseConfirmCapacity(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.MaxOutstandingConfirms = 2
	pc, _ := newTestPubChannel(t, b, cfg)
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Hold })
	for i := range 6 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
		_ = pc.publish(ctx, "ex", "k", false, pub(string(rune('a'+i))))
		cancel()
	}
	if n := len(b.OpsOf("publish")); n != 2 {
		t.Fatalf("abandoned publishes still await their confirm: %d sent, bound 2", n)
	}
	if pc.unresolved() != 2 {
		t.Fatalf("unresolved=%d", pc.unresolved())
	}
	// Late confirms settle them, releasing the capacity exactly once.
	b.SetConfirmPolicy(nil)
	b.ReleaseHeld()
	eventually(t, "late confirms settle", func() bool { return pc.unresolved() == 0 })
	if err := pc.publish(testCtx(t), "ex", "k", false, pub("after")); err != nil {
		t.Fatal(err)
	}
	if pc.unresolved() != 0 {
		t.Fatalf("capacity released more or less than once: unresolved=%d", pc.unresolved())
	}
}

func TestPubChannelRetiresAChannelThatStopsConfirming(t *testing.T) {
	// A channel whose capacity is held by overdue publishes is wedged:
	// retiring it fails them as ambiguous and lets its owner open a new one.
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.MaxOutstandingConfirms = 1
	cfg.PublishConfirmTimeout = 20 * time.Millisecond
	pc, _ := newTestPubChannel(t, b, cfg)
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Drop })
	err := pc.publish(context.Background(), "ex", "k", false, pub("lost"))
	if !errors.Is(err, ErrPublishConfirmTimeout) {
		t.Fatal(err)
	}
	err = pc.publish(context.Background(), "ex", "k", false, pub("blocked"))
	if err == nil || !isDefinite(err) {
		t.Fatalf("no capacity: a definite failure, got %v", err)
	}
	select {
	case <-pc.closedCh():
	case <-time.After(time.Second):
		t.Fatal("a wedged channel must be retired")
	}
	eventually(t, "retirement settles what the channel held", func() bool { return pc.unresolved() == 0 })
	if err := pc.publish(context.Background(), "ex", "k", false, pub("later")); !errors.Is(err, errChannelGone) {
		t.Fatalf("a retired channel sends nothing: %v", err)
	}
}

func TestPubChannelClosureSettlesAbandonedPublishes(t *testing.T) {
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Hold })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_ = pc.publish(ctx, "ex", "k", false, pub("a"))
	if pc.unresolved() != 1 {
		t.Fatal("the abandoned publish awaits its confirm")
	}
	b.KillConnections()
	<-pc.closedCh()
	eventually(t, "closure settles it", func() bool { return pc.unresolved() == 0 })
}

func TestPubChannelLateReturnDoesNotFailALaterAttempt(t *testing.T) {
	// The first attempt is unroutable and its caller gives up before the
	// broker answers. A binding appears; a second attempt with the same id and
	// destination routes. The first attempt's late return must not be read as
	// the second's verdict.
	b := fakebroker.New()
	pc, conn := newTestPubChannel(t, b, fastConfig())
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Hold })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := pc.publish(ctx, "ex", "late", true, pub("same"))
	var pe *PublishError
	if !errors.As(err, &pe) || !pe.Ambiguous() {
		t.Fatalf("first attempt: %v", err)
	}
	setup, _ := conn.Channel()
	_ = setup.QueueBind("q", "late", "ex", false, nil)
	_ = setup.Close()

	second := make(chan error, 1)
	go func() { second <- pc.publish(context.Background(), "ex", "late", true, pub("same")) }()
	eventually(t, "second sent", func() bool { return len(b.OpsOf("publish")) == 2 })
	b.ReleaseHeld() // the first's return and confirm, then the second's confirm
	if err := recvWithin(t, second, time.Second); err != nil {
		t.Fatalf("the routed second attempt was blamed for the first's return: %v", err)
	}
	eventually(t, "both settled", func() bool { return pc.unresolved() == 0 })
}

func TestPubChannelReturnAfterRetirementReachesNoOtherChannel(t *testing.T) {
	b := fakebroker.New()
	old, _ := newTestPubChannel(t, b, fastConfig())
	b.SetConfirmPolicy(func(fakebroker.Published) fakebroker.ConfirmAction { return fakebroker.Hold })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_ = old.publish(ctx, "ex", "nowhere", true, pub("same"))
	old.close()
	b.SetConfirmPolicy(nil)
	fresh, conn := newTestPubChannel(t, b, fastConfig())
	setup, _ := conn.Channel()
	_ = setup.QueueBind("q", "nowhere", "ex", false, nil)
	_ = setup.Close()
	b.ReleaseHeld()
	if err := fresh.publish(testCtx(t), "ex", "nowhere", true, pub("same")); err != nil {
		t.Fatalf("a publish on a fresh channel is judged by its own outcome: %v", err)
	}
}

func TestPubChannelFastConfirmKeepsItsVerdict(t *testing.T) {
	// The confirm can settle a publication before its caller has seen the
	// writer commit to it; the broker's verdict must survive that race.
	b := fakebroker.New()
	pc, _ := newTestPubChannel(t, b, fastConfig())
	for i := range 300 {
		if err := pc.publish(testCtx(t), "ex", "nowhere", true, amqp.Publishing{}); !errors.Is(err, ErrUnroutable) {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
}
