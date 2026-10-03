package protobus

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

// recordingComponent records restore and disconnect calls.
type recordingComponent struct {
	name    string
	log     *eventLog
	restore func(ctx context.Context, conn transport.Conn) error
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

func (c *recordingComponent) restoreTopology(ctx context.Context, conn transport.Conn) error {
	c.log.add("restore:" + c.name)
	if c.restore != nil {
		return c.restore(ctx, conn)
	}
	return nil
}

func (c *recordingComponent) connectionLost(error) { c.log.add("lost:" + c.name) }

func newTestSession(t *testing.T, b *fakebroker.Broker, cfg Config) (*session, *eventLog) {
	t.Helper()
	events := &eventLog{}
	s := newSession(b.Dial, "amqp://guest:secret@fake/", cfg, testLogger(t), func(e ConnectionEvent) {
		events.add("event:" + e.Kind.String())
	})
	if err := s.connect(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	return s, events
}

func TestSessionConnectsAndIsReady(t *testing.T) {
	b := fakebroker.New()
	s, _ := newTestSession(t, b, fastConfig())
	if err := s.whenReady(testCtx(t)); err != nil {
		t.Fatalf("ready after connect: %v", err)
	}
	if _, _, ok := s.current(); !ok {
		t.Fatal("current() must report the live connection")
	}
}

func TestSessionInitialDialFailureIsReturnedNotRetried(t *testing.T) {
	b := fakebroker.New()
	b.SetDialFault(errors.New("connection refused"))
	s := newSession(b.Dial, "amqp://fake", fastConfig(), testLogger(t), nil)
	defer s.close()
	if err := s.connect(testCtx(t)); err == nil {
		t.Fatal("initial dial failure must be returned")
	}
	if b.Dials() != 1 {
		t.Fatalf("the first connection is not retried; dials=%d", b.Dials())
	}
}

func TestSessionReconnectsAndRestoresInRegistrationOrder(t *testing.T) {
	b := fakebroker.New()
	s, events := newTestSession(t, b, fastConfig())
	s.register(&recordingComponent{name: "callbacks", log: events})
	s.register(&recordingComponent{name: "dispatcher", log: events})
	s.register(&recordingComponent{name: "service", log: events})

	b.KillConnections()
	eventually(t, "reconnected", func() bool { return slices.Contains(events.snapshot(), "event:reconnected") })

	want := []string{
		"lost:callbacks", "lost:dispatcher", "lost:service", "event:disconnected",
		"event:reconnecting",
		"restore:callbacks", "restore:dispatcher", "restore:service",
		"event:reconnected",
	}
	if got := events.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("events\n got %v\nwant %v", got, want)
	}
	if err := s.whenReady(testCtx(t)); err != nil {
		t.Fatal(err)
	}
}

func TestSessionWithholdsReadinessUntilRestorersFinish(t *testing.T) {
	b := fakebroker.New()
	s, events := newTestSession(t, b, fastConfig())
	release := make(chan struct{})
	s.register(&recordingComponent{name: "slow", log: events, restore: func(ctx context.Context, _ transport.Conn) error {
		<-release
		return nil
	}})
	b.KillConnections()
	eventually(t, "restore started", func() bool { return slices.Contains(events.snapshot(), "restore:slow") })

	readyErr := make(chan error, 1)
	go func() { readyErr <- s.whenReady(context.Background()) }()
	select {
	case err := <-readyErr:
		t.Fatalf("whenReady returned %v while a restorer was still running", err)
	case <-time.After(30 * time.Millisecond):
	}
	if _, _, ok := s.current(); ok {
		t.Fatal("current() must not report ready mid-restore")
	}
	close(release)
	if err := recvWithin(t, readyErr, 2*time.Second); err != nil {
		t.Fatalf("parked waiter must resolve once restored: %v", err)
	}
}

func TestSessionRetriesWhenARestorerFails(t *testing.T) {
	b := fakebroker.New()
	s, events := newTestSession(t, b, fastConfig())
	var calls atomic.Int32
	var conns []transport.Conn
	var mu sync.Mutex
	s.register(&recordingComponent{name: "flaky", log: events, restore: func(_ context.Context, c transport.Conn) error {
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
		if calls.Add(1) < 3 {
			return errors.New("queue declare failed")
		}
		return nil
	}})
	b.KillConnections()
	eventually(t, "reconnected", func() bool { return slices.Contains(events.snapshot(), "event:reconnected") })
	if calls.Load() != 3 {
		t.Fatalf("restore attempts %d, want 3", calls.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for i, c := range conns[:2] {
		if !c.IsClosed() {
			t.Fatalf("connection %d could not be restored and must be discarded, not left open", i)
		}
	}
	if conns[2].IsClosed() {
		t.Fatal("the restored connection must stay open")
	}
}

func TestSessionGivesUpAfterMaxRetries(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.Reconnect.MaxRetries = 3
	s, events := newTestSession(t, b, cfg)

	b.SetDialFault(errors.New("connection refused"))
	waiter := make(chan error, 1)
	b.KillConnections()
	eventually(t, "disconnected", func() bool { return slices.Contains(events.snapshot(), "event:disconnected") })
	go func() { waiter <- s.whenReady(context.Background()) }()

	select {
	case <-s.done():
	case <-time.After(3 * time.Second):
		t.Fatal("the session must report done after giving up")
	}
	if !errors.Is(s.err(), ErrNotReady) {
		t.Fatalf("err() = %v, want ErrNotReady", s.err())
	}
	if err := recvWithin(t, waiter, time.Second); !errors.Is(err, ErrNotReady) {
		t.Fatalf("a parked waiter must be failed when the session gives up, got %v", err)
	}
	if got := b.Dials(); got != 1+3 {
		t.Fatalf("dials %d, want initial + 3 attempts", got)
	}
	if !slices.Contains(events.snapshot(), "event:gave-up") {
		t.Fatalf("events %v", events.snapshot())
	}
}

func TestSessionWhenReadyTimesOut(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.ConnectionReadyTimeout = 40 * time.Millisecond
	s, events := newTestSession(t, b, cfg)
	b.SetDialFault(errors.New("down"))
	b.KillConnections()
	eventually(t, "disconnected", func() bool { return slices.Contains(events.snapshot(), "event:disconnected") })
	start := time.Now()
	err := s.whenReady(context.Background())
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("got %v, want ErrNotReady", err)
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("whenReady must wait out the ready timeout")
	}
	b.SetDialFault(nil)
}

func TestSessionWhenReadyHonoursContext(t *testing.T) {
	b := fakebroker.New()
	s, events := newTestSession(t, b, fastConfig())
	b.SetDialFault(errors.New("down"))
	b.KillConnections()
	eventually(t, "disconnected", func() bool { return slices.Contains(events.snapshot(), "event:disconnected") })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.whenReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	b.SetDialFault(nil)
}

func TestSessionCloseDuringOutageStopsReconnecting(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.Reconnect.MaxRetries = 0 // forever
	s, events := newTestSession(t, b, cfg)
	b.SetDialFault(errors.New("down"))
	b.KillConnections()
	eventually(t, "reconnecting", func() bool { return slices.Contains(events.snapshot(), "event:reconnecting") })

	waiter := make(chan error, 1)
	go func() { waiter <- s.whenReady(context.Background()) }()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if err := recvWithin(t, waiter, time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("parked waiter after close: %v", err)
	}
	dials := b.Dials()
	b.SetDialFault(nil)
	time.Sleep(50 * time.Millisecond)
	if b.Dials() != dials {
		t.Fatal("a closed session must not dial again")
	}
	if err := s.whenReady(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("whenReady after close: %v", err)
	}
}

func TestSessionCloseIsIdempotent(t *testing.T) {
	b := fakebroker.New()
	s, _ := newTestSession(t, b, fastConfig())
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	<-s.done()
	if !errors.Is(s.err(), ErrClosed) {
		t.Fatalf("err() after close = %v", s.err())
	}
}

func TestSessionDropDuringRestoreDoesNotFork(t *testing.T) {
	// The socket dies while a restorer is mid-flight. The attempt must fail
	// and be retried by the one supervisor, never running two lineages that
	// both end up announcing themselves ready.
	b := fakebroker.New()
	s, events := newTestSession(t, b, fastConfig())
	var calls atomic.Int32
	s.register(&recordingComponent{name: "victim", log: events, restore: func(_ context.Context, c transport.Conn) error {
		if calls.Add(1) == 1 {
			b.KillConnections() // the broker goes away again, mid-restore
			if _, err := c.Channel(); err != nil {
				return err
			}
		}
		return nil
	}})
	b.KillConnections()
	eventually(t, "reconnected", func() bool { return slices.Contains(events.snapshot(), "event:reconnected") })
	time.Sleep(50 * time.Millisecond)
	n := 0
	for _, e := range events.snapshot() {
		if e == "event:reconnected" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("reconnected announced %d times: %v", n, events.snapshot())
	}
	conn, _, ok := s.current()
	if !ok || conn.IsClosed() {
		t.Fatal("the surviving connection must be live")
	}
}

func TestSessionUnregisteredComponentIsNotRestored(t *testing.T) {
	b := fakebroker.New()
	s, events := newTestSession(t, b, fastConfig())
	unregister := s.register(&recordingComponent{name: "gone", log: events})
	unregister()
	unregister() // idempotent
	b.KillConnections()
	eventually(t, "reconnected", func() bool { return slices.Contains(events.snapshot(), "event:reconnected") })
	for _, e := range events.snapshot() {
		if e == "restore:gone" || e == "lost:gone" {
			t.Fatalf("unregistered component was called: %v", events.snapshot())
		}
	}
}

func TestBackoffGrowsCapsAndJitters(t *testing.T) {
	p := ReconnectPolicy{InitialDelay: 100 * time.Millisecond, MaxDelay: time.Second, Multiplier: 2}
	for attempt, base := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 5: time.Second, 50: time.Second} {
		for range 50 {
			d := p.backoff(attempt)
			if d < base || d > base+base*3/10 {
				t.Fatalf("attempt %d: %v outside [%v, %v]", attempt, d, base, base+base*3/10)
			}
		}
	}
}

func TestConnectionEventKindNames(t *testing.T) {
	for k, want := range map[ConnectionEventKind]string{
		EventDisconnected: "disconnected", EventReconnecting: "reconnecting",
		EventReconnected: "reconnected", EventGaveUp: "gave-up",
	} {
		if k.String() != want {
			t.Errorf("%d: %q", k, k.String())
		}
	}
}
