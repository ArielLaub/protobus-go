package protobus

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

// ConnectionEventKind names a change in the broker connection's state.
type ConnectionEventKind int

const (
	// EventDisconnected: the connection was lost unexpectedly. Calls in
	// flight fail with ErrDisconnected; new publishes wait for readiness.
	EventDisconnected ConnectionEventKind = iota + 1
	// EventReconnecting: a reconnection attempt is scheduled after Delay.
	EventReconnecting
	// EventReconnected: the connection and every component's topology are
	// back. Traffic flows again.
	EventReconnected
	// EventGaveUp: Config.Reconnect.MaxRetries consecutive attempts failed.
	// The Bus is finished; Done is closed and Err reports why.
	EventGaveUp
)

func (k ConnectionEventKind) String() string {
	switch k {
	case EventDisconnected:
		return "disconnected"
	case EventReconnecting:
		return "reconnecting"
	case EventReconnected:
		return "reconnected"
	case EventGaveUp:
		return "gave-up"
	}
	return fmt.Sprintf("ConnectionEventKind(%d)", int(k))
}

// ConnectionEvent describes a connection state change. Observers registered
// with WithConnectionObserver receive them in order, on the goroutine that
// supervises the connection, so an observer must not block.
type ConnectionEvent struct {
	Kind    ConnectionEventKind
	Attempt int           // EventReconnecting, EventReconnected: the attempt number
	Delay   time.Duration // EventReconnecting: the wait before the attempt
	Err     error         // EventDisconnected: the cause; EventGaveUp: the last failure
}

// backoff is the wait before reconnection attempt n (1-based): exponential,
// capped at MaxDelay, plus up to 30% jitter so a fleet of clients does not
// reconnect in lockstep after a broker restart.
func (p ReconnectPolicy) backoff(attempt int) time.Duration {
	base := float64(p.InitialDelay)
	for i := 1; i < attempt && base < float64(p.MaxDelay); i++ {
		base *= p.Multiplier
	}
	base = min(base, float64(p.MaxDelay))
	return time.Duration(base + rand.Float64()*0.3*base)
}

// component is anything holding broker state that must be rebuilt after a
// reconnection: channels, queues, bindings, consumers.
type component interface {
	// restoreTopology rebuilds the component on a fresh connection. It runs
	// on the supervisor goroutine, in registration order, before the
	// connection is reported ready; an error fails the attempt, which is
	// retried on a new connection.
	restoreTopology(ctx context.Context, conn transport.Conn) error
	// connectionLost is told the connection went away. It must not block.
	connectionLost(cause error)
}

type registration struct{ c component }

// session owns the broker connection: the initial dial, unexpected-close
// detection, reconnection with backoff, coordinated restoration and
// readiness. Exactly one supervisor goroutine reconnects, so a socket that
// drops mid-restore can never fork two connection lineages.
type session struct {
	dial    transport.Dialer
	url     string
	cfg     Config
	log     *slog.Logger
	observe func(ConnectionEvent)
	name    string

	ctx    context.Context // cancelled by close
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	conn    transport.Conn
	gen     uint64
	ready   chan struct{} // closed while the connection carries traffic
	isReady bool
	comps   []*registration

	doneOnce sync.Once
	doneCh   chan struct{}
	doneErr  error

	// restoreMu serialises topology work: reconnection restores and a
	// component repairing its own channel never interleave.
	restoreMu sync.Mutex
}

func newSession(dial transport.Dialer, url string, cfg Config, log *slog.Logger, observe func(ConnectionEvent)) *session {
	ctx, cancel := context.WithCancel(context.Background())
	return &session{
		dial: dial, url: url, cfg: cfg, log: log, observe: observe, name: "protobus-go",
		ctx: ctx, cancel: cancel, ready: make(chan struct{}), doneCh: make(chan struct{}),
	}
}

func (s *session) emit(e ConnectionEvent) {
	if s.observe != nil {
		s.observe(e)
	}
}

func (s *session) amqpConfig() amqp.Config {
	return amqp.Config{
		Heartbeat: s.cfg.Heartbeat,
		Locale:    "en_US",
		Properties: amqp.Table{
			"connection_name": s.name,
			"product":         "protobus-go",
			"version":         Version,
			"platform":        "Go",
		},
	}
}

func (s *session) dialOnce(ctx context.Context) (transport.Conn, error) {
	s.log.LogAttrs(ctx, slog.LevelInfo, "connecting to bus", attrOperation("connect"), slog.String("url", RedactURL(s.url)))
	conn, err := s.dial(ctx, s.url, s.amqpConfig())
	if err != nil {
		// The client library's error may quote the URL; log only its class.
		s.log.LogAttrs(ctx, slog.LevelError, "failed to connect", attrOperation("connect"), attrOutcome(outcomeFailed), attrSafeError(err))
		return nil, err
	}
	return conn, nil
}

// connect makes the first connection. It is not retried: a process that
// cannot reach its broker at startup should fail visibly and be restarted by
// its supervisor, rather than appear healthy while serving nothing.
func (s *session) connect(ctx context.Context) error {
	conn, err := s.dialOnce(ctx)
	if err != nil {
		return fmt.Errorf("protobus: connect: %w", err)
	}
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		_ = conn.Close()
		return ErrClosed
	}
	s.conn = conn
	s.gen++
	s.mu.Unlock()
	s.markReady()
	s.log.LogAttrs(ctx, slog.LevelInfo, "connected to bus", attrOperation("connect"), attrOutcome(outcomeOK))
	s.wg.Add(1)
	go s.supervise(closed)
	return nil
}

func (s *session) supervise(closed chan *amqp.Error) {
	defer s.wg.Done()
	for {
		var cause error
		select {
		case e, ok := <-closed:
			if ok && e != nil {
				cause = e
			} else {
				cause = amqp.ErrClosed
			}
		case <-s.ctx.Done():
			return
		}
		if s.ctx.Err() != nil {
			return // a deliberate close, not a failure
		}
		s.lost(cause)
		if closed = s.reconnect(); closed == nil {
			return
		}
	}
}

func (s *session) lost(cause error) {
	s.markNotReady()
	s.log.LogAttrs(s.ctx, slog.LevelWarn, "connection lost", attrOperation("connect"), attrSafeError(cause))
	for _, r := range s.registrations() {
		r.c.connectionLost(cause)
	}
	s.emit(ConnectionEvent{Kind: EventDisconnected, Err: cause})
}

// reconnect retries until a connection is up and restored, returning its
// close-notification channel, or nil once the session is closed or gives up.
func (s *session) reconnect() chan *amqp.Error {
	var lastErr error
	for attempt := 1; ; attempt++ {
		if max := s.cfg.Reconnect.MaxRetries; max > 0 && attempt > max {
			s.giveUp(attempt-1, lastErr)
			return nil
		}
		delay := s.cfg.Reconnect.backoff(attempt)
		s.log.LogAttrs(s.ctx, slog.LevelInfo, "scheduling reconnection", attrOperation("reconnect"), attrAttempt(attempt), attrDuration(delay))
		s.emit(ConnectionEvent{Kind: EventReconnecting, Attempt: attempt, Delay: delay})
		if !sleepCtx(s.ctx, delay) {
			return nil
		}
		conn, err := s.dialOnce(s.ctx)
		if err != nil {
			lastErr = err
			continue
		}
		closed := conn.NotifyClose(make(chan *amqp.Error, 1))
		s.mu.Lock()
		s.conn = conn
		s.gen++
		s.mu.Unlock()

		if err := s.runRestorers(conn); err != nil {
			lastErr = err
			s.log.LogAttrs(s.ctx, slog.LevelError, "reconnection attempt failed during restore",
				attrOperation("reconnect"), attrAttempt(attempt), attrOutcome(outcomeFailed), attrSafeError(err))
			// A connection that is up but could not be restored looks healthy
			// and serves nothing. Drop it and try again.
			_ = conn.Close()
			if s.ctx.Err() != nil {
				return nil
			}
			continue
		}
		// The socket may have died while restorers ran; then the attempt
		// failed, however well the restorers think they did.
		select {
		case <-closed:
			lastErr = amqp.ErrClosed
			continue
		default:
		}
		s.markReady()
		s.log.LogAttrs(s.ctx, slog.LevelInfo, "reconnected", attrOperation("reconnect"), attrAttempt(attempt), attrOutcome(outcomeOK))
		s.emit(ConnectionEvent{Kind: EventReconnected, Attempt: attempt})
		return closed
	}
}

func (s *session) runRestorers(conn transport.Conn) error {
	s.restoreMu.Lock()
	defer s.restoreMu.Unlock()
	for _, r := range s.registrations() {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if err := r.c.restoreTopology(s.ctx, conn); err != nil {
			return fmt.Errorf("restoring %T: %w", r.c, err)
		}
	}
	return nil
}

func (s *session) giveUp(attempts int, last error) {
	err := fmt.Errorf("%w: gave up reconnecting after %d attempts", ErrNotReady, attempts)
	s.log.LogAttrs(s.ctx, slog.LevelError, "giving up on the broker connection", attrOperation("reconnect"),
		attrAttempt(attempts), attrOutcome(outcomeFailed), attrSafeError(last))
	s.finish(err)
	s.emit(ConnectionEvent{Kind: EventGaveUp, Attempt: attempts, Err: last})
}

func (s *session) finish(err error) {
	s.doneOnce.Do(func() {
		s.mu.Lock()
		s.doneErr = err
		s.mu.Unlock()
		close(s.doneCh)
	})
}

func (s *session) markReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isReady {
		s.isReady = true
		close(s.ready)
	}
}

func (s *session) markNotReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isReady {
		s.isReady = false
		s.ready = make(chan struct{})
	}
}

func (s *session) registrations() []*registration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*registration(nil), s.comps...)
}

// register adds a component to restoration, after those already registered,
// and returns a function that removes it again.
func (s *session) register(c component) (unregister func()) {
	r := &registration{c: c}
	s.mu.Lock()
	s.comps = append(s.comps, r)
	s.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			for i, x := range s.comps {
				if x == r {
					s.comps = append(s.comps[:i:i], s.comps[i+1:]...)
					return
				}
			}
		})
	}
}

// current returns the live connection and its generation, and whether it is
// ready for traffic.
func (s *session) current() (transport.Conn, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.gen, s.isReady && s.doneErr == nil
}

// whenReady waits until the connection carries traffic. It fails with
// ErrClosed after close, with ErrNotReady once the session has given up or
// the wait exceeds Config.ConnectionReadyTimeout, and with ctx's error.
func (s *session) whenReady(ctx context.Context) error {
	s.mu.Lock()
	ready := s.ready
	s.mu.Unlock()
	select {
	case <-s.doneCh:
		return s.err()
	default:
	}
	select {
	case <-ready:
		return nil
	default:
	}
	timer := time.NewTimer(s.cfg.ConnectionReadyTimeout)
	defer timer.Stop()
	select {
	case <-ready:
		select {
		case <-s.doneCh:
			return s.err()
		default:
			return nil
		}
	case <-s.doneCh:
		return s.err()
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("%w: the connection did not become ready within %v", ErrNotReady, s.cfg.ConnectionReadyTimeout)
	}
}

func (s *session) done() <-chan struct{} { return s.doneCh }

// err is nil while the session runs, ErrClosed after close, or the reason it
// gave up.
func (s *session) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doneErr
}

// close tears the session down. It is idempotent.
func (s *session) close() error {
	s.finish(ErrClosed)
	s.cancel()
	s.mu.Lock()
	conn := s.conn
	s.isReady = false
	s.mu.Unlock()
	if conn != nil && !conn.IsClosed() {
		_ = conn.Close()
	}
	s.wg.Wait()
	return nil
}

// sleepCtx waits d, reporting false if ctx ends first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
