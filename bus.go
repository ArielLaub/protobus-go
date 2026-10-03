package protobus

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

// Bus is a process's connection to the protobus message bus: the equivalent
// of a Context in the TypeScript and Python ports.
//
// A Bus is safe for concurrent use. Every client, service and event listener
// built on it shares one AMQP connection, with a channel per component.
// Create one with Dial and release it with Close (immediate) or Shutdown
// (graceful).
type Bus struct {
	cfg   Config
	log   *slog.Logger
	sess  *session
	files *protoregistry.Files
	types *protoregistry.Types

	dispatcher *dispatcher
	events     *eventPublisher
	cancels    *cancelRegistry
	cancelSub  *cancelListener

	deliveries inflight // deliveries received and not yet settled
	handlers   inflight // handler goroutines still running

	mu        sync.Mutex
	services  []*Service
	listeners []*EventListener
	closers   []func()
	closed    bool
}

// Dial connects to the broker at url and prepares the bus for calls and
// events. The first connection is not retried; once connected, a lost
// connection is re-established automatically (see Config.Reconnect).
//
// Without WithConfig the configuration comes from ConfigFromEnv.
func Dial(ctx context.Context, url string, opts ...DialOption) (*Bus, error) {
	var o dialOptions
	for _, opt := range opts {
		opt.applyDial(&o)
	}
	cfg := ConfigFromEnv()
	if o.cfg != nil {
		cfg = *o.cfg
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	log := o.log
	if log == nil {
		log = defaultLogger(nil)
	}
	dial := o.dialer
	if dial == nil {
		dial = transport.Dial
	}
	b := &Bus{
		cfg:     cfg,
		log:     log,
		files:   o.files,
		types:   o.types,
		cancels: newCancelRegistry(),
	}
	if b.files == nil {
		b.files = protoregistry.GlobalFiles
	}
	if b.types == nil {
		b.types = protoregistry.GlobalTypes
	}
	b.sess = newSession(dial, url, cfg, log, o.observe)
	if o.connName != "" {
		b.sess.name = o.connName
	}
	if err := b.sess.connect(ctx); err != nil {
		_ = b.sess.close()
		return nil, err
	}
	b.dispatcher = newDispatcher(b)
	b.events = newEventPublisher(b)
	for _, c := range []interface {
		component
		closer
	}{b.dispatcher, b.events} {
		if _, err := b.attach(ctx, c); err != nil {
			_ = b.Close()
			return nil, err
		}
	}
	return b, nil
}

type closer interface{ close() }

// attach initialises c on the current connection and enrols it in
// restoration, atomically with respect to reconnection. The returned function
// unenrols it.
func (b *Bus) attach(ctx context.Context, c interface {
	component
	closer
}) (unregister func(), err error) {
	for {
		if err := b.sess.whenReady(ctx); err != nil {
			return nil, err
		}
		unregister, retry, err := b.attachOnce(ctx, c)
		if !retry {
			return unregister, err
		}
		// The connection was not ready after all: a reconnection had restored
		// it but not yet announced it, or it dropped. Wait again.
	}
}

func (b *Bus) attachOnce(ctx context.Context, c interface {
	component
	closer
}) (unregister func(), retry bool, err error) {
	b.sess.restoreMu.Lock()
	defer b.sess.restoreMu.Unlock()
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return nil, false, ErrClosed
	}
	conn, _, ready := b.sess.current()
	if !ready {
		return nil, true, nil
	}
	if err := c.restoreTopology(ctx, conn); err != nil {
		if conn.IsClosed() {
			return nil, true, nil
		}
		return nil, false, err
	}
	unreg := b.sess.register(c)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		unreg()
		c.close()
		return nil, false, ErrClosed
	}
	b.closers = append(b.closers, func() { unreg(); c.close() })
	b.mu.Unlock()
	return unreg, false, nil
}

// Config returns the bus's configuration.
func (b *Bus) Config() Config { return b.cfg }

// Logger returns the bus's logger.
func (b *Bus) Logger() *slog.Logger { return b.log }

// Done is closed when the bus stops for good: after Close or Shutdown, or
// when reconnection gives up. Err then reports why.
func (b *Bus) Done() <-chan struct{} { return b.sess.done() }

// Err is nil while the bus runs; ErrClosed after Close; or, after giving up
// on the broker, an error wrapping ErrNotReady.
func (b *Bus) Err() error { return b.sess.err() }

// InFlight reports how many deliveries are being handled, counting a handler
// that outlived its processing timeout until it actually returns.
func (b *Bus) InFlight() int { return max(b.deliveries.count(), b.handlers.count()) }

// Drain waits until no delivery is in flight and no handler is running, or ctx
// ends. Pair it with Service.StopConsuming: stop intake first, then drain.
func (b *Bus) Drain(ctx context.Context) error {
	if err := b.deliveries.wait(ctx); err != nil {
		return err
	}
	return b.handlers.wait(ctx)
}

// Shutdown stops the bus gracefully: every service stops taking new work,
// in-flight work is given until ctx ends (or Config.ShutdownDrainTimeout,
// whichever is sooner) to finish, then everything is closed. Work still
// running at the deadline stays unacknowledged and is redelivered elsewhere.
//
// It returns the drain's error, if the deadline cut it short.
func (b *Bus) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	services := append([]*Service(nil), b.services...)
	listeners := append([]*EventListener(nil), b.listeners...)
	b.mu.Unlock()
	for _, s := range services {
		if err := s.StopConsuming(ctx); err != nil {
			b.log.LogAttrs(ctx, slog.LevelWarn, "failed to stop consuming", attrOperation("shutdown"),
				attrService(s.Name()), attrSafeError(err))
		}
	}
	for _, l := range listeners {
		_ = l.StopConsuming(ctx)
	}
	drainCtx, cancel := context.WithTimeout(ctx, b.cfg.ShutdownDrainTimeout)
	defer cancel()
	start := time.Now()
	drainErr := b.Drain(drainCtx)
	if drainErr != nil {
		b.log.LogAttrs(ctx, slog.LevelWarn, "drain deadline reached; unfinished work will be redelivered",
			attrOperation("shutdown"), slog.Int("inFlight", b.InFlight()), attrDuration(time.Since(start)))
		drainErr = fmt.Errorf("protobus: shutdown drain incomplete: %w", drainErr)
	}
	if err := b.Close(); err != nil {
		return err
	}
	return drainErr
}

// Close closes the bus at once: consumers stop, calls in flight fail with
// ErrClosed, and the connection is closed. Unacknowledged deliveries are
// redelivered by the broker. Use Shutdown to let work finish first.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	closers := b.closers
	b.closers = nil
	b.mu.Unlock()
	// Fail the session first, so components closing see ErrClosed rather
	// than reconnecting.
	b.sess.finish(ErrClosed)
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	return b.sess.close()
}

// ---- in-flight accounting ------------------------------------------------------

// inflight counts work in progress and lets callers wait for it to reach zero.
type inflight struct {
	mu   sync.Mutex
	n    int
	zero chan struct{} // closed while n == 0; nil means "closed"
}

func (f *inflight) add() {
	f.mu.Lock()
	if f.n == 0 {
		f.zero = make(chan struct{})
	}
	f.n++
	f.mu.Unlock()
}

func (f *inflight) done() {
	f.mu.Lock()
	f.n--
	if f.n == 0 {
		close(f.zero)
	}
	f.mu.Unlock()
}

func (f *inflight) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

func (f *inflight) wait(ctx context.Context) error {
	f.mu.Lock()
	if f.n == 0 {
		f.mu.Unlock()
		return nil
	}
	zero := f.zero
	f.mu.Unlock()
	select {
	case <-zero:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
