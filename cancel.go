package protobus

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

// ErrCancelled is the cause of a handler's context when the caller abandoned
// the call: it broke out of a stream, its context ended, or the stream went
// idle. context.Cause(ctx) reports it inside the handler, and
// ServerStream.Send returns an error wrapping it.
var ErrCancelled = errors.New("protobus: cancelled by the caller")

// cancelRegistry maps correlation ids to the deliveries handling them, so a
// cancel notice can stop the right handlers. The same message can be in
// flight more than once (a redelivery overlapping its predecessor), and every
// copy belongs to the caller's call, so a cancel stops them all.
type cancelRegistry struct {
	mu sync.Mutex
	m  map[string]map[*cancelEntry]struct{}
}

type cancelEntry struct{ cancel context.CancelCauseFunc }

func newCancelRegistry() *cancelRegistry {
	return &cancelRegistry{m: map[string]map[*cancelEntry]struct{}{}}
}

func (r *cancelRegistry) add(id string, cancel context.CancelCauseFunc) (remove func()) {
	if id == "" {
		return func() {}
	}
	e := &cancelEntry{cancel: cancel}
	r.mu.Lock()
	set := r.m[id]
	if set == nil {
		set = map[*cancelEntry]struct{}{}
		r.m[id] = set
	}
	set[e] = struct{}{}
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if set := r.m[id]; set != nil {
			delete(set, e)
			if len(set) == 0 {
				delete(r.m, id)
			}
		}
	}
}

// cancel stops every delivery with this correlation id, reporting whether
// there was any.
func (r *cancelRegistry) cancel(id string) bool {
	r.mu.Lock()
	set := r.m[id]
	entries := make([]*cancelEntry, 0, len(set))
	for e := range set {
		entries = append(entries, e)
	}
	r.mu.Unlock()
	for _, e := range entries {
		e.cancel(ErrCancelled)
	}
	return len(entries) > 0
}

func (r *cancelRegistry) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.m)
}

// cancelListener hears stream-cancellation notices on the fanout cancel
// exchange. Every service process binds its own exclusive queue and hears
// every cancel, ignoring the ones it does not own: a caller cannot know which
// replica is producing its stream.
//
// It is best effort by design. Cancels are consumed with auto-ack (a lost
// cancel means the stream runs on, as if it was never sent), and a
// deployment whose credentials cannot declare the exchange runs without
// cancellation rather than failing to start.
type cancelListener struct {
	bus  *Bus
	mu   sync.Mutex
	ch   transport.Channel
	done chan struct{}
}

func (c *cancelListener) restoreTopology(ctx context.Context, conn transport.Conn) error {
	if err := c.start(conn); err != nil {
		c.bus.log.LogAttrs(ctx, slog.LevelWarn,
			"stream cancellation unavailable; streams will run to completion, everything else is unaffected",
			attrOperation("cancel"), attrSafeError(err))
	}
	return nil // never fails the connection over a missing convenience
}

func (c *cancelListener) start(conn transport.Conn) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	ex := c.bus.cfg.CancelExchange
	fail := func(err error) error { _ = ch.Close(); return err }
	if err := ch.ExchangeDeclare(ex, "fanout", true, false, false, false, nil); err != nil {
		return fail(err)
	}
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return fail(err)
	}
	if err := ch.QueueBind(q.Name, "", ex, false, nil); err != nil {
		return fail(err)
	}
	deliveries, err := ch.ConsumeWithContext(context.Background(), q.Name, "", true, true, false, false, nil)
	if err != nil {
		return fail(err)
	}
	done := make(chan struct{})
	c.mu.Lock()
	c.ch, c.done = ch, done
	c.mu.Unlock()
	go func() {
		defer close(done)
		for d := range deliveries {
			c.handle(&d)
		}
	}()
	return nil
}

func (c *cancelListener) handle(d *amqp.Delivery) {
	if d.CorrelationId == "" {
		return
	}
	if c.bus.cancels.cancel(d.CorrelationId) {
		c.bus.log.LogAttrs(context.Background(), slog.LevelDebug, "call cancelled by its caller",
			attrOperation("cancel"), attrCorrelationID(d.CorrelationId), attrOutcome(outcomeCancelled))
	}
}

func (c *cancelListener) connectionLost(error) {}

func (c *cancelListener) close() {
	c.mu.Lock()
	ch, done := c.ch, c.done
	c.ch = nil
	c.mu.Unlock()
	if ch != nil {
		_ = ch.Close()
		<-done
	}
}

// ensureCancelListener starts the process's cancel listener once, when the
// first service starts: only a process serving requests has anything to
// cancel.
func (b *Bus) ensureCancelListener(ctx context.Context) error {
	b.mu.Lock()
	if b.cancelSub != nil {
		b.mu.Unlock()
		return nil
	}
	b.cancelSub = &cancelListener{bus: b}
	b.mu.Unlock()
	return b.attach(ctx, b.cancelSub)
}
