package protobus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
	"github.com/ArielLaub/protobus-go/v2/internal/uuid"
)

// errChannelGone reports a publish that was never sent because its channel
// had already closed. It is definite, unlike a channel closing while a confirm
// is pending, so the owner can safely republish on a fresh channel.
var errChannelGone = fmt.Errorf("protobus: channel unavailable: %w", amqp.ErrClosed)

// pubChannel publishes on a confirm-mode channel and resolves each publish
// only when the broker has positively confirmed it.
//
// One goroutine (loop) consumes the channel's returns and confirms. amqp091
// sends a basic.return on an unbuffered channel before it processes the
// basic.ack that follows it, so by the time loop sees the ack it has already
// recorded the return, in program order. That is what lets a mandatory
// publish learn it was unroutable without a race.
type pubChannel struct {
	ch      transport.Channel
	timeout time.Duration
	slots   chan struct{} // bounds unconfirmed publishes

	sendMu  sync.Mutex // keeps sequence numbers aligned with publish order
	mu      sync.Mutex
	pending map[uint64]*pendingPublish

	closed   chan struct{}
	closeErr error
	loopDone chan struct{}
}

type pendingPublish struct {
	messageID string
	mandatory bool
	returned  bool
	done      chan struct{}
	err       error
}

// newPubChannel puts ch into confirm mode and starts tracking it.
func newPubChannel(ch transport.Channel, cfg Config) (*pubChannel, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("protobus: enabling publisher confirms: %w", err)
	}
	p := &pubChannel{
		ch:       ch,
		timeout:  cfg.PublishConfirmTimeout,
		slots:    make(chan struct{}, cfg.MaxOutstandingConfirms),
		pending:  map[uint64]*pendingPublish{},
		closed:   make(chan struct{}),
		loopDone: make(chan struct{}),
	}
	// Sized so the client library never blocks delivering a confirm: there
	// are never more outstanding than there are slots.
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, cfg.MaxOutstandingConfirms))
	returns := ch.NotifyReturn(make(chan amqp.Return))
	closes := ch.NotifyClose(make(chan *amqp.Error, 1))
	go p.loop(confirms, returns, closes)
	return p, nil
}

func (p *pubChannel) loop(confirms <-chan amqp.Confirmation, returns <-chan amqp.Return, closes <-chan *amqp.Error) {
	defer close(p.loopDone)
	for {
		select {
		case r, ok := <-returns:
			if !ok {
				returns = nil
				continue
			}
			p.markReturned(r.MessageId)
		case c, ok := <-confirms:
			if !ok {
				confirms = nil
				continue
			}
			p.resolve(c)
		case e, ok := <-closes:
			reason := "channel closed"
			if ok && e != nil {
				reason = e.Error()
			}
			p.shutdown(reason)
			return
		}
	}
}

func (p *pubChannel) markReturned(messageID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Only publishes still awaiting their confirm. A return that outlived its
	// own publish must not be read as the verdict on a later one reusing the
	// id, which stable message ids make routine.
	for _, pp := range p.pending {
		if pp.mandatory && pp.messageID == messageID {
			pp.returned = true
		}
	}
}

func (p *pubChannel) resolve(c amqp.Confirmation) {
	p.mu.Lock()
	pp, ok := p.pending[c.DeliveryTag]
	delete(p.pending, c.DeliveryTag)
	p.mu.Unlock()
	if !ok {
		return // its publisher already gave up
	}
	switch {
	case !c.Ack:
		pp.err = ErrPublishNacked
	case pp.returned:
		pp.err = ErrUnroutable
	}
	close(pp.done)
}

func (p *pubChannel) shutdown(reason string) {
	p.mu.Lock()
	pending := p.pending
	p.pending = map[uint64]*pendingPublish{}
	p.closeErr = errors.New(reason)
	p.mu.Unlock()
	close(p.closed)
	for _, pp := range pending {
		pp.err = ErrChannelClosed
		close(pp.done)
	}
}

func (p *pubChannel) closedCh() <-chan struct{} { return p.closed }

func (p *pubChannel) isClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}

// publish sends msg and waits for its confirm. A nil error means the broker
// accepted it and, when mandatory, routed it to at least one queue.
//
// Failures:
//   - errChannelGone: the channel was already closed; nothing was sent.
//   - a context error with nothing sent, while waiting for a confirm slot.
//   - *PublishError wrapping ErrPublishNacked or ErrUnroutable (definite), or
//     ErrPublishConfirmTimeout, ErrChannelClosed or the context's error
//     (ambiguous: the broker may have stored the message).
func (p *pubChannel) publish(ctx context.Context, exchange, key string, mandatory bool, msg amqp.Publishing) error {
	if msg.MessageId == "" {
		msg.MessageId = uuid.New()
	}
	select {
	case p.slots <- struct{}{}:
	case <-p.closed:
		return errChannelGone
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.slots }()

	pp := &pendingPublish{messageID: msg.MessageId, mandatory: mandatory, done: make(chan struct{})}
	fail := func(err error, detail string) error {
		return &PublishError{Err: err, MessageID: msg.MessageId, Exchange: exchange, RoutingKey: key, Detail: detail}
	}

	p.sendMu.Lock()
	if p.isClosed() {
		p.sendMu.Unlock()
		return errChannelGone
	}
	tag := p.ch.GetNextPublishSeqNo()
	// Registered before the send: a fast broker can confirm before
	// PublishWithContext even returns.
	p.mu.Lock()
	p.pending[tag] = pp
	p.mu.Unlock()
	err := p.ch.PublishWithContext(context.WithoutCancel(ctx), exchange, key, mandatory, false, msg)
	p.sendMu.Unlock()
	if err != nil {
		p.mu.Lock()
		delete(p.pending, tag)
		p.mu.Unlock()
		if errors.Is(err, amqp.ErrClosed) {
			return errChannelGone
		}
		return fmt.Errorf("protobus: publish to %s -> %s: %w", exchange, key, err)
	}

	timer := time.NewTimer(p.timeout)
	defer timer.Stop()
	select {
	case <-pp.done:
		if pp.err != nil {
			detail := ""
			if errors.Is(pp.err, ErrChannelClosed) {
				p.mu.Lock()
				if p.closeErr != nil {
					detail = p.closeErr.Error()
				}
				p.mu.Unlock()
			}
			return fail(pp.err, detail)
		}
		return nil
	case <-timer.C:
		p.abandon(tag)
		return fail(ErrPublishConfirmTimeout, fmt.Sprintf("no confirm within %v", p.timeout))
	case <-ctx.Done():
		p.abandon(tag)
		return fail(ctx.Err(), "gave up waiting for the confirm")
	}
}

func (p *pubChannel) abandon(tag uint64) {
	p.mu.Lock()
	delete(p.pending, tag)
	p.mu.Unlock()
}

// close closes the channel and waits for the tracking goroutine to finish.
func (p *pubChannel) close() {
	_ = p.ch.Close()
	<-p.loopDone
}
