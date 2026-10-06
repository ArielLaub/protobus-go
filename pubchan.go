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

// errPublishStalled reports a publish that was never sent because the channel
// could not take it within PublishConfirmTimeout: earlier publishes held every
// confirm slot, or a transport write held the send path. It is definite.
var errPublishStalled = errors.New("protobus: publish not sent: the channel had no capacity in time")

// pubChannel publishes on a confirm-mode channel and resolves each publish
// only when the broker has positively confirmed it.
//
// Every publication holds a confirm slot from admission until it is settled:
// by its confirm, by the channel closing, or by never being sent. A caller
// that stops waiting (a timeout or a cancelled context) does not settle it,
// so MaxOutstandingConfirms bounds what the broker and amqp091 are tracking,
// not merely who is waiting.
//
// One goroutine (writer) performs every transport write, in admission order,
// which keeps sequence numbers aligned with publishes. amqp091 cannot
// interrupt a write in progress, so a caller hands its publication to the
// writer and waits on channels it can abandon: waiting for a slot or for the
// writer is cancellable and sends nothing, and the writer checks the caller's
// context once more before committing to the write.
//
// One goroutine (loop) consumes the channel's returns and confirms. amqp091
// sends a basic.return on an unbuffered channel before it processes the
// basic.ack that follows it, so by the time loop sees the ack it has already
// recorded the return, in program order. That is what lets a mandatory
// publish learn it was unroutable without a race.
type pubChannel struct {
	ch      transport.Channel
	timeout time.Duration
	slots   chan struct{} // one per unsettled publication
	sendq   chan *pendingPublish

	mu      sync.Mutex
	pending map[uint64]*pendingPublish // sent, awaiting a confirm

	closed     chan struct{}
	closeErr   error
	loopDone   chan struct{}
	writerDone chan struct{}
	retireOnce sync.Once
	retiring   sync.WaitGroup
}

type pendingPublish struct {
	ctx       context.Context
	exchange  string
	key       string
	mandatory bool
	msg       amqp.Publishing

	committed chan struct{} // closed when the writer commits to the write
	returned  bool          // under pubChannel.mu
	sentAt    time.Time     // under pubChannel.mu: when its write completed

	settleOnce sync.Once
	release    func()
	done       chan struct{} // closed once settled
	err        error         // broker verdict (a PublishError's Err)
	sendErr    error         // definite: never sent
}

// settle records the publication's outcome and releases its slot. Only the
// first call counts: a confirm, the channel closing and a failed write can
// race to settle the same publication.
func (pp *pendingPublish) settle(verdict, sendErr error) {
	pp.settleOnce.Do(func() {
		pp.err, pp.sendErr = verdict, sendErr
		close(pp.done)
		pp.release()
	})
}

// newPubChannel puts ch into confirm mode and starts tracking it.
func newPubChannel(ch transport.Channel, cfg Config) (*pubChannel, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("protobus: enabling publisher confirms: %w", err)
	}
	p := &pubChannel{
		ch:         ch,
		timeout:    cfg.PublishConfirmTimeout,
		slots:      make(chan struct{}, cfg.MaxOutstandingConfirms),
		sendq:      make(chan *pendingPublish),
		pending:    map[uint64]*pendingPublish{},
		closed:     make(chan struct{}),
		loopDone:   make(chan struct{}),
		writerDone: make(chan struct{}),
	}
	// Sized so the client library never blocks delivering a confirm: there
	// are never more unsettled publications than there are slots.
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, cfg.MaxOutstandingConfirms))
	returns := ch.NotifyReturn(make(chan amqp.Return))
	closes := ch.NotifyClose(make(chan *amqp.Error, 1))
	go p.loop(confirms, returns, closes)
	go p.writer()
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
			p.markReturned(r)
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
			// Confirms (and returns) already received must win over the
			// close: reporting a confirmed publish as ambiguous invites a
			// duplicate.
			p.drain(confirms, returns)
			p.shutdown(reason)
			return
		}
	}
}

func (p *pubChannel) drain(confirms <-chan amqp.Confirmation, returns <-chan amqp.Return) {
	for {
		select {
		case r, ok := <-returns:
			if !ok {
				returns = nil
				continue
			}
			p.markReturned(r)
		case c, ok := <-confirms:
			if !ok {
				confirms = nil
				continue
			}
			p.resolve(c)
		default:
			return
		}
	}
}

func (p *pubChannel) markReturned(r amqp.Return) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// A return carries no delivery tag. It is attributed among the
	// publications still awaiting their confirm, which includes those whose
	// caller stopped waiting: a publication leaves pending only when its own
	// confirm settles it, and the broker sends a return before the confirm of
	// the same publication. Publications sharing an id (stable message ids
	// make that routine) are told apart by destination, and among those with
	// the same destination the oldest not yet returned is the one bounced:
	// the broker routes, and so returns, a channel's publications in order.
	var match *pendingPublish
	var matchTag uint64
	for tag, pp := range p.pending {
		if pp.mandatory && !pp.returned && pp.msg.MessageId == r.MessageId &&
			pp.exchange == r.Exchange && pp.key == r.RoutingKey && (match == nil || tag < matchTag) {
			match, matchTag = pp, tag
		}
	}
	if match != nil {
		match.returned = true
	}
}

func (p *pubChannel) resolve(c amqp.Confirmation) {
	p.mu.Lock()
	pp, ok := p.pending[c.DeliveryTag]
	delete(p.pending, c.DeliveryTag)
	returned := ok && pp.returned
	p.mu.Unlock()
	if !ok {
		return
	}
	var verdict error
	switch {
	case !c.Ack:
		verdict = ErrPublishNacked
	case returned:
		verdict = ErrUnroutable
	}
	pp.settle(verdict, nil)
}

func (p *pubChannel) shutdown(reason string) {
	p.mu.Lock()
	pending := p.pending
	p.pending = map[uint64]*pendingPublish{}
	p.closeErr = errors.New(reason)
	p.mu.Unlock()
	close(p.closed)
	for _, pp := range pending {
		pp.settle(ErrChannelClosed, nil)
	}
}

// writer performs every transport write on the channel, one at a time.
func (p *pubChannel) writer() {
	defer close(p.writerDone)
	for {
		select {
		case pp := <-p.sendq:
			p.send(pp)
		case <-p.closed:
			return
		}
	}
}

func (p *pubChannel) send(pp *pendingPublish) {
	// The last moment a publication can still be withdrawn: past this point
	// it is committed, and the caller's outcome is ambiguous until settled.
	if err := pp.ctx.Err(); err != nil {
		pp.settle(nil, err)
		return
	}
	if p.isClosed() {
		pp.settle(nil, errChannelGone)
		return
	}
	tag := p.ch.GetNextPublishSeqNo()
	// Registered before the send: a fast broker can confirm before
	// PublishWithContext even returns.
	p.mu.Lock()
	p.pending[tag] = pp
	p.mu.Unlock()
	close(pp.committed)
	// Detached: the decision to send was taken above. amqp091 would not
	// interrupt the write anyway, only refuse to start it.
	err := p.ch.PublishWithContext(context.WithoutCancel(pp.ctx), pp.exchange, pp.key, pp.mandatory, false, pp.msg)
	if err == nil {
		p.mu.Lock()
		pp.sentAt = time.Now()
		p.mu.Unlock()
		return
	}
	// amqp091 rolls its sequence back on a failed write, so the tag will be
	// reused by the next publication: forget it now.
	p.mu.Lock()
	if p.pending[tag] == pp {
		delete(p.pending, tag)
	}
	p.mu.Unlock()
	if errors.Is(err, amqp.ErrClosed) {
		pp.settle(nil, errChannelGone)
		return
	}
	pp.settle(nil, fmt.Errorf("protobus: publish to %s -> %s: %w", pp.exchange, pp.key, err))
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
// Waiting for a confirm slot and for the send path is bounded by ctx and by
// PublishConfirmTimeout, and sends nothing if it ends; the wait for the write
// and its confirm is bounded the same way, starting when the publication is
// committed to the write. A publication the caller stops waiting for keeps
// its slot until the broker settles it or the channel closes.
//
// Failures:
//   - errChannelGone: the channel was already closed; nothing was sent.
//   - the context's error or errPublishStalled, unwrapped: nothing was sent.
//   - an error from the write itself: nothing was sent.
//   - *PublishError wrapping ErrPublishNacked or ErrUnroutable (definite), or
//     ErrPublishConfirmTimeout, ErrChannelClosed or the context's error
//     (ambiguous: the broker may have stored the message).
func (p *pubChannel) publish(ctx context.Context, exchange, key string, mandatory bool, msg amqp.Publishing) error {
	if msg.MessageId == "" {
		msg.MessageId = uuid.New()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(p.timeout)
	defer timer.Stop()
	stalled := func() error {
		p.retireIfWedged()
		return fmt.Errorf("%w (%v)", errPublishStalled, p.timeout)
	}

	select {
	case p.slots <- struct{}{}:
	case <-p.closed:
		return errChannelGone
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return stalled()
	}
	pp := &pendingPublish{
		ctx: ctx, exchange: exchange, key: key, mandatory: mandatory, msg: msg,
		committed: make(chan struct{}), done: make(chan struct{}),
		release: func() { <-p.slots },
	}
	fail := func(err error, detail string) error {
		return &PublishError{Err: err, MessageID: msg.MessageId, Exchange: exchange, RoutingKey: key, Detail: detail}
	}

	select {
	case p.sendq <- pp:
	case <-p.closed:
		pp.settle(nil, errChannelGone)
		return errChannelGone
	case <-ctx.Done():
		pp.settle(nil, ctx.Err())
		return ctx.Err()
	case <-timer.C:
		pp.settle(nil, nil)
		return stalled()
	}
	outcome := func() error {
		switch {
		case pp.sendErr != nil:
			return pp.sendErr
		case pp.err != nil:
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
	}
	// The writer decides at once, without blocking: commit, or settle unsent.
	// A fast confirm can settle the publication before this goroutine sees
	// the commit, so done may already be closed either way.
	select {
	case <-pp.committed:
	case <-pp.done:
		return outcome()
	}

	resetTimer(timer, p.timeout)
	select {
	case <-pp.done:
		return outcome()
	case <-timer.C:
		return fail(ErrPublishConfirmTimeout, fmt.Sprintf("no confirm within %v", p.timeout))
	case <-ctx.Done():
		return fail(ctx.Err(), "gave up waiting for the confirm")
	}
}

// retireIfWedged closes the channel when a publication could not get in
// because earlier ones have gone unconfirmed for longer than the confirm
// timeout: the broker is not confirming, and the capacity they hold would
// never come back. Closing settles them as ambiguous (ErrChannelClosed) and
// lets the channel's owner open a fresh one. A stuck write is not grounds:
// a new channel on the same connection would stall the same way.
func (p *pubChannel) retireIfWedged() {
	p.mu.Lock()
	var oldest time.Time
	for _, pp := range p.pending {
		if pp.sentAt.IsZero() {
			continue // still being written
		}
		if oldest.IsZero() || pp.sentAt.Before(oldest) {
			oldest = pp.sentAt
		}
	}
	p.mu.Unlock()
	if oldest.IsZero() || time.Since(oldest) < p.timeout {
		return
	}
	p.retireOnce.Do(func() {
		p.retiring.Add(1)
		go func() {
			defer p.retiring.Done()
			_ = p.ch.Close()
		}()
	})
}

// close closes the channel and waits for its goroutines to finish.
func (p *pubChannel) close() {
	_ = p.ch.Close()
	<-p.loopDone
	<-p.writerDone
	p.retiring.Wait()
}
