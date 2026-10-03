package protobus

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
	"github.com/ArielLaub/protobus-go/v2/internal/uuid"
)

// dispatcher is the caller side of RPC: one publishing channel, one reply
// queue per process, and the table of calls awaiting replies.
//
// The reply queue is server-named, exclusive and auto-delete, bound to the
// callbacks (direct) exchange under its own name; services reply to that
// exchange with routing key = replyTo. This is the protocol the other ports
// speak, so RabbitMQ's direct reply-to is deliberately not used.
type dispatcher struct {
	bus *Bus

	mu         sync.Mutex
	pub        *pubChannel
	consumeCh  transport.Channel
	replyQueue string
	// replyGen identifies the installed reply consumer. Recovery of a dead
	// consumer is keyed on it, so a recovery can tell its own earlier cleanup
	// from a rebuild someone else completed.
	replyGen uint64
	calls    map[string]*pendingCall
	streams  map[string]*clientStream
	loopDone chan struct{}
	closed   bool

	// bufferedBytes is the total held across every stream, bounded by
	// Config.StreamMaxTotalBufferedBytes.
	bufferedBytes atomic.Int64
}

type callResult struct {
	body []byte
	err  error
}

// pendingCall awaits one reply. Every attempt to publish a call gets a fresh
// one, so a signal meant for an earlier attempt can never answer a later one.
type pendingCall struct{ done chan callResult }

func newPendingCall() *pendingCall { return &pendingCall{done: make(chan callResult, 1)} }

// resolve delivers the call's outcome. It never blocks: the first outcome
// wins, and the supervisor and the reply reader must never wait on a caller.
func (c *pendingCall) resolve(r callResult) {
	select {
	case c.done <- r:
	default:
	}
}

func newDispatcher(b *Bus) *dispatcher {
	return &dispatcher{bus: b, calls: map[string]*pendingCall{}, streams: map[string]*clientStream{}}
}

// declareCoreExchanges declares every exchange protobus publishes to.
// Publishers declare what they publish to, so a client starting before any
// service gets a working bus rather than a 404 that closes its channel.
// The arguments are exactly those every port uses, so the redeclaration is
// always equivalent.
func declareCoreExchanges(ch transport.Channel, cfg Config) error {
	for _, ex := range []struct{ name, kind string }{
		{cfg.BusExchange, "topic"},
		{cfg.CallbacksExchange, "direct"},
		{cfg.EventsExchange, "topic"},
		{cfg.CancelExchange, "fanout"},
	} {
		if err := ch.ExchangeDeclare(ex.name, ex.kind, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declaring exchange %s: %w", ex.name, err)
		}
	}
	return nil
}

func (d *dispatcher) restoreTopology(ctx context.Context, conn transport.Conn) error {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return nil
	}
	cfg := d.bus.cfg
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	fail := func(err error) error { _ = ch.Close(); return err }
	if err := declareCoreExchanges(ch, cfg); err != nil {
		return fail(err)
	}
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return fail(fmt.Errorf("declaring reply queue: %w", err))
	}
	if err := ch.QueueBind(q.Name, q.Name, cfg.CallbacksExchange, false, nil); err != nil {
		return fail(fmt.Errorf("binding reply queue: %w", err))
	}
	pubRaw, err := conn.Channel()
	if err != nil {
		return fail(err)
	}
	pub, err := newPubChannel(pubRaw, cfg)
	if err != nil {
		_ = pubRaw.Close()
		return fail(err)
	}
	// Consumed last, once nothing else can fail, so a failed restore never
	// leaves an orphan consumer filling an unread buffer. Replies are
	// consumed with auto-ack: they are addressed to this process alone, and a
	// reply lost with the process has no one left to read it.
	deliveries, err := ch.ConsumeWithContext(context.WithoutCancel(ctx), q.Name, "", true, true, false, false, nil)
	if err != nil {
		pub.close()
		return fail(fmt.Errorf("consuming replies: %w", err))
	}
	done := make(chan struct{})
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		pub.close()
		_ = ch.Close()
		for range deliveries {
		}
		return nil
	}
	d.replyGen++
	gen := d.replyGen
	d.pub, d.consumeCh, d.replyQueue, d.loopDone = pub, ch, q.Name, done
	d.mu.Unlock()
	go d.receive(ch, gen, deliveries, done)
	d.bus.log.LogAttrs(ctx, slog.LevelDebug, "reply queue ready", attrOperation("consume"), attrQueue(q.Name))
	return nil
}

func (d *dispatcher) receive(ch transport.Channel, gen uint64, deliveries <-chan amqp.Delivery, done chan struct{}) {
	for dl := range deliveries {
		d.route(&dl)
	}
	close(done)
	// The consumer ended. On a deliberate close or a lost connection that is
	// expected; on a live connection the channel died under us.
	d.mu.Lock()
	unexpected := !d.closed && d.replyGen == gen && d.consumeCh == ch
	d.mu.Unlock()
	if _, _, ready := d.bus.sess.current(); unexpected && ready {
		go d.recoverReplies(gen)
	}
}

// route hands a reply to the call or stream waiting for its correlationId.
// It never blocks: it runs on the one goroutine reading every reply.
func (d *dispatcher) route(dl *amqp.Delivery) {
	id := dl.CorrelationId
	d.mu.Lock()
	if s, ok := d.streams[id]; ok {
		d.mu.Unlock()
		s.push(dl)
		return
	}
	call, ok := d.calls[id]
	if ok {
		delete(d.calls, id)
	}
	d.mu.Unlock()
	if !ok {
		// A late reply to a call that timed out, or one for a previous
		// connection. Nothing is waiting, so there is nothing to do.
		d.bus.log.LogAttrs(context.Background(), slog.LevelDebug, "dropping reply nobody awaits",
			attrOperation("reply"), attrCorrelationID(id), attrSize(len(dl.Body)), attrOutcome(outcomeDropped))
		return
	}
	call.resolve(callResult{body: dl.Body})
}

// failPending fails every call and stream in flight with err.
func (d *dispatcher) failPending(err error) {
	d.mu.Lock()
	calls, streams := d.calls, d.streams
	d.calls, d.streams = map[string]*pendingCall{}, map[string]*clientStream{}
	d.mu.Unlock()
	for _, c := range calls {
		c.resolve(callResult{err: err})
	}
	for _, s := range streams {
		s.fail(err)
	}
}

func (d *dispatcher) connectionLost(error) {
	d.failPending(ErrDisconnected)
	d.mu.Lock()
	d.pub, d.consumeCh, d.replyQueue = nil, nil, ""
	d.mu.Unlock()
}

func (d *dispatcher) close() {
	d.mu.Lock()
	d.closed = true
	pub, ch, done := d.pub, d.consumeCh, d.loopDone
	d.pub, d.consumeCh, d.replyQueue = nil, nil, ""
	d.mu.Unlock()
	d.failPending(ErrClosed)
	if ch != nil {
		_ = ch.Close()
	}
	if pub != nil {
		pub.close()
	}
	if done != nil {
		<-done
	}
}

// channel waits for a usable publishing channel and the reply queue the
// request should name.
func (d *dispatcher) channel(ctx context.Context) (*pubChannel, string, error) {
	for {
		if err := d.bus.sess.whenReady(ctx); err != nil {
			return nil, "", err
		}
		d.mu.Lock()
		closed, pub, replyTo := d.closed, d.pub, d.replyQueue
		d.mu.Unlock()
		if closed {
			return nil, "", ErrClosed
		}
		// Never hand out a channel without a reply queue: a request naming
		// none would be served and its answer thrown away.
		if pub != nil && !pub.isClosed() && replyTo != "" {
			return pub, replyTo, nil
		}
		// Ready, but a channel of this component died on a live connection,
		// or a restore is completing. Rebuild what is missing.
		if err := d.repair(ctx); err != nil {
			if !errors.Is(err, errConnectionGone) {
				return nil, "", err
			}
			// The connection died under the repair; the supervisor is about
			// to notice. Pause briefly so whenReady sees it.
			if !sleepCtx(ctx, 10*time.Millisecond) {
				return nil, "", ctx.Err()
			}
		}
	}
}

// errConnectionGone reports a repair attempted on a connection that has died
// but not yet been noticed: the caller should wait for readiness again.
var errConnectionGone = errors.New("protobus: connection gone")

// repair rebuilds whatever is missing on the current connection: the
// publishing channel alone when only it died (a channel exception), or the
// whole topology when the reply consumer is gone too.
func (d *dispatcher) repair(ctx context.Context) error {
	d.bus.sess.restoreMu.Lock()
	defer d.bus.sess.restoreMu.Unlock()
	d.mu.Lock()
	old, replyTo := d.pub, d.replyQueue
	pubHealthy := old != nil && !old.isClosed()
	d.mu.Unlock()
	if pubHealthy && replyTo != "" {
		return nil // repaired by someone else meanwhile
	}
	conn, _, ready := d.bus.sess.current()
	if !ready {
		return nil // a reconnection is under way; channel() waits for it
	}
	if conn.IsClosed() {
		return errConnectionGone
	}
	if replyTo == "" {
		if old != nil {
			old.close()
		}
		if err := d.restoreTopology(ctx, conn); err != nil {
			return connectionGoneOr(conn, err)
		}
		return nil
	}
	raw, err := conn.Channel()
	if err != nil {
		return connectionGoneOr(conn, err)
	}
	pub, err := newPubChannel(raw, d.bus.cfg)
	if err != nil {
		_ = raw.Close()
		return connectionGoneOr(conn, err)
	}
	d.mu.Lock()
	d.pub = pub
	d.mu.Unlock()
	if old != nil {
		old.close()
	}
	return nil
}

func connectionGoneOr(conn transport.Conn, err error) error {
	if conn.IsClosed() || errors.Is(err, amqp.ErrClosed) {
		return errConnectionGone
	}
	return err
}

// recoverReplies rebuilds the reply queue after its consumer died on a live
// connection. Calls waiting on the old queue cannot receive their replies any
// more, so they fail with ErrDisconnected.
func (d *dispatcher) recoverReplies(gen uint64) {
	delay := d.bus.cfg.Reconnect.InitialDelay
	for {
		retry, err := d.tryRecoverReplies(gen)
		if !retry {
			return
		}
		d.bus.log.LogAttrs(context.Background(), slog.LevelWarn, "failed to rebuild the reply queue",
			attrOperation("consume"), attrSafeError(err), attrDuration(delay))
		if !sleepCtx(d.bus.sess.ctx, delay) {
			return
		}
		delay = min(delay*2, d.bus.cfg.Reconnect.MaxDelay)
	}
}

func (d *dispatcher) tryRecoverReplies(gen uint64) (retry bool, err error) {
	d.bus.sess.restoreMu.Lock()
	defer d.bus.sess.restoreMu.Unlock()
	d.mu.Lock()
	stale := d.closed || d.replyGen != gen
	pub, dead := d.pub, d.consumeCh
	d.mu.Unlock()
	conn, _, ready := d.bus.sess.current()
	if stale || !ready {
		return false, nil // closed, already rebuilt, or a reconnection owns it
	}
	if dead != nil {
		// First attempt: retire the dead consumer and what rode on it.
		d.connectionLost(errChannelGone)
		_ = dead.Close() // still open when the broker cancelled the consumer
		if pub != nil {
			pub.close()
		}
	}
	if err := d.restoreTopology(d.bus.sess.ctx, conn); err != nil {
		return true, err
	}
	return false, nil
}

// call publishes a request and waits for its reply.
func (d *dispatcher) call(ctx context.Context, routingKey string, body []byte, o *callOptions) ([]byte, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	// An explicit timeout always applies (the context's own deadline still
	// wins when it is sooner); the configured default only fills in when the
	// caller set neither.
	limit := o.timeout
	if _, has := ctx.Deadline(); !has && limit == 0 {
		limit = d.bus.cfg.RPCTimeout
	}
	if limit > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, limit)
		defer cancel()
	}

	msg := amqp.Publishing{
		ContentType:   contentTypeOctetStream,
		CorrelationId: uuid.New(),
		DeliveryMode:  amqp.Persistent,
		Body:          body,
	}
	if o.priority != nil {
		msg.Priority = *o.priority
	}
	if o.messageID != nil {
		msg.MessageId = *o.messageID
	}

	timeout := func(err error) error {
		if errors.Is(err, context.DeadlineExceeded) {
			return rpcTimeoutError(routingKey, msg.CorrelationId, err)
		}
		return err
	}
	var call *pendingCall
	for {
		pub, replyTo, err := d.channel(ctx)
		if err != nil {
			return nil, timeout(err)
		}
		if !o.noReply {
			// Registered BEFORE the publish: a fast service can answer before
			// the broker's confirm reaches us.
			call = newPendingCall()
			msg.ReplyTo = replyTo
			d.mu.Lock()
			d.calls[msg.CorrelationId] = call
			d.mu.Unlock()
		}
		// Mandatory, fire-and-forget included: a request no service is bound
		// to fails fast with ErrUnroutable rather than vanishing.
		err = pub.publish(ctx, d.bus.cfg.BusExchange, routingKey, true, msg)
		if err == nil {
			break
		}
		if call != nil {
			d.forget(msg.CorrelationId, call)
		}
		if errors.Is(err, errChannelGone) {
			continue // never sent; wait for a channel and send again
		}
		// The publish outcome is the more specific answer than an expired
		// deadline: "the request never left" beats "no reply in time".
		var pe *PublishError
		if errors.As(err, &pe) && errors.Is(pe.Err, context.DeadlineExceeded) {
			return nil, timeout(err)
		}
		return nil, err
	}
	if o.noReply {
		return nil, nil
	}
	select {
	case r := <-call.done:
		return r.body, r.err
	case <-ctx.Done():
		d.forget(msg.CorrelationId, call)
		return nil, timeout(ctx.Err())
	}
}

// forget unregisters call, if it is still the one registered under id.
func (d *dispatcher) forget(id string, call *pendingCall) {
	d.mu.Lock()
	if d.calls[id] == call {
		delete(d.calls, id)
	}
	d.mu.Unlock()
}

// ---- streaming --------------------------------------------------------------

// clientStream buffers the frames of one server-streaming reply. push runs on
// the reply-reading goroutine and never blocks; the caller's iterator drains
// it.
type clientStream struct {
	d      *dispatcher
	id     string
	limits struct {
		chunks int
		bytes  int64
		total  int64
	}

	mu      sync.Mutex
	chunks  [][]byte
	bytes   int64
	lastSeq int64 // -1 before the first sequenced frame
	ended   bool
	err     error
	notify  chan struct{} // buffered 1
}

func (d *dispatcher) newStream(id string) *clientStream {
	s := &clientStream{d: d, id: id, lastSeq: -1, notify: make(chan struct{}, 1)}
	s.limits.chunks = d.bus.cfg.StreamMaxBufferedChunks
	s.limits.bytes = d.bus.cfg.StreamMaxBufferedBytes
	s.limits.total = d.bus.cfg.StreamMaxTotalBufferedBytes
	return s
}

func (s *clientStream) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// dropBufferLocked discards buffered chunks and returns their bytes to the
// process-wide allowance.
func (s *clientStream) dropBufferLocked() {
	s.d.bufferedBytes.Add(-s.bytes)
	s.bytes = 0
	s.chunks = nil
}

func (s *clientStream) push(dl *amqp.Delivery) {
	final := streamFinal(dl.Headers)
	seq, sequenced := streamSeq(dl.Headers)

	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.wake()
	if s.ended {
		return
	}
	if sequenced {
		expected := s.lastSeq + 1
		switch {
		case seq < expected:
			// A redelivered frame already seen: drop it, keeping the stream
			// idempotent. Its final flag still counts.
			s.ended = s.ended || final
			return
		case seq > expected:
			// A lost frame. Yielding what did arrive would hand the caller a
			// short stream that looks complete, so fail loudly.
			s.err = fmt.Errorf("%w: got seq %d, expected %d (stream %s)", ErrStreamSequence, seq, expected, s.id)
			s.ended = true
			s.dropBufferLocked()
			return
		}
		s.lastSeq = seq
	}
	if n := int64(len(dl.Body)); n > 0 {
		// An empty body is an end marker, not a chunk.
		total := s.d.bufferedBytes.Add(n)
		if len(s.chunks)+1 > s.limits.chunks || s.bytes+n > s.limits.bytes || total > s.limits.total {
			s.d.bufferedBytes.Add(-n)
			s.err = fmt.Errorf("%w: %d chunks / %d bytes buffered for this call, %d across all calls "+
				"(limits %d / %d / %d); the caller is not keeping up with the producer",
				ErrStreamBackpressure, len(s.chunks)+1, s.bytes+n, total, s.limits.chunks, s.limits.bytes, s.limits.total)
			s.ended = true
			s.dropBufferLocked()
			return
		}
		s.chunks = append(s.chunks, dl.Body)
		s.bytes += n
	}
	if final {
		s.ended = true
	}
}

func (s *clientStream) fail(err error) {
	s.mu.Lock()
	switch {
	case s.ended && s.err == nil:
		// Complete: every frame has arrived. Let the caller drain it; the
		// failure came too late to matter.
	case !s.ended:
		s.err = err
		s.ended = true
		s.dropBufferLocked()
	}
	s.mu.Unlock()
	s.wake()
}

// next returns the next chunk, or done with the stream's terminal error.
func (s *clientStream) next() (chunk []byte, done bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, true, s.err
	}
	if len(s.chunks) > 0 {
		chunk = s.chunks[0]
		s.chunks[0] = nil
		s.chunks = s.chunks[1:]
		s.bytes -= int64(len(chunk))
		s.d.bufferedBytes.Add(-int64(len(chunk)))
		return chunk, false, nil
	}
	return nil, s.ended, nil
}

// errStreamReleased marks a stream its caller has finished with, so a frame
// still on its way is dropped rather than buffered for no one.
var errStreamReleased = errors.New("protobus: stream released")

func (s *clientStream) release() {
	s.d.mu.Lock()
	if s.d.streams[s.id] == s {
		delete(s.d.streams, s.id)
	}
	s.d.mu.Unlock()
	s.mu.Lock()
	s.ended = true
	if s.err == nil {
		s.err = errStreamReleased
	}
	s.dropBufferLocked()
	s.mu.Unlock()
}

// stream publishes a request expecting a streaming reply and yields the raw
// reply frames. Nothing is published until the sequence is ranged over, and
// each range performs one call.
func (d *dispatcher) stream(ctx context.Context, routingKey string, body []byte, o *streamOptions) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}
		idle := d.bus.cfg.StreamIdleTimeout
		if o.idleTimeout > 0 {
			idle = o.idleTimeout
		}
		msg := amqp.Publishing{
			ContentType:   contentTypeOctetStream,
			CorrelationId: uuid.New(),
			DeliveryMode:  amqp.Persistent,
			Body:          body,
		}
		s, err := d.publishStream(ctx, routingKey, msg)
		if s != nil {
			defer s.release()
		}
		if err != nil {
			yield(nil, err)
			return
		}

		completed := false
		defer func() {
			if !completed {
				d.cancelStream(ctx, s.id)
			}
		}()

		timer := time.NewTimer(idle)
		defer timer.Stop()
		for {
			chunk, done, err := s.next()
			switch {
			case err != nil:
				completed = errors.Is(err, ErrDisconnected) || errors.Is(err, ErrClosed)
				yield(nil, err)
				return
			case chunk != nil:
				resetTimer(timer, idle)
				if !yield(chunk, nil) {
					return // the caller broke out: tell the producer to stop
				}
				resetTimer(timer, idle)
				continue
			case done:
				completed = true
				return
			}
			select {
			case <-s.notify:
				resetTimer(timer, idle)
			case <-timer.C:
				yield(nil, fmt.Errorf("%w: nothing for %v (stream %s)", ErrStreamTimeout, idle, s.id))
				return
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			}
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	t.Stop()
	t.Reset(d)
}

// publishStream registers a stream and publishes its request. Each attempt
// gets a fresh stream, so the failure of an attempt that never left cannot
// end the stream of the one that did.
func (d *dispatcher) publishStream(ctx context.Context, routingKey string, msg amqp.Publishing) (*clientStream, error) {
	for {
		pub, replyTo, err := d.channel(ctx)
		if err != nil {
			return nil, err
		}
		s := d.newStream(msg.CorrelationId)
		msg.ReplyTo = replyTo
		d.mu.Lock()
		d.streams[s.id] = s
		d.mu.Unlock()
		// Mandatory, so a streaming method no service is bound to fails fast
		// with ErrUnroutable instead of waiting out the idle timeout.
		err = pub.publish(ctx, d.bus.cfg.BusExchange, routingKey, true, msg)
		if err == nil {
			return s, nil
		}
		s.release()
		if errors.Is(err, errChannelGone) {
			continue
		}
		return nil, err
	}
}

// cancelStream tells the producer of a stream the caller has abandoned to
// stop. It is best effort and at most once per call: the notice is an
// ordinary message, and a producer that never hears it simply runs to
// completion.
func (d *dispatcher) cancelStream(ctx context.Context, correlationID string) {
	d.mu.Lock()
	pub := d.pub
	d.mu.Unlock()
	if pub == nil || pub.isClosed() {
		return
	}
	// Bounded and detached from the caller's context, which is often the
	// reason the stream ended.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), min(d.bus.cfg.PublishConfirmTimeout, 5*time.Second))
	defer cancel()
	err := pub.publish(cctx, d.bus.cfg.CancelExchange, "", false, amqp.Publishing{
		ContentType:   contentTypeOctetStream,
		CorrelationId: correlationID,
	})
	if err != nil {
		d.bus.log.LogAttrs(ctx, slog.LevelDebug, "failed to publish stream cancel", attrOperation("cancel"),
			attrCorrelationID(correlationID), attrSafeError(err))
	}
}
