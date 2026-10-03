package protobus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
	"github.com/ArielLaub/protobus-go/v2/internal/uuid"
)

// handlerResult is what a delivery handler decided.
type handlerResult struct {
	// reply is published to the request's replyTo before the request is
	// acknowledged. nil publishes nothing.
	reply []byte
	// err fails the attempt: it is settled through the retry ladder.
	err error
	// errReply is the caller-facing error, published on terminal paths (the
	// dead-letter queue, a reject, early ack). Never on a retry: the caller
	// stays parked while the message is retried.
	errReply []byte
	// handled marks err as deliberate, so it is not retried.
	handled bool
}

type deliveryHandler func(ctx context.Context, d *amqp.Delivery, ctl *deliveryControl) handlerResult

// deliveryControl is what a handler may do besides return a result.
type deliveryControl struct {
	pub      *pubChannel
	once     sync.Once
	disarmed chan struct{}

	cancels  *cancelRegistry
	id       string
	cancel   context.CancelCauseFunc
	unlisten func()
}

// cancellable lets the caller's cancel notices reach this delivery. Only
// streams register: a cancel is how a streaming caller abandons its call,
// and no port sends one for anything else.
func (c *deliveryControl) cancellable() {
	if c.unlisten == nil && c.cancels != nil {
		c.unlisten = c.cancels.add(c.id, c.cancel)
	}
}

func (c *deliveryControl) release() {
	if c.unlisten != nil {
		c.unlisten()
	}
}

// disarmTimeout lifts the processing timeout for the rest of this delivery.
// A streaming method calls it: a stream is bounded by its caller's idle
// timeout and cancellation, not by a per-message deadline.
func (c *deliveryControl) disarmTimeout() { c.once.Do(func() { close(c.disarmed) }) }

type retrySpec struct {
	maxRetries int
	exchange   string // topic exchange the retry queue is bound to with '#'
	queue      string
	dlq        string
}

type consumerSpec struct {
	// queue is the queue name, or "" for a server-named, exclusive,
	// auto-delete queue that lives as long as the connection.
	queue     string
	queueArgs amqp.Table
	exchange  string
	bindings  func() []string
	// declare runs after the queue exists and before consuming starts:
	// retry topology, extra bindings.
	declare func(ch transport.Channel, queue string) error
	lateAck bool
	// prefetch bounds unacknowledged deliveries (late ack).
	prefetch int
	// concurrency bounds concurrent handlers under early ack; 0 is unbounded.
	concurrency int
	timeout     time.Duration
	retry       *retrySpec
	// dlqHandled sends a handled failure straight to the DLQ when one exists,
	// rather than dropping it (event subscriptions).
	dlqHandled   bool
	timeoutReply func(d *amqp.Delivery, err error) []byte
	handle       deliveryHandler
	describe     string // for logs: the service or listener name
}

// consumer consumes one queue: the channel, its topology, the consume loop
// and the settlement of every delivery.
//
// Deliveries are handled concurrently, a goroutine each, with the broker's
// prefetch as the bound under late ack. Settlement of each delivery is
// independent (single-tag acks), so a slow message never holds up the rest.
type consumer struct {
	bus  *Bus
	spec consumerSpec

	mu         sync.Mutex
	ch         transport.Channel
	pub        *pubChannel
	queue      string
	tag        string
	consuming  bool // the application wants deliveries
	closed     bool
	loopDone   chan struct{}
	loopStop   chan struct{} // closed to make the current loop let go
	sem        chan struct{}
	unregister func()
	// handlers in flight, so a lost connection or a close can cancel their
	// contexts: their deliveries can no longer be settled.
	active map[*activeHandler]struct{}
}

type activeHandler struct{ cancel context.CancelCauseFunc }

func newConsumer(b *Bus, spec consumerSpec) *consumer {
	c := &consumer{bus: b, spec: spec, active: map[*activeHandler]struct{}{}}
	if !spec.lateAck && spec.concurrency > 0 {
		c.sem = make(chan struct{}, spec.concurrency)
	}
	return c
}

func (c *consumer) queueName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queue
}

// restoreTopology declares the consumer's topology on a fresh channel and,
// if the application is consuming, starts the consumer.
func (c *consumer) restoreTopology(ctx context.Context, conn transport.Conn) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	fail := func(err error) error { _ = ch.Close(); return err }
	pub, err := newPubChannel(ch, c.bus.cfg)
	if err != nil {
		return fail(err)
	}
	// A prefetch bounds what the broker pushes into process memory. Under
	// early ack it is the only bound: deliveries are acknowledged only once a
	// handler slot is free.
	if err := ch.Qos(c.spec.prefetch, 0, false); err != nil {
		return fail(err)
	}
	if err := ch.ExchangeDeclare(c.spec.exchange, "topic", true, false, false, false, nil); err != nil {
		return fail(fmt.Errorf("declaring exchange %s: %w", c.spec.exchange, err))
	}
	anonymous := c.spec.queue == ""
	q, err := ch.QueueDeclare(c.spec.queue, !anonymous, anonymous, anonymous, false, c.spec.queueArgs)
	if err != nil {
		return fail(fmt.Errorf("declaring queue %s: %w", c.spec.queue, err))
	}
	for _, key := range c.spec.bindings() {
		if err := ch.QueueBind(q.Name, key, c.spec.exchange, false, nil); err != nil {
			return fail(fmt.Errorf("binding %s to %s: %w", q.Name, key, err))
		}
	}
	if c.spec.declare != nil {
		if err := c.spec.declare(ch, q.Name); err != nil {
			return fail(err)
		}
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		pub.close()
		return nil
	}
	old, oldDone, oldStop := c.pub, c.loopDone, c.loopStop
	c.ch, c.pub, c.queue, c.loopDone, c.loopStop = ch, pub, q.Name, nil, nil
	consuming := c.consuming
	c.mu.Unlock()
	if old != nil && old != pub {
		// The old loop lets go of any delivery it holds (an early-ack loop
		// can be parked on a handler slot), so waiting for it is short.
		if oldStop != nil {
			close(oldStop)
		}
		old.close()
		if oldDone != nil {
			<-oldDone
		}
	}
	if consuming {
		return c.startConsuming(ctx)
	}
	return nil
}

func (c *consumer) startConsuming(ctx context.Context) error {
	c.mu.Lock()
	ch, pub, queue := c.ch, c.pub, c.queue
	c.mu.Unlock()
	if ch == nil {
		return fmt.Errorf("%w: no channel", ErrNotReady)
	}
	tag := uuid.New()
	exclusive := c.spec.queue == ""
	deliveries, err := ch.ConsumeWithContext(context.WithoutCancel(ctx), queue, tag, false, exclusive, false, false, nil)
	if err != nil {
		return fmt.Errorf("consuming %s: %w", queue, err)
	}
	done, stop := make(chan struct{}), make(chan struct{})
	c.mu.Lock()
	// The application may have stopped consuming (a shutdown during a
	// reconnection) while the consumer was being registered: honour that.
	keep := c.consuming && !c.closed && c.ch == ch
	if keep {
		c.tag = tag
	}
	c.loopDone, c.loopStop = done, stop
	c.mu.Unlock()
	go c.loop(ch, pub, deliveries, done, stop)
	if !keep {
		_ = ch.Cancel(tag, false)
		return nil
	}
	c.bus.log.LogAttrs(ctx, slog.LevelDebug, "consuming", attrOperation("consume"), attrQueue(queue), attrService(c.spec.describe))
	return nil
}

// start declares the topology and begins consuming. A start that fails can
// be retried.
func (c *consumer) start(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.consuming = true
	c.mu.Unlock()
	unregister, err := c.bus.attach(ctx, c)
	if err != nil {
		c.mu.Lock()
		c.consuming = false
		c.mu.Unlock()
		return err
	}
	c.mu.Lock()
	c.unregister = unregister
	c.mu.Unlock()
	return nil
}

func (c *consumer) loop(ch transport.Channel, pub *pubChannel, deliveries <-chan amqp.Delivery, done, stop chan struct{}) {
	defer close(done)
	for d := range deliveries {
		// Counted from receipt, so a drain cannot observe zero while a
		// delivery waits for a handler slot.
		c.bus.deliveries.add()
		if c.sem != nil {
			select {
			case c.sem <- struct{}{}:
			case <-stop:
				// Let go: unacknowledged, the broker redelivers it.
				c.bus.deliveries.done()
				continue
			}
		}
		go func() {
			defer c.bus.deliveries.done()
			if c.sem != nil {
				defer func() { <-c.sem }()
			}
			c.process(&d, pub)
		}()
	}
	// The deliveries ended. A deliberate stop, a closed bus and a lost
	// connection are all expected; a channel that died on a live connection
	// is not, and is rebuilt.
	c.mu.Lock()
	unexpected := c.consuming && !c.closed && c.ch == ch
	c.mu.Unlock()
	if _, _, ready := c.bus.sess.current(); unexpected && ready {
		go c.recover(ch)
	}
}

func (c *consumer) recover(dead transport.Channel) {
	delay := c.bus.cfg.Reconnect.InitialDelay
	for {
		retry, err := c.tryRecover(dead)
		if !retry {
			return
		}
		c.bus.log.LogAttrs(c.bus.sess.ctx, slog.LevelWarn, "failed to rebuild a consumer channel",
			attrOperation("consume"), attrService(c.spec.describe), attrSafeError(err), attrDuration(delay))
		if !sleepCtx(c.bus.sess.ctx, delay) {
			return
		}
		delay = min(delay*2, c.bus.cfg.Reconnect.MaxDelay)
	}
}

func (c *consumer) tryRecover(dead transport.Channel) (retry bool, err error) {
	c.bus.sess.restoreMu.Lock()
	defer c.bus.sess.restoreMu.Unlock()
	c.mu.Lock()
	stale := c.closed || !c.consuming || c.ch != dead
	c.mu.Unlock()
	conn, _, ready := c.bus.sess.current()
	if stale || !ready || conn.IsClosed() {
		return false, nil
	}
	c.bus.log.LogAttrs(c.bus.sess.ctx, slog.LevelWarn, "consumer channel closed on a live connection; rebuilding",
		attrOperation("consume"), attrService(c.spec.describe))
	if err := c.restoreTopology(c.bus.sess.ctx, conn); err != nil {
		return true, err
	}
	return false, nil
}

// connectionLost cancels the handlers of a late-ack consumer: their
// deliveries can no longer be acknowledged and will be redelivered, so the
// work is wasted. Early-acked work is the only copy and is left to finish.
func (c *consumer) connectionLost(error) {
	if c.spec.lateAck {
		c.cancelActive(ErrDisconnected)
	}
}

func (c *consumer) cancelActive(cause error) {
	c.mu.Lock()
	active := make([]*activeHandler, 0, len(c.active))
	for h := range c.active {
		active = append(active, h)
	}
	c.mu.Unlock()
	for _, h := range active {
		h.cancel(cause)
	}
}

// stopConsuming cancels the consumer, leaving the channel open so work in
// hand can still publish its replies and settle. It is the first step of a
// graceful shutdown, and it is final: a reconnection does not resume it.
func (c *consumer) stopConsuming() {
	c.mu.Lock()
	c.consuming = false
	ch, tag := c.ch, c.tag
	c.tag = ""
	c.mu.Unlock()
	if ch != nil && tag != "" && !ch.IsClosed() {
		_ = ch.Cancel(tag, false)
	}
}

// close stops the consumer for good and closes its channel. Handlers still
// running are cancelled with ErrClosed: their deliveries can no longer settle.
// It is idempotent.
func (c *consumer) close() {
	c.stopConsuming()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	pub, done, stop, unregister := c.pub, c.loopDone, c.loopStop, c.unregister
	c.pub, c.ch = nil, nil
	c.mu.Unlock()
	if unregister != nil {
		unregister()
	}
	if stop != nil {
		close(stop)
	}
	c.cancelActive(ErrClosed)
	if pub != nil {
		pub.close()
	}
	if done != nil {
		<-done
	}
}

// bind adds a binding to the live channel. Bindings are also re-applied on
// every restore. It runs under the restore lock, so it cannot slip between a
// restore reading the bindings and installing its channel.
func (c *consumer) bind(key string) error {
	c.bus.sess.restoreMu.Lock()
	defer c.bus.sess.restoreMu.Unlock()
	c.mu.Lock()
	ch, queue := c.ch, c.queue
	c.mu.Unlock()
	if ch == nil || ch.IsClosed() {
		return nil // the next restore applies it
	}
	return ch.QueueBind(queue, key, c.spec.exchange, false, nil)
}

// ---- per-delivery processing ---------------------------------------------------

func (c *consumer) process(d *amqp.Delivery, pub *pubChannel) {
	log := c.bus.log
	if !c.spec.lateAck {
		// Early ack: at most once. Retries and dead-lettering are impossible,
		// but a failure is still reported to the caller.
		if err := d.Ack(false); err != nil {
			// The channel is gone and the broker will redeliver the message.
			// Running it here as well would break at-most-once.
			log.LogAttrs(context.Background(), slog.LevelWarn, "early ack failed; leaving the message to its redelivery",
				attrOperation("ack"), attrQueue(c.queueName()), attrCorrelationID(d.CorrelationId), attrSafeError(err))
			return
		}
	}

	ctx, cancel := context.WithCancelCause(c.bus.sess.ctx)
	defer cancel(nil)
	h := &activeHandler{cancel: cancel}
	c.mu.Lock()
	c.active[h] = struct{}{}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.active, h)
		c.mu.Unlock()
	}()

	ctl := &deliveryControl{pub: pub, disarmed: make(chan struct{}), cancels: c.bus.cancels, id: d.CorrelationId, cancel: cancel}
	defer ctl.release()
	start := time.Now()
	res := c.run(ctx, cancel, d, ctl)

	switch cause := context.Cause(ctx); {
	case errors.Is(cause, ErrCancelled):
		// The caller asked to stop. That is a normal ending: settle without
		// a reply, a retry or the dead-letter queue.
		if c.spec.lateAck {
			c.ack(d)
		}
		log.LogAttrs(ctx, slog.LevelDebug, "delivery ended by its caller", attrOperation("consume"),
			attrCorrelationID(d.CorrelationId), attrOutcome(outcomeCancelled), attrDuration(time.Since(start)))
		return
	case errors.Is(cause, ErrDisconnected), errors.Is(cause, ErrClosed):
		// Its channel is gone; the broker redelivers it.
		return
	}
	c.settle(d, pub, res)
}

// run executes the handler, racing it against the processing timeout. A
// handler that overruns is abandoned, not stopped (Go cannot preempt it): its
// context is cancelled, the attempt fails, and the goroutine is still counted
// as running until it returns, so a drain does not report done while user
// code is mid-transaction.
func (c *consumer) run(ctx context.Context, cancel context.CancelCauseFunc, d *amqp.Delivery, ctl *deliveryControl) handlerResult {
	done := make(chan handlerResult, 1)
	c.bus.handlers.add()
	go func() {
		defer c.bus.handlers.done()
		defer func() {
			if v := recover(); v != nil {
				err := fmt.Errorf("protobus: handler panicked: %v", v)
				c.bus.log.LogAttrs(ctx, slog.LevelError, "handler panicked", attrOperation("consume"),
					attrQueue(c.queueName()), attrCorrelationID(d.CorrelationId), slog.Any("panic", v),
					slog.String("stack", string(debug.Stack())))
				var reply []byte
				if c.spec.timeoutReply != nil {
					reply = c.spec.timeoutReply(d, err)
				}
				done <- handlerResult{err: err, errReply: reply}
			}
		}()
		done <- c.spec.handle(ctx, d, ctl)
	}()

	if c.spec.timeout <= 0 {
		return <-done
	}
	timer := time.NewTimer(c.spec.timeout)
	defer timer.Stop()
	select {
	case r := <-done:
		return r
	case <-ctl.disarmed:
		return <-done
	case <-timer.C:
		err := newProcessingTimeoutError(d.CorrelationId, c.spec.timeout)
		cancel(err)
		c.bus.log.LogAttrs(ctx, slog.LevelError, "handler exceeded the processing timeout", attrOperation("consume"),
			attrQueue(c.queueName()), attrCorrelationID(d.CorrelationId), attrOutcome(outcomeTimeout),
			attrDuration(c.spec.timeout))
		var reply []byte
		if c.spec.timeoutReply != nil {
			reply = c.spec.timeoutReply(d, err)
		}
		return handlerResult{err: err, errReply: reply}
	}
}

// settle answers and settles one delivery. Every path publishes first and
// settles second (reply then ack; retry copy then ack; DLQ copy then ack), so
// the worst a failure can do is cause a redelivery, never lose the message.
func (c *consumer) settle(d *amqp.Delivery, pub *pubChannel, res handlerResult) {
	log := c.bus.log
	ctx := context.Background()
	queue := c.queueName()

	if res.err == nil {
		if res.reply != nil && d.ReplyTo != "" {
			if err := c.publishReply(pub, d, res.reply, nil); err != nil {
				c.requeueLater(d, err)
				return
			}
		}
		if c.spec.lateAck {
			c.ack(d)
		}
		return
	}

	if !c.spec.lateAck {
		c.replyError(pub, d, res.errReply)
		return
	}

	retry := c.spec.retry
	switch {
	case retry != nil && !res.handled:
		attempt := retryCount(d.Headers)
		if attempt < retry.maxRetries {
			// The caller stays parked: no reply while the message is retried.
			if err := c.publishRetry(pub, d, attempt, res.err); err != nil {
				c.requeueLater(d, err)
				return
			}
			c.ack(d)
			log.LogAttrs(ctx, slog.LevelWarn, "retrying message", attrOperation("retry"), attrQueue(queue),
				attrCorrelationID(d.CorrelationId), attrMessageID(d.MessageId), attrAttempt(attempt+1),
				attrOutcome(outcomeRetried), attrSafeError(res.err))
			return
		}
		c.deadLetter(pub, d, attempt, res)
	case retry != nil && res.handled && c.spec.dlqHandled:
		c.deadLetter(pub, d, retryCount(d.Headers), res)
	default:
		c.replyError(pub, d, res.errReply)
		if err := d.Reject(false); err != nil {
			log.LogAttrs(ctx, slog.LevelWarn, "reject failed", attrOperation("reject"), attrQueue(queue),
				attrCorrelationID(d.CorrelationId), attrSafeError(err))
			return
		}
		log.LogAttrs(ctx, slog.LevelWarn, "rejected message", attrOperation("reject"), attrQueue(queue),
			attrCorrelationID(d.CorrelationId), attrMessageID(d.MessageId), attrOutcome(outcomeRejected), attrSafeError(res.err))
	}
}

func (c *consumer) deadLetter(pub *pubChannel, d *amqp.Delivery, attempt int, res handlerResult) {
	// The reply is best effort and goes first: the caller has its own
	// timeout, while the DLQ is the only durable record of the message.
	c.replyError(pub, d, res.errReply)
	if err := c.publishDeadLetter(pub, d, attempt, res.err); err != nil {
		c.requeueLater(d, err)
		return
	}
	c.ack(d)
	c.bus.log.LogAttrs(context.Background(), slog.LevelError, "message dead-lettered", attrOperation("dead-letter"),
		attrQueue(c.queueName()), attrCorrelationID(d.CorrelationId), attrMessageID(d.MessageId),
		attrAttempt(attempt), attrOutcome(outcomeDeadLetter), attrSafeError(res.err))
}

func (c *consumer) ack(d *amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		// The channel is gone; the broker redelivers the message.
		c.bus.log.LogAttrs(context.Background(), slog.LevelWarn, "ack failed; the message will be redelivered",
			attrOperation("ack"), attrQueue(c.queueName()), attrCorrelationID(d.CorrelationId), attrSafeError(err))
	}
}

// requeueLater handles a settlement that could not be published: the message
// goes back to the queue after a pause, rather than sitting unacknowledged on
// a prefetch slot until its channel dies.
func (c *consumer) requeueLater(d *amqp.Delivery, cause error) {
	if !c.spec.lateAck {
		// Already acknowledged on arrival; settling it again would be a
		// channel error. All that is left is to say so.
		c.bus.log.LogAttrs(context.Background(), slog.LevelError, "failed to publish the reply of an early-acked message",
			attrOperation("settle"), attrQueue(c.queueName()), attrCorrelationID(d.CorrelationId), attrSafeError(cause))
		return
	}
	c.bus.log.LogAttrs(context.Background(), slog.LevelError, "failed to settle message; requeueing it",
		attrOperation("settle"), attrQueue(c.queueName()), attrCorrelationID(d.CorrelationId), attrSafeError(cause))
	sleepCtx(c.bus.sess.ctx, requeueDelay)
	if err := d.Nack(false, true); err != nil {
		c.bus.log.LogAttrs(context.Background(), slog.LevelWarn, "requeue failed; the broker redelivers on channel close",
			attrOperation("settle"), attrCorrelationID(d.CorrelationId), attrSafeError(err))
	}
}

// requeueDelay paces requeues of messages whose settlement failed.
var requeueDelay = time.Second

func (c *consumer) publishReply(pub *pubChannel, d *amqp.Delivery, body []byte, headers amqp.Table) error {
	return pub.publish(context.Background(), c.bus.cfg.CallbacksExchange, d.ReplyTo, false, amqp.Publishing{
		ContentType:   contentTypeOctetStream,
		CorrelationId: d.CorrelationId,
		Headers:       headers,
		Body:          body,
	})
}

// replyError tells the caller about a terminal failure. Best effort: a
// failure here is logged and settlement goes on, because the caller has its
// own timeout and the message's settlement must not hinge on the reply.
func (c *consumer) replyError(pub *pubChannel, d *amqp.Delivery, body []byte) {
	if body == nil || d.ReplyTo == "" {
		return
	}
	if err := c.publishReply(pub, d, body, nil); err != nil {
		c.bus.log.LogAttrs(context.Background(), slog.LevelError,
			"failed to publish the error reply; the caller will time out", attrOperation("reply"),
			attrCorrelationID(d.CorrelationId), attrSafeError(err))
	}
}

func originalRoutingKey(d *amqp.Delivery) string {
	switch v := d.Headers[headerOriginalKey].(type) {
	case string:
		if v != "" {
			return v
		}
	case []byte:
		if len(v) > 0 {
			return string(v)
		}
	}
	return d.RoutingKey
}

func firstFailure(d *amqp.Delivery) any {
	if v, ok := d.Headers[headerFirstFailure]; ok {
		if n, ok := headerInt(v); ok && n != 0 {
			return v // carried forward as it arrived
		}
	}
	return time.Now().UnixMilli()
}

// publishRetry parks the message on the retry queue, through the retry
// exchange and under its original routing key: when the queue's TTL expires,
// the broker dead-letters it back to the service's exchange under that key,
// which is what makes it route to the service queue again.
func (c *consumer) publishRetry(pub *pubChannel, d *amqp.Delivery, attempt int, cause error) error {
	key := originalRoutingKey(d)
	headers := maps.Clone(d.Headers)
	if headers == nil {
		headers = amqp.Table{}
	}
	headers[headerRetryCount] = intHeader(int64(attempt + 1))
	headers[headerOriginalKey] = key
	headers[headerFirstFailure] = firstFailure(d)
	headers[headerLastError] = safeErrorSummary(cause)
	msg := carriedProperties(d)
	msg.Headers = headers
	msg.DeliveryMode = amqp.Persistent
	msg.CorrelationId = d.CorrelationId
	msg.MessageId = d.MessageId
	msg.ReplyTo = d.ReplyTo
	msg.Body = d.Body
	// Mandatory: a retry queue that has gone missing must not swallow the
	// message while the original is acknowledged.
	return pub.publish(context.Background(), c.spec.retry.exchange, key, true, msg)
}

func (c *consumer) publishDeadLetter(pub *pubChannel, d *amqp.Delivery, attempt int, cause error) error {
	headers := maps.Clone(d.Headers)
	if headers == nil {
		headers = amqp.Table{}
	}
	headers[headerRetryCount] = intHeader(int64(attempt))
	headers[headerOriginalKey] = originalRoutingKey(d)
	headers[headerOriginalQueue] = c.queueName()
	headers[headerFirstFailure] = firstFailure(d)
	headers[headerDeadLetterTime] = time.Now().UnixMilli()
	headers[headerLastError] = safeErrorSummary(cause)
	msg := carriedProperties(d)
	msg.Headers = headers
	msg.DeliveryMode = amqp.Persistent
	msg.CorrelationId = d.CorrelationId
	msg.MessageId = d.MessageId
	// No replyTo: nothing should answer from the DLQ.
	msg.Body = d.Body
	return pub.publish(context.Background(), "", c.spec.retry.dlq, true, msg)
}
