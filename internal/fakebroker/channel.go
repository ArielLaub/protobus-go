package fakebroker

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

// ---- connection -------------------------------------------------------------

type conn struct {
	b        *Broker
	channels map[*channel]struct{}
	closed   bool
	closeL   []chan *amqp.Error
	nextID   int
}

func (c *conn) Channel() (transport.Channel, error) {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	if c.closed {
		return nil, amqp.ErrClosed
	}
	c.nextID++
	ch := &channel{c: c, id: c.nextID, consumers: map[string]*consumer{}, unacked: map[uint64]*inflight{}}
	ch.events = newEventPump()
	c.channels[ch] = struct{}{}
	return ch, nil
}

func (c *conn) NotifyClose(l chan *amqp.Error) chan *amqp.Error {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	if c.closed {
		close(l)
		return l
	}
	c.closeL = append(c.closeL, l)
	return l
}

func (c *conn) IsClosed() bool {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	return c.closed
}

func (c *conn) Close() error {
	c.b.mu.Lock()
	if c.closed {
		c.b.mu.Unlock()
		return amqp.ErrClosed
	}
	var post []func()
	c.shutdown(nil, &post)
	c.b.mu.Unlock()
	run(post)
	return nil
}

// shutdown closes the connection and its channels. A nil err is a graceful,
// client-initiated close. Caller holds b.mu.
func (c *conn) shutdown(err *amqp.Error, post *[]func()) {
	if c.closed {
		return
	}
	c.closed = true
	for ch := range c.channels {
		ch.shutdown(err, post)
	}
	for name, q := range c.b.queues {
		if q.exclusv && q.owner == c {
			c.b.deleteQueue(q, post)
			delete(c.b.queues, name)
		}
	}
	delete(c.b.conns, c)
	notifyClose(c.closeL, err, post)
	c.closeL = nil
}

// notifyClose sends err (when non-nil) to each listener, then closes it, as
// amqp091 does.
func notifyClose(ls []chan *amqp.Error, err *amqp.Error, post *[]func()) {
	for _, l := range ls {
		l := l
		*post = append(*post, func() {
			go func() {
				if err != nil {
					l <- err
				}
				close(l)
			}()
		})
	}
}

// ---- channel ----------------------------------------------------------------

type channel struct {
	c          *conn
	id         int
	closed     bool
	confirm    bool
	publishSeq uint64
	prefetch   int
	nextTag    uint64
	consumers  map[string]*consumer
	unacked    map[uint64]*inflight
	closeL     []chan *amqp.Error
	publishL   []chan amqp.Confirmation
	returnL    []chan amqp.Return
	cancelL    []chan string
	events     *eventPump
	ctagSeq    int
}

type inflight struct {
	msg  *message
	q    *queue
	cons *consumer
}

var _ transport.Channel = (*channel)(nil)

func (ch *channel) b() *Broker { return ch.c.b }

// fail raises a channel exception: the channel closes with err, which is also
// returned to the caller of the failing method.
func (ch *channel) fail(err *amqp.Error, post *[]func()) error {
	ch.shutdown(err, post)
	return err
}

func (ch *channel) shutdown(err *amqp.Error, post *[]func()) {
	if ch.closed {
		return
	}
	ch.closed = true
	// Consumers go first, or the requeue below would hand the messages
	// straight back to this dying channel.
	for _, cons := range ch.consumers {
		ch.removeConsumer(cons, post)
	}
	// Unacknowledged messages go back to their queues, marked redelivered.
	tags := slices.Sorted(func(yield func(uint64) bool) {
		for t := range ch.unacked {
			if !yield(t) {
				return
			}
		}
	})
	for i := len(tags) - 1; i >= 0; i-- {
		in := ch.unacked[tags[i]]
		in.msg.redelivered = true
		ch.b().enqueue(in.q, in.msg, true, post)
	}
	ch.unacked = map[uint64]*inflight{}
	delete(ch.c.channels, ch)
	ch.events.stop()
	notifyClose(ch.closeL, err, post)
	publishL, returnL, cancelL := ch.publishL, ch.returnL, ch.cancelL
	*post = append(*post, func() {
		go func() {
			ch.events.wait()
			for _, l := range publishL {
				close(l)
			}
			for _, l := range returnL {
				close(l)
			}
			for _, l := range cancelL {
				close(l)
			}
		}()
	})
	ch.closeL, ch.publishL, ch.returnL, ch.cancelL = nil, nil, nil, nil
}

func (ch *channel) removeConsumer(cons *consumer, post *[]func()) {
	delete(ch.consumers, cons.tag)
	q := cons.q
	q.consumers = slices.DeleteFunc(q.consumers, func(c *consumer) bool { return c == cons })
	cons.out.stop()
	if q.autoDelete && q.hadConsumer && len(q.consumers) == 0 && !q.deleted {
		ch.b().deleteQueue(q, post)
	}
}

func (ch *channel) lock() (post *[]func(), unlock func()) {
	b := ch.b()
	b.mu.Lock()
	var p []func()
	return &p, func() { b.mu.Unlock(); run(p) }
}

func (ch *channel) Confirm(bool) error {
	_, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return amqp.ErrClosed
	}
	ch.confirm = true
	return nil
}

func (ch *channel) NotifyPublish(l chan amqp.Confirmation) chan amqp.Confirmation {
	_, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		close(l)
		return l
	}
	ch.publishL = append(ch.publishL, l)
	return l
}

func (ch *channel) NotifyReturn(l chan amqp.Return) chan amqp.Return {
	_, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		close(l)
		return l
	}
	ch.returnL = append(ch.returnL, l)
	return l
}

func (ch *channel) NotifyClose(l chan *amqp.Error) chan *amqp.Error {
	_, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		close(l)
		return l
	}
	ch.closeL = append(ch.closeL, l)
	return l
}

func (ch *channel) NotifyCancel(l chan string) chan string {
	_, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		close(l)
		return l
	}
	ch.cancelL = append(ch.cancelL, l)
	return l
}

func (ch *channel) Qos(prefetchCount, _ int, _ bool) error {
	_, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return amqp.ErrClosed
	}
	ch.prefetch = prefetchCount
	return nil
}

func (ch *channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, _ bool, _ amqp.Table) error {
	post, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return amqp.ErrClosed
	}
	b := ch.b()
	if ex, ok := b.exchanges[name]; ok {
		if ex.kind != kind || ex.durable != durable || ex.autoDelete != autoDelete || ex.intern != internal {
			return ch.fail(channelError(amqp.PreconditionFailed,
				"PRECONDITION_FAILED - inequivalent arg for exchange '%s'", name), post)
		}
		return nil
	}
	switch kind {
	case "direct", "topic", "fanout":
	default:
		return ch.fail(channelError(amqp.CommandInvalid, "COMMAND_INVALID - unknown exchange type '%s'", kind), post)
	}
	b.exchanges[name] = &exchange{name: name, kind: kind, durable: durable, autoDelete: autoDelete, intern: internal}
	return nil
}

func (ch *channel) QueueDeclare(name string, durable, autoDelete, exclusive, _ bool, args amqp.Table) (amqp.Queue, error) {
	post, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return amqp.Queue{}, amqp.ErrClosed
	}
	b := ch.b()
	if name == "" {
		b.nameSeq++
		name = fmt.Sprintf("amq.gen-%06d", b.nameSeq)
	}
	if q, ok := b.queues[name]; ok {
		if q.exclusv && q.owner != ch.c {
			return amqp.Queue{}, ch.fail(channelError(amqp.ResourceLocked,
				"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '%s'", name), post)
		}
		if q.durable != durable || q.autoDelete != autoDelete || q.exclusv != exclusive || !argsEquivalent(q.args, args) {
			return amqp.Queue{}, ch.fail(channelError(amqp.PreconditionFailed,
				"PRECONDITION_FAILED - inequivalent arg for queue '%s'", name), post)
		}
		return amqp.Queue{Name: name, Messages: len(q.msgs), Consumers: len(q.consumers)}, nil
	}
	q := &queue{name: name, durable: durable, autoDelete: autoDelete, exclusv: exclusive, args: args}
	if exclusive {
		q.owner = ch.c
	}
	if v, ok := args["x-message-ttl"]; ok {
		n, ok := intArg(v)
		if !ok || n < 0 {
			return amqp.Queue{}, ch.fail(channelError(amqp.PreconditionFailed,
				"PRECONDITION_FAILED - invalid arg 'x-message-ttl' for queue '%s'", name), post)
		}
		q.ttl = msDuration(n)
	}
	if v, ok := args["x-dead-letter-exchange"].(string); ok {
		q.dlx = &v
	}
	if v, ok := args["x-dead-letter-routing-key"].(string); ok {
		q.dlxKey = &v
	}
	if v, ok := args["x-max-priority"]; ok {
		n, ok := intArg(v)
		if !ok || n < 0 || n > 255 {
			return amqp.Queue{}, ch.fail(channelError(amqp.PreconditionFailed,
				"PRECONDITION_FAILED - invalid arg 'x-max-priority' for queue '%s'", name), post)
		}
		q.maxPriority = int(n)
	}
	b.queues[name] = q
	return amqp.Queue{Name: name}, nil
}

func (ch *channel) QueueBind(name, key, exchangeName string, _ bool, _ amqp.Table) error {
	post, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return amqp.ErrClosed
	}
	b := ch.b()
	ex, ok := b.exchanges[exchangeName]
	if !ok || exchangeName == "" {
		return ch.fail(channelError(amqp.NotFound, "NOT_FOUND - no exchange '%s'", exchangeName), post)
	}
	if _, ok := b.queues[name]; !ok {
		return ch.fail(channelError(amqp.NotFound, "NOT_FOUND - no queue '%s'", name), post)
	}
	bd := binding{queue: name, key: key}
	if !slices.Contains(ex.bindings, bd) {
		ex.bindings = append(ex.bindings, bd)
	}
	return nil
}

func (ch *channel) ConsumeWithContext(ctx context.Context, queueName, tag string, autoAck, exclusive, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	post, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return nil, amqp.ErrClosed
	}
	b := ch.b()
	q, ok := b.queues[queueName]
	if !ok {
		return nil, ch.fail(channelError(amqp.NotFound, "NOT_FOUND - no queue '%s'", queueName), post)
	}
	if q.exclusv && q.owner != ch.c {
		return nil, ch.fail(channelError(amqp.ResourceLocked,
			"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '%s'", queueName), post)
	}
	for _, c := range q.consumers {
		if exclusive || c.exclusive {
			return nil, ch.fail(channelError(amqp.AccessRefused,
				"ACCESS_REFUSED - queue '%s' in exclusive use", queueName), post)
		}
	}
	if tag == "" {
		ch.ctagSeq++
		tag = "amq.ctag-" + strconv.Itoa(ch.id) + "-" + strconv.Itoa(ch.ctagSeq)
	}
	if _, dup := ch.consumers[tag]; dup {
		return nil, ch.fail(channelError(amqp.NotAllowed, "NOT_ALLOWED - attempt to reuse consumer tag '%s'", tag), post)
	}
	cons := &consumer{tag: tag, ch: ch, q: q, autoAck: autoAck, exclusive: exclusive, prefetch: ch.prefetch, out: newDeliveryPump()}
	ch.consumers[tag] = cons
	q.consumers = append(q.consumers, cons)
	q.hadConsumer = true
	b.dispatch(q, post)
	if ctx.Done() != nil {
		context.AfterFunc(ctx, func() { _ = ch.Cancel(tag, false) })
	}
	return cons.out.out, nil
}

func (ch *channel) Cancel(tag string, _ bool) error {
	post, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return amqp.ErrClosed
	}
	if cons, ok := ch.consumers[tag]; ok {
		ch.removeConsumer(cons, post)
	}
	return nil
}

func (ch *channel) PublishWithContext(ctx context.Context, exchangeName, key string, mandatory, _ bool, msg amqp.Publishing) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	post, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return amqp.ErrClosed
	}
	b := ch.b()
	var tag uint64
	if ch.confirm {
		ch.publishSeq++
		tag = ch.publishSeq
	}
	b.ops = append(b.ops, Op{Kind: "publish", Exchange: exchangeName, Key: key, Mandatory: mandatory,
		MessageID: msg.MessageId, CorrelationID: msg.CorrelationId, Msg: clonePublishing(msg)})

	if _, ok := b.exchanges[exchangeName]; !ok {
		// RabbitMQ answers a publish to a missing exchange by closing the
		// channel, asynchronously: the publish call itself succeeds.
		ch.shutdown(channelError(amqp.NotFound, "NOT_FOUND - no exchange '%s' in vhost '/'", exchangeName), post)
		return nil
	}

	action := Ack
	if b.confirmPolicy != nil {
		action = b.confirmPolicy(Published{Exchange: exchangeName, Key: key, Mandatory: mandatory, Msg: msg})
	}
	switch action {
	case Nack:
		if ch.confirm {
			ch.emitConfirm(amqp.Confirmation{DeliveryTag: tag, Ack: false})
		}
		return nil
	case Drop:
		return nil
	}

	routed := b.route(exchangeName, key, msg, post)
	if !routed && mandatory {
		ret := amqp.Return{
			ReplyCode: amqp.NoRoute, ReplyText: "NO_ROUTE", Exchange: exchangeName, RoutingKey: key,
			ContentType: msg.ContentType, ContentEncoding: msg.ContentEncoding, Headers: msg.Headers,
			DeliveryMode: msg.DeliveryMode, Priority: msg.Priority, CorrelationId: msg.CorrelationId,
			ReplyTo: msg.ReplyTo, Expiration: msg.Expiration, MessageId: msg.MessageId,
			Timestamp: msg.Timestamp, Type: msg.Type, UserId: msg.UserId, AppId: msg.AppId, Body: msg.Body,
		}
		b.ops = append(b.ops, Op{Kind: "return", Exchange: exchangeName, Key: key, MessageID: msg.MessageId, CorrelationID: msg.CorrelationId})
		ch.emitReturn(ret)
	}
	if ch.confirm {
		var delay time.Duration
		if b.confirmDelay != nil {
			delay = b.confirmDelay(Published{Exchange: exchangeName, Key: key, Mandatory: mandatory, Msg: msg})
		}
		if delay <= 0 {
			ch.emitConfirm(amqp.Confirmation{DeliveryTag: tag, Ack: true})
			return nil
		}
		time.AfterFunc(delay, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if !ch.closed {
				ch.emitConfirm(amqp.Confirmation{DeliveryTag: tag, Ack: true})
			}
		})
	}
	return nil
}

func (ch *channel) emitReturn(r amqp.Return) {
	ls := slices.Clone(ch.returnL)
	ch.events.push(func(stop <-chan struct{}) {
		for _, l := range ls {
			select {
			case l <- r:
			case <-stop:
				return
			}
		}
	})
}

func (ch *channel) emitConfirm(c amqp.Confirmation) {
	ls := slices.Clone(ch.publishL)
	ch.events.push(func(stop <-chan struct{}) {
		for _, l := range ls {
			select {
			case l <- c:
			case <-stop:
				return
			}
		}
	})
}

func (ch *channel) GetNextPublishSeqNo() uint64 {
	_, unlock := ch.lock()
	defer unlock()
	return ch.publishSeq + 1
}

func (ch *channel) IsClosed() bool {
	_, unlock := ch.lock()
	defer unlock()
	return ch.closed
}

func (ch *channel) Close() error {
	post, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return amqp.ErrClosed
	}
	ch.shutdown(nil, post)
	return nil
}

// ---- settlement (amqp.Acknowledger) ----------------------------------------

type acker struct{ ch *channel }

func (a acker) settle(kind string, tag uint64, multiple, requeue bool) error {
	ch := a.ch
	post, unlock := ch.lock()
	defer unlock()
	if ch.closed {
		return amqp.ErrClosed
	}
	var tags []uint64
	if multiple {
		for t := range ch.unacked {
			if t <= tag {
				tags = append(tags, t)
			}
		}
		slices.Sort(tags)
	} else if _, ok := ch.unacked[tag]; ok {
		tags = []uint64{tag}
	}
	if len(tags) == 0 {
		ch.shutdown(channelError(amqp.PreconditionFailed, "PRECONDITION_FAILED - unknown delivery tag %d", tag), post)
		return nil
	}
	b := ch.b()
	touched := map[*queue]bool{}
	for _, t := range tags {
		in := ch.unacked[t]
		delete(ch.unacked, t)
		in.cons.unacked--
		b.ops = append(b.ops, Op{Kind: kind, Queue: in.q.name, Key: in.msg.key, Requeue: requeue,
			MessageID: in.msg.pub.MessageId, CorrelationID: in.msg.pub.CorrelationId, Msg: in.msg.pub})
		switch {
		case kind == "ack":
		case requeue:
			in.msg.redelivered = true
			b.enqueue(in.q, in.msg, true, post)
		default:
			b.deadLetter(in.q, in.msg, "rejected", post)
		}
		touched[in.q] = true
	}
	for q := range touched {
		b.dispatch(q, post)
	}
	return nil
}

func (a acker) Ack(tag uint64, multiple bool) error { return a.settle("ack", tag, multiple, false) }
func (a acker) Nack(tag uint64, multiple, requeue bool) error {
	return a.settle("nack", tag, multiple, requeue)
}
func (a acker) Reject(tag uint64, requeue bool) error { return a.settle("reject", tag, false, requeue) }

// ---- consumer -----------------------------------------------------------------

type consumer struct {
	tag       string
	ch        *channel
	q         *queue
	autoAck   bool
	exclusive bool
	prefetch  int
	unacked   int
	out       *deliveryPump
}

func (c *consumer) hasCapacity() bool {
	return c.autoAck || c.prefetch == 0 || c.unacked < c.prefetch
}

// deliver hands m to the consumer. Caller holds b.mu.
func (c *consumer) deliver(q *queue, m *message) {
	ch := c.ch
	ch.nextTag++
	tag := ch.nextTag
	if !c.autoAck {
		ch.unacked[tag] = &inflight{msg: m, q: q, cons: c}
		c.unacked++
	}
	p := m.pub
	c.out.push(amqp.Delivery{
		Acknowledger: acker{ch}, Headers: clonePublishing(p).Headers, ContentType: p.ContentType,
		ContentEncoding: p.ContentEncoding, DeliveryMode: p.DeliveryMode, Priority: p.Priority,
		CorrelationId: p.CorrelationId, ReplyTo: p.ReplyTo, Expiration: p.Expiration, MessageId: p.MessageId,
		Timestamp: p.Timestamp, Type: p.Type, UserId: p.UserId, AppId: p.AppId, ConsumerTag: c.tag,
		DeliveryTag: tag, Redelivered: m.redelivered, Exchange: m.exchange, RoutingKey: m.key, Body: p.Body,
	})
}

// ---- pumps --------------------------------------------------------------------

// deliveryPump feeds a consumer's deliveries channel from an unbounded
// buffer, as amqp091 does, so the broker never blocks on a slow consumer.
type deliveryPump struct {
	mu     sync.Mutex
	items  []amqp.Delivery
	signal chan struct{}
	quit   chan struct{}
	once   sync.Once
	out    chan amqp.Delivery
}

func newDeliveryPump() *deliveryPump {
	p := &deliveryPump{signal: make(chan struct{}, 1), quit: make(chan struct{}), out: make(chan amqp.Delivery)}
	go p.run()
	return p
}

func (p *deliveryPump) push(d amqp.Delivery) {
	p.mu.Lock()
	p.items = append(p.items, d)
	p.mu.Unlock()
	select {
	case p.signal <- struct{}{}:
	default:
	}
}

func (p *deliveryPump) stop() { p.once.Do(func() { close(p.quit) }) }

func (p *deliveryPump) run() {
	defer close(p.out)
	for {
		p.mu.Lock()
		if len(p.items) == 0 {
			p.mu.Unlock()
			select {
			case <-p.signal:
				continue
			case <-p.quit:
				return
			}
		}
		d := p.items[0]
		p.items = p.items[1:]
		p.mu.Unlock()
		select {
		case p.out <- d:
		case <-p.quit:
			return
		}
	}
}

// eventPump delivers a channel's returns and confirms in order, with the
// blocking sends amqp091 performs, so a return is always received before the
// confirm that follows it.
type eventPump struct {
	mu     sync.Mutex
	items  []func(stop <-chan struct{})
	signal chan struct{}
	quit   chan struct{}
	done   chan struct{}
	once   sync.Once
}

func newEventPump() *eventPump {
	p := &eventPump{signal: make(chan struct{}, 1), quit: make(chan struct{}), done: make(chan struct{})}
	go p.run()
	return p
}

func (p *eventPump) push(f func(stop <-chan struct{})) {
	p.mu.Lock()
	p.items = append(p.items, f)
	p.mu.Unlock()
	select {
	case p.signal <- struct{}{}:
	default:
	}
}

func (p *eventPump) stop() { p.once.Do(func() { close(p.quit) }) }
func (p *eventPump) wait() { <-p.done }

func (p *eventPump) run() {
	defer close(p.done)
	for {
		p.mu.Lock()
		if len(p.items) == 0 {
			p.mu.Unlock()
			select {
			case <-p.signal:
				continue
			case <-p.quit:
				return
			}
		}
		f := p.items[0]
		p.items = p.items[1:]
		p.mu.Unlock()
		select {
		case <-p.quit:
			return
		default:
		}
		f(p.quit)
	}
}
