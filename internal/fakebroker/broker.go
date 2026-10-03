// Package fakebroker is an in-memory AMQP 0-9-1 broker with RabbitMQ's
// semantics, for tests.
//
// It implements the transport interfaces and models the behaviours protobus
// relies on: direct, topic, fanout and default-exchange routing; server-named
// and exclusive queues; argument-equivalence checks that close the channel
// with 406; publisher confirms with basic.return arriving before the ack;
// per-consumer prefetch; ack, nack and reject with requeue; message TTL with
// dead-lettering under the original routing key (and x-death); priority
// queues; channel exceptions; and connection loss. Faults can be injected
// (dial failures, nacked or never-confirmed publishes), and every publish and
// settlement is recorded in order so tests can assert on sequencing.
//
// It is deliberately not a complete broker. The integration suite runs the
// same scenarios against RabbitMQ, which is what keeps this model honest.
package fakebroker

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

// ConfirmAction is the broker's answer to one publish.
type ConfirmAction int

const (
	Ack  ConfirmAction = iota // route the message and confirm it
	Nack                      // refuse the message (basic.nack); nothing is routed
	Drop                      // never confirm; nothing is routed
)

// Published describes a publish, as handed to a confirm policy.
type Published struct {
	Exchange  string
	Key       string
	Mandatory bool
	Msg       amqp.Publishing
}

// Op is one recorded broker operation.
type Op struct {
	Kind          string // publish, deliver, ack, nack, reject, return, expire
	Exchange      string
	Key           string
	Queue         string
	Mandatory     bool
	Requeue       bool
	MessageID     string
	CorrelationID string
	Msg           amqp.Publishing
}

// Broker is the in-memory broker. The zero value is not usable; call New.
type Broker struct {
	mu            sync.Mutex
	exchanges     map[string]*exchange
	queues        map[string]*queue
	conns         map[*conn]struct{}
	dialFault     error
	dials         int
	confirmPolicy func(Published) ConfirmAction
	confirmDelay  func(Published) time.Duration
	ops           []Op
	nameSeq       int
}

type exchange struct {
	name, kind                  string
	durable, autoDelete, intern bool
	bindings                    []binding
}

type binding struct{ queue, key string }

type queue struct {
	name                         string
	durable, autoDelete, exclusv bool
	owner                        *conn
	args                         amqp.Table
	ttl                          time.Duration
	dlx                          *string
	dlxKey                       *string
	maxPriority                  int
	msgs                         []*message
	consumers                    []*consumer
	rr                           int
	hadConsumer                  bool
	deleted                      bool
}

type message struct {
	pub         amqp.Publishing
	exchange    string
	key         string
	redelivered bool
	timer       *time.Timer
}

// New returns an empty broker with RabbitMQ's default exchange.
func New() *Broker {
	return &Broker{
		exchanges: map[string]*exchange{"": {name: "", kind: "direct", durable: true}},
		queues:    map[string]*queue{},
		conns:     map[*conn]struct{}{},
	}
}

var _ transport.Dialer = New().Dial

// Dial opens a connection, or fails with the injected dial fault.
func (b *Broker) Dial(ctx context.Context, _ string, _ amqp.Config) (transport.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dials++
	if b.dialFault != nil {
		return nil, b.dialFault
	}
	c := &conn{b: b, channels: map[*channel]struct{}{}}
	b.conns[c] = struct{}{}
	return c, nil
}

// DialPeer opens a connection that ignores injected dial faults and is not
// counted by Dials. Tests use it for processes standing in for other peers,
// which keep working while the code under test is kept off the broker.
func (b *Broker) DialPeer() transport.Conn {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := &conn{b: b, channels: map[*channel]struct{}{}}
	b.conns[c] = struct{}{}
	return c
}

// SetDialFault makes subsequent dials fail with err, until reset with nil.
func (b *Broker) SetDialFault(err error) {
	b.mu.Lock()
	b.dialFault = err
	b.mu.Unlock()
}

// Dials reports how many dials have been attempted.
func (b *Broker) Dials() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dials
}

// SetConfirmPolicy decides the confirm for every subsequent publish. nil
// restores the default, which acks everything.
func (b *Broker) SetConfirmPolicy(p func(Published) ConfirmAction) {
	b.mu.Lock()
	b.confirmPolicy = p
	b.mu.Unlock()
}

// SetConfirmDelay delays the confirm of matching publishes, as a broker does
// for a persistent message awaiting fsync. Routing (and any basic.return) is
// immediate; only the ack is late, and later publishes may be confirmed
// first. nil removes the delay.
func (b *Broker) SetConfirmDelay(f func(Published) time.Duration) {
	b.mu.Lock()
	b.confirmDelay = f
	b.mu.Unlock()
}

// Ops returns a copy of the operation log.
func (b *Broker) Ops() []Op {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.ops)
}

// OpsOf returns the recorded operations of one kind.
func (b *Broker) OpsOf(kind string) []Op {
	var out []Op
	for _, op := range b.Ops() {
		if op.Kind == kind {
			out = append(out, op)
		}
	}
	return out
}

// DeleteQueue deletes a queue as an operator would; its consumers are
// cancelled by the broker (basic.cancel).
func (b *Broker) DeleteQueue(name string) {
	b.mu.Lock()
	var post []func()
	if q, ok := b.queues[name]; ok {
		b.deleteQueue(q, &post)
	}
	b.mu.Unlock()
	run(post)
}

// HasQueue reports whether a queue exists.
func (b *Broker) HasQueue(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.queues[name]
	return ok
}

// HasExchange reports whether an exchange exists.
func (b *Broker) HasExchange(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.exchanges[name]
	return ok
}

// QueueInfo describes a queue.
type QueueInfo struct {
	Name                           string
	Durable, AutoDelete, Exclusive bool
	Args                           amqp.Table
	Messages                       int
	Consumers                      int
}

// Queue describes a queue, and reports whether it exists.
func (b *Broker) Queue(name string) (QueueInfo, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[name]
	if !ok {
		return QueueInfo{}, false
	}
	return QueueInfo{
		Name: q.name, Durable: q.durable, AutoDelete: q.autoDelete, Exclusive: q.exclusv,
		Args: maps.Clone(q.args), Messages: len(q.msgs), Consumers: len(q.consumers),
	}, true
}

// QueueDepth is the number of messages waiting (not delivered) in a queue.
func (b *Broker) QueueDepth(name string) int {
	info, _ := b.Queue(name)
	return info.Messages
}

// Messages returns the messages waiting in a queue, in delivery order.
func (b *Broker) Messages(name string) []amqp.Publishing {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[name]
	if !ok {
		return nil
	}
	out := make([]amqp.Publishing, len(q.msgs))
	for i, m := range q.msgs {
		out[i] = m.pub
	}
	return out
}

// Bindings returns the routing keys binding queue to exchange.
func (b *Broker) Bindings(exchangeName, queueName string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ex, ok := b.exchanges[exchangeName]
	if !ok {
		return nil
	}
	var keys []string
	for _, bd := range ex.bindings {
		if bd.queue == queueName {
			keys = append(keys, bd.key)
		}
	}
	return keys
}

// ExchangeKind returns an exchange's type, or "" if it does not exist.
func (b *Broker) ExchangeKind(name string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ex, ok := b.exchanges[name]; ok {
		return ex.kind
	}
	return ""
}

// Publish injects a message from outside any connection, as another process
// on the bus would.
func (b *Broker) Publish(exchangeName, key string, msg amqp.Publishing) error {
	b.mu.Lock()
	var post []func()
	if _, ok := b.exchanges[exchangeName]; !ok {
		b.mu.Unlock()
		return fmt.Errorf("fakebroker: no exchange %q", exchangeName)
	}
	b.ops = append(b.ops, Op{Kind: "publish", Exchange: exchangeName, Key: key, MessageID: msg.MessageId, CorrelationID: msg.CorrelationId, Msg: msg})
	b.route(exchangeName, key, msg, &post)
	b.mu.Unlock()
	run(post)
	return nil
}

// KillConnections drops every connection as a broker failure or network
// partition would: channels and connections close with CONNECTION_FORCED,
// unacknowledged messages are requeued and exclusive queues disappear.
func (b *Broker) KillConnections() {
	b.mu.Lock()
	var post []func()
	err := &amqp.Error{Code: amqp.ConnectionForced, Reason: "CONNECTION_FORCED - broker forced connection closure", Server: true, Recover: true}
	for c := range b.conns {
		c.shutdown(err, &post)
	}
	b.mu.Unlock()
	run(post)
}

// Restart simulates a broker restart: connections are killed, non-durable
// queues and exchanges vanish, and durable queues keep only persistent
// messages.
func (b *Broker) Restart() {
	b.KillConnections()
	b.mu.Lock()
	var post []func()
	defer func() { b.mu.Unlock(); run(post) }()
	for name, q := range b.queues {
		if !q.durable {
			b.deleteQueue(q, &post)
			delete(b.queues, name)
			continue
		}
		q.msgs = slices.DeleteFunc(q.msgs, func(m *message) bool {
			if m.pub.DeliveryMode != amqp.Persistent {
				m.stopTimer()
				return true
			}
			return false
		})
	}
	for name, ex := range b.exchanges {
		if name != "" && !ex.durable {
			delete(b.exchanges, name)
		}
	}
}

func run(post []func()) {
	for _, f := range post {
		f()
	}
}

func (m *message) stopTimer() {
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
}

// ---- routing --------------------------------------------------------------

// route delivers msg to every queue the exchange routes key to, and reports
// whether any queue received it. Caller holds b.mu.
func (b *Broker) route(exchangeName, key string, msg amqp.Publishing, post *[]func()) bool {
	ex := b.exchanges[exchangeName]
	var targets []*queue
	if exchangeName == "" {
		if q, ok := b.queues[key]; ok {
			targets = append(targets, q)
		}
	} else {
		seen := map[*queue]bool{}
		for _, bd := range ex.bindings {
			q, ok := b.queues[bd.queue]
			if !ok || seen[q] {
				continue
			}
			var hit bool
			switch ex.kind {
			case "fanout":
				hit = true
			case "direct":
				hit = bd.key == key
			case "topic":
				hit = topicMatch(strings.Split(bd.key, "."), strings.Split(key, "."))
			}
			if hit {
				seen[q] = true
				targets = append(targets, q)
			}
		}
	}
	for _, q := range targets {
		b.enqueue(q, &message{pub: clonePublishing(msg), exchange: exchangeName, key: key}, false, post)
	}
	return len(targets) > 0
}

func topicMatch(p, t []string) bool {
	switch {
	case len(p) == 0:
		return len(t) == 0
	case p[0] == "#":
		return topicMatch(p[1:], t) || len(t) > 0 && topicMatch(p, t[1:])
	case len(t) == 0:
		return false
	case p[0] == "*" || p[0] == t[0]:
		return topicMatch(p[1:], t[1:])
	}
	return false
}

func clonePublishing(p amqp.Publishing) amqp.Publishing {
	p.Headers = maps.Clone(p.Headers)
	p.Body = slices.Clone(p.Body)
	return p
}

// enqueue adds m to q (at the head when requeued) and dispatches.
func (b *Broker) enqueue(q *queue, m *message, front bool, post *[]func()) {
	if q.deleted {
		return
	}
	if front {
		q.msgs = slices.Insert(q.msgs, 0, m)
	} else {
		q.msgs = append(q.msgs, m)
	}
	if q.ttl > 0 && m.timer == nil {
		m.timer = time.AfterFunc(q.ttl, func() { b.expire(q, m) })
	}
	b.dispatch(q, post)
}

func (b *Broker) expire(q *queue, m *message) {
	b.mu.Lock()
	var post []func()
	i := slices.Index(q.msgs, m)
	if i < 0 || q.deleted {
		b.mu.Unlock()
		return
	}
	q.msgs = slices.Delete(q.msgs, i, i+1)
	m.timer = nil
	b.ops = append(b.ops, Op{Kind: "expire", Queue: q.name, Key: m.key, MessageID: m.pub.MessageId, CorrelationID: m.pub.CorrelationId, Msg: m.pub})
	b.deadLetter(q, m, "expired", &post)
	b.mu.Unlock()
	run(post)
}

// deadLetter republishes m to q's dead-letter exchange, if it has one.
func (b *Broker) deadLetter(q *queue, m *message, reason string, post *[]func()) {
	if q.dlx == nil {
		return
	}
	if _, ok := b.exchanges[*q.dlx]; !ok {
		return
	}
	key := m.key
	if q.dlxKey != nil {
		key = *q.dlxKey
	}
	pub := clonePublishing(m.pub)
	if pub.Headers == nil {
		pub.Headers = amqp.Table{}
	}
	death := amqp.Table{
		"count": int64(1), "reason": reason, "queue": q.name, "time": time.Now().Truncate(time.Second),
		"exchange": m.exchange, "routing-keys": []any{m.key},
	}
	prior, _ := pub.Headers["x-death"].([]any)
	pub.Headers["x-death"] = append([]any{death}, prior...)
	b.route(*q.dlx, key, pub, post)
}

// dispatch hands waiting messages to consumers with spare prefetch capacity,
// round-robin. Caller holds b.mu.
func (b *Broker) dispatch(q *queue, post *[]func()) {
	for len(q.msgs) > 0 && len(q.consumers) > 0 {
		var target *consumer
		for i := range q.consumers {
			c := q.consumers[(q.rr+i)%len(q.consumers)]
			if c.hasCapacity() {
				target = c
				q.rr = (q.rr + i + 1) % len(q.consumers)
				break
			}
		}
		if target == nil {
			return
		}
		i := q.next()
		m := q.msgs[i]
		q.msgs = slices.Delete(q.msgs, i, i+1)
		m.stopTimer()
		target.deliver(q, m)
		b.ops = append(b.ops, Op{Kind: "deliver", Queue: q.name, Key: m.key, MessageID: m.pub.MessageId, CorrelationID: m.pub.CorrelationId, Msg: m.pub})
	}
}

// next picks the message to deliver: the highest priority, FIFO within one.
func (q *queue) next() int {
	if q.maxPriority == 0 {
		return 0
	}
	best, bestP := 0, -1
	for i, m := range q.msgs {
		p := min(int(m.pub.Priority), q.maxPriority)
		if p > bestP {
			best, bestP = i, p
		}
	}
	return best
}

func (b *Broker) deleteQueue(q *queue, post *[]func()) {
	q.deleted = true
	for _, m := range q.msgs {
		m.stopTimer()
	}
	q.msgs = nil
	for _, ex := range b.exchanges {
		ex.bindings = slices.DeleteFunc(ex.bindings, func(bd binding) bool { return bd.queue == q.name })
	}
	for _, c := range slices.Clone(q.consumers) {
		c.ch.removeConsumer(c, post)
		for _, l := range c.ch.cancelL {
			l, tag := l, c.tag
			*post = append(*post, func() { l <- tag })
		}
	}
	delete(b.queues, q.name)
}

// ---- argument handling ------------------------------------------------------

func intArg(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	}
	return 0, false
}

// argsEquivalent mirrors RabbitMQ's check: integer widths form one class.
func argsEquivalent(a, b amqp.Table) bool {
	norm := func(t amqp.Table) map[string]string {
		out := map[string]string{}
		for k, v := range t {
			if n, ok := intArg(v); ok {
				out[k] = "int:" + strconv.FormatInt(n, 10)
				continue
			}
			out[k] = fmt.Sprintf("%T:%v", v, v)
		}
		return out
	}
	return maps.Equal(norm(a), norm(b))
}

func channelError(code int, format string, args ...any) *amqp.Error {
	return &amqp.Error{Code: code, Reason: fmt.Sprintf(format, args...), Server: true}
}

func msDuration(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }
