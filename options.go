package protobus

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

// Options.
//
// Every option type is an interface with an unexported method, so one
// constructor can configure several kinds of thing (WithInstance names both
// a service and the client addressing it) and new options can be added
// without breaking anyone.

// DialOption configures a Bus.
type DialOption interface{ applyDial(*dialOptions) }

// CallOption configures one unary call. Options that only make sense for a
// unary call (priority, fire-and-forget) do not satisfy StreamOption, so
// passing one to a streaming call does not compile.
type CallOption interface{ applyCall(*callOptions) }

// StreamOption configures one streaming call.
type StreamOption interface{ applyStream(*streamOptions) }

// ClientOption configures a Client.
type ClientOption interface{ applyClient(*clientOptions) }

// ServiceOption configures a Service.
type ServiceOption interface{ applyService(*serviceOptions) }

// ListenerOption configures a standalone EventListener.
type ListenerOption interface{ applyListener(*listenerOptions) }

// PublishOption configures one event publish.
type PublishOption interface{ applyPublish(*publishOptions) }

// SubscribeOption configures one event subscription.
type SubscribeOption interface{ applySubscribe(*subscribeOptions) }

type dialOptions struct {
	cfg      *Config
	log      *slog.Logger
	observe  func(ConnectionEvent)
	connName string
	files    *protoregistry.Files
	types    *protoregistry.Types
	dialer   transport.Dialer
	requeue  time.Duration // test hook: delay before requeueing an unsettled message
}

type callOptions struct {
	actor     string
	priority  *uint8
	messageID *string
	noReply   bool
	timeout   time.Duration
}

type streamOptions struct {
	actor       string
	idleTimeout time.Duration
}

type clientOptions struct {
	instance string
}

type listenerOptions struct {
	concurrency int
	retry       EventRetryPolicy
}

type publishOptions struct {
	topic     string
	messageID *string
}

type subscribeOptions struct {
	topic string
}

// option implements whichever option interfaces it has a function for; the
// concrete types below restrict each constructor to the interfaces it is
// meaningful for.
type option struct {
	dial      func(*dialOptions)
	call      func(*callOptions)
	stream    func(*streamOptions)
	client    func(*clientOptions)
	service   func(*serviceOptions)
	listener  func(*listenerOptions)
	publish   func(*publishOptions)
	subscribe func(*subscribeOptions)
}

type (
	dialOption       struct{ o option }
	callOption       struct{ o option }
	streamOption     struct{ o option }
	serviceOption    struct{ o option }
	callStreamOption struct{ o option }
	instanceOption   struct{ o option }
	messageIDOption  struct{ o option }
	topicOption      struct{ o option }
	eventOption      struct{ o option }
)

func (x dialOption) applyDial(o *dialOptions)            { x.o.dial(o) }
func (x callOption) applyCall(o *callOptions)            { x.o.call(o) }
func (x streamOption) applyStream(o *streamOptions)      { x.o.stream(o) }
func (x serviceOption) applyService(o *serviceOptions)   { x.o.service(o) }
func (x callStreamOption) applyCall(o *callOptions)      { x.o.call(o) }
func (x callStreamOption) applyStream(o *streamOptions)  { x.o.stream(o) }
func (x instanceOption) applyService(o *serviceOptions)  { x.o.service(o) }
func (x instanceOption) applyClient(o *clientOptions)    { x.o.client(o) }
func (x messageIDOption) applyCall(o *callOptions)       { x.o.call(o) }
func (x messageIDOption) applyPublish(o *publishOptions) { x.o.publish(o) }
func (x topicOption) applyPublish(o *publishOptions)     { x.o.publish(o) }
func (x topicOption) applySubscribe(o *subscribeOptions) { x.o.subscribe(o) }
func (x eventOption) applyService(o *serviceOptions)     { x.o.service(o) }
func (x eventOption) applyListener(o *listenerOptions)   { x.o.listener(o) }

// CallStreamOption configures both unary and streaming calls.
type CallStreamOption interface {
	CallOption
	StreamOption
}

// InstanceOption names a service instance, for both the Service and the
// clients addressing it.
type InstanceOption interface {
	ServiceOption
	ClientOption
}

// MessageIDOption sets the message id of a call or of an event.
type MessageIDOption interface {
	CallOption
	PublishOption
}

// TopicOption sets the topic an event is published or subscribed under.
type TopicOption interface {
	PublishOption
	SubscribeOption
}

// EventOption configures event handling, on a Service or on a standalone
// EventListener.
type EventOption interface {
	ServiceOption
	ListenerOption
}

// ---- dial options -----------------------------------------------------------------

// WithConfig replaces the configuration, which otherwise comes from
// ConfigFromEnv: the environment variables every protobus port reads.
func WithConfig(c Config) DialOption {
	return dialOption{option{dial: func(o *dialOptions) { o.cfg = &c }}}
}

// WithLogger sets the logger. By default protobus logs through slog.Default,
// filtered by LOG_LEVEL when that is set.
func WithLogger(l *slog.Logger) DialOption {
	return dialOption{option{dial: func(o *dialOptions) { o.log = l }}}
}

// WithConnectionObserver receives connection state changes, in order, on the
// goroutine that supervises the connection. It must not block.
func WithConnectionObserver(f func(ConnectionEvent)) DialOption {
	return dialOption{option{dial: func(o *dialOptions) { o.observe = f }}}
}

// WithConnectionName labels the connection in the RabbitMQ management UI.
func WithConnectionName(name string) DialOption {
	return dialOption{option{dial: func(o *dialOptions) { o.connName = name }}}
}

// WithRegistry resolves services and message types from files and types
// instead of the global registries generated code populates. Use it with
// schemas loaded at runtime (see the protoload package).
func WithRegistry(files *protoregistry.Files, types *protoregistry.Types) DialOption {
	return dialOption{option{dial: func(o *dialOptions) { o.files, o.types = files, types }}}
}

// withDialer substitutes the transport (see protobustest).
func withDialer(d transport.Dialer) DialOption {
	return dialOption{option{dial: func(o *dialOptions) { o.dialer = d }}}
}

// withRequeueDelay shortens the pause before an unsettled message is
// requeued, for tests.
func withRequeueDelay(d time.Duration) DialOption {
	return dialOption{option{dial: func(o *dialOptions) { o.requeue = d }}}
}

// ---- call options -------------------------------------------------------------

// WithActor records who the call is made on behalf of. It travels in the
// request envelope for tracing and auditing; nothing authenticates it.
func WithActor(actor string) CallStreamOption {
	return callStreamOption{option{
		call:   func(o *callOptions) { o.actor = actor },
		stream: func(o *streamOptions) { o.actor = actor },
	}}
}

// WithPriority sets the AMQP message priority. It only reorders messages on a
// service queue declared with WithMaxPriority; RabbitMQ ignores it anywhere
// else, which is what lets a new publisher talk to an old consumer. A priority
// above the queue's maximum is clamped by the broker.
//
// amqp091 omits a zero priority from the wire, and RabbitMQ sorts a message
// with none as 0, so WithPriority(PriorityNormal) behaves exactly as the
// explicit 0 the TypeScript port sends.
func WithPriority(p uint8) CallOption {
	return callOption{option{call: func(o *callOptions) { o.priority = &p }}}
}

// WithMessageID sets a call's or an event's identity, as the receiver sees it
// in CallInfo.MessageID or EventInfo.MessageID. It is carried unchanged across
// redeliveries and retry and dead-letter hops.
//
// Set it so a republish after an AMBIGUOUS failure (see PublishError) is
// recognisable as the same logical message: derive it from the work (an order
// id), never from a clock or counter. It must be non-blank and at most 255
// bytes, the AMQP limit; anything else fails with ErrInvalidMessageID before
// anything is sent.
func WithMessageID(id string) MessageIDOption {
	return messageIDOption{option{
		call:    func(o *callOptions) { o.messageID = &id },
		publish: func(o *publishOptions) { o.messageID = &id },
	}}
}

// WithTimeout bounds the call, covering the wait for the broker's confirm as
// well as for the reply. The context's own deadline still applies when it is
// sooner. With neither, Config.RPCTimeout bounds the call.
func WithTimeout(d time.Duration) CallOption {
	return callOption{option{call: func(o *callOptions) { o.timeout = d }}}
}

// NoReply publishes the request without asking for a reply. The call returns
// once the broker has confirmed the request and routed it to a service queue
// (ErrUnroutable otherwise); the service runs the method and its result is
// discarded.
func NoReply() CallOption {
	return callOption{option{call: func(o *callOptions) { o.noReply = true }}}
}

// WithIdleTimeout bounds the gap between stream chunks, defaulting to
// Config.StreamIdleTimeout. The stream as a whole has no deadline unless its
// context sets one.
func WithIdleTimeout(d time.Duration) StreamOption {
	return streamOption{option{stream: func(o *streamOptions) { o.idleTimeout = d }}}
}

// ---- instance, topic and event options --------------------------------------------

// WithInstance names one instance of a service. On a Service, its queue and
// routing keys become "<service>.<instance>", so several instances of one
// contract run side by side; on a client, calls are routed to that instance.
// The envelope still names the contract method, which is what the instance
// validates requests against.
//
// The name must be one routing-key word: no '.', '*', '#' or whitespace.
func WithInstance(name string) InstanceOption {
	return instanceOption{option{
		service: func(o *serviceOptions) { o.instance = name },
		client:  func(o *clientOptions) { o.instance = name },
	}}
}

func validInstance(name string) error {
	if name != "" && strings.ContainsAny(name, ".*# \t\r\n") {
		return fmt.Errorf("protobus: instance name %q must be a single routing-key word", name)
	}
	return nil
}

// WithTopic publishes an event under topic instead of the default
// "EVENT.<type>", or subscribes to a topic pattern instead of it. Patterns
// follow AMQP topic rules: '*' matches one word, '#' zero or more.
func WithTopic(topic string) TopicOption {
	return topicOption{option{
		publish:   func(o *publishOptions) { o.topic = topic },
		subscribe: func(o *subscribeOptions) { o.topic = topic },
	}}
}

// WithEventConcurrency sets how many events are handled at once, each on its
// own goroutine. Default Config.DefaultPrefetch.
func WithEventConcurrency(n int) EventOption {
	return eventOption{option{
		service:  func(o *serviceOptions) { o.eventConcurrency = n },
		listener: func(o *listenerOptions) { o.concurrency = n },
	}}
}

// WithEventRetry enables retries for event handlers. A retried event comes
// back only to the listener that failed it (each has its own redelivery
// path), but there it re-runs every matching handler, including those that
// succeeded: make handlers idempotent, keyed on EventInfo.MessageID. After
// MaxRetries the event goes to "<queue>.DLQ", as does an event whose handler
// returns a HandledError or that does not decode. Off by default: a failing
// handler then drops its event, so one poisonous event cannot stall a
// subscriber.
//
// It needs a named queue; a private listener queue disappears with its
// connection.
func WithEventRetry(p EventRetryPolicy) EventOption {
	return eventOption{option{
		service:  func(o *serviceOptions) { o.eventRetry = p },
		listener: func(o *listenerOptions) { o.retry = p },
	}}
}

// ---- validation -------------------------------------------------------------------

// maxMessageIDBytes is the AMQP shortstr limit on message-id.
const maxMessageIDBytes = 255

func validateMessageID(id *string) error {
	if id == nil {
		return nil
	}
	// Blank is refused, not treated as unset: an id derived from a field that
	// came out empty would silently become a fresh id per attempt, which is
	// exactly the duplication this option exists to prevent.
	if !utf8.ValidString(*id) || strings.TrimSpace(*id) == "" {
		return fmt.Errorf("%w: must be a non-blank string; leave it unset to have one generated", ErrInvalidMessageID)
	}
	if n := len(*id); n > maxMessageIDBytes {
		return fmt.Errorf("%w: %d bytes, AMQP allows at most %d; hash a long key rather than concatenating it",
			ErrInvalidMessageID, n, maxMessageIDBytes)
	}
	return nil
}

func (o *callOptions) validate() error {
	if err := validateMessageID(o.messageID); err != nil {
		return err
	}
	if o.timeout < 0 {
		return fmt.Errorf("protobus: negative call timeout %v", o.timeout)
	}
	return nil
}
