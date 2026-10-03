package protobus

import (
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

// Option configures a Bus.
type Option func(*busOptions)

type busOptions struct {
	cfg      *Config
	log      *slog.Logger
	observe  func(ConnectionEvent)
	connName string
	files    *protoregistry.Files
	types    *protoregistry.Types
	dialer   transport.Dialer
}

// WithConfig replaces the configuration, which otherwise comes from
// ConfigFromEnv.
func WithConfig(c Config) Option { return func(o *busOptions) { o.cfg = &c } }

// WithLogger sets the logger. By default protobus logs to stderr at the level
// named by LOG_LEVEL.
func WithLogger(l *slog.Logger) Option { return func(o *busOptions) { o.log = l } }

// WithConnectionObserver receives connection state changes, in order, on the
// goroutine supervising the connection. It must not block.
func WithConnectionObserver(f func(ConnectionEvent)) Option {
	return func(o *busOptions) { o.observe = f }
}

// WithConnectionName labels the connection in the RabbitMQ management UI.
func WithConnectionName(name string) Option { return func(o *busOptions) { o.connName = name } }

// WithRegistry resolves services and message types from files and types
// instead of the global registries generated code populates. Use it with
// schemas loaded at runtime (see the protoload package).
func WithRegistry(files *protoregistry.Files, types *protoregistry.Types) Option {
	return func(o *busOptions) { o.files, o.types = files, types }
}

// withDialer substitutes the transport, for tests.
func withDialer(d transport.Dialer) Option { return func(o *busOptions) { o.dialer = d } }

// ---- call options -------------------------------------------------------------

// CallOption configures one unary call. Options that only make sense for
// unary calls (priority, message id, fire-and-forget) are CallOptions only,
// so passing one to a streaming call does not compile.
type CallOption interface{ applyCall(*callOptions) }

// StreamOption configures one streaming call.
type StreamOption interface{ applyStream(*streamOptions) }

// CallStreamOption is accepted by both kinds of call.
type CallStreamOption interface {
	CallOption
	StreamOption
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

type callOptionFunc func(*callOptions)

func (f callOptionFunc) applyCall(o *callOptions) { f(o) }

type bothOption struct {
	call   func(*callOptions)
	stream func(*streamOptions)
}

func (b bothOption) applyCall(o *callOptions)     { b.call(o) }
func (b bothOption) applyStream(o *streamOptions) { b.stream(o) }

type streamOptionFunc func(*streamOptions)

func (f streamOptionFunc) applyStream(o *streamOptions) { f(o) }

// WithActor records who the call is made on behalf of. It travels in the
// request envelope for tracing and auditing; nothing authenticates it.
func WithActor(actor string) CallStreamOption {
	return bothOption{
		call:   func(o *callOptions) { o.actor = actor },
		stream: func(o *streamOptions) { o.actor = actor },
	}
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
	return callOptionFunc(func(o *callOptions) { o.priority = &p })
}

// WithMessageID sets the message's identity, as the service sees it in
// CallInfo.MessageID. It is carried unchanged across redeliveries and retry
// and dead-letter hops.
//
// Set it so a republish after an AMBIGUOUS failure (see PublishError) is
// recognisable as the same logical message: derive it from the work (an order
// id), never from a clock or counter. It must be non-blank and at most 255
// bytes, the AMQP limit.
func WithMessageID(id string) CallOption {
	return callOptionFunc(func(o *callOptions) { o.messageID = &id })
}

// WithTimeout bounds the call, covering the wait for the broker's confirm as
// well as for the reply. The context's own deadline still applies when it is
// sooner. Without either, Config.RPCTimeout bounds the call.
func WithTimeout(d time.Duration) CallOption {
	return callOptionFunc(func(o *callOptions) { o.timeout = d })
}

// NoReply publishes the request without waiting for, or asking for, a reply.
// The call returns once the broker confirms the request. The service still
// runs the method; its result is discarded.
func NoReply() CallOption { return callOptionFunc(func(o *callOptions) { o.noReply = true }) }

// WithIdleTimeout bounds the gap between stream chunks, defaulting to
// Config.StreamIdleTimeout. The stream as a whole has no deadline unless its
// context sets one.
func WithIdleTimeout(d time.Duration) StreamOption {
	return streamOptionFunc(func(o *streamOptions) { o.idleTimeout = d })
}

// maxMessageIDBytes is the AMQP shortstr limit on message-id.
const maxMessageIDBytes = 255

func (o *callOptions) validate() error {
	if id := o.messageID; id != nil {
		// Blank is refused, not treated as unset: an id derived from a field
		// that came out empty would silently become a fresh id per attempt,
		// which is exactly the duplication this option exists to prevent.
		if !utf8.ValidString(*id) || isBlank(*id) {
			return fmt.Errorf("%w: must be a non-blank string; leave it unset to have one generated", ErrInvalidMessageID)
		}
		if n := len(*id); n > maxMessageIDBytes {
			return fmt.Errorf("%w: %d bytes, AMQP allows at most %d; hash a long key rather than concatenating it",
				ErrInvalidMessageID, n, maxMessageIDBytes)
		}
	}
	if o.timeout < 0 {
		return fmt.Errorf("protobus: negative call timeout %v", o.timeout)
	}
	return nil
}

func isBlank(s string) bool {
	for _, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r', '\v', '\f':
		default:
			return false
		}
	}
	return true
}
