package protobus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ArielLaub/protobus-go/v2/internal/transport"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
)

// RetryPolicy governs how a service retries requests whose handler failed
// with an unhandled error. A failed attempt is parked on <service>.Retry for
// Delay, then redelivered; after MaxRetries retries it goes to
// <service>.DLQ, and only then is the caller told it failed.
type RetryPolicy struct {
	// MaxRetries is the number of retries after the first attempt. 0
	// disables retrying and the dead-letter queue: a failure is answered and
	// the request dropped.
	MaxRetries int
	// Delay is the retry queue's message TTL. RabbitMQ fixes it when the
	// queue is first declared; changing it later fails startup with
	// ErrRetryQueueMismatch until the queue is deleted.
	Delay time.Duration
	// MessageTTL, when set, expires requests waiting in the service queue.
	MessageTTL time.Duration
}

// DefaultRetryPolicy is the retry policy of the other ports: three retries,
// five seconds apart.
func DefaultRetryPolicy() RetryPolicy { return RetryPolicy{MaxRetries: 3, Delay: 5 * time.Second} }

// EventRetryPolicy governs retries of event handlers (see WithEventRetry).
type EventRetryPolicy struct {
	// MaxRetries is the number of retries after the first attempt; 0 (the
	// default) disables event retries.
	MaxRetries int
	// Delay is the event retry queue's message TTL; see RetryPolicy.Delay.
	Delay time.Duration
}

type serviceOptions struct {
	instance          string
	maxConcurrent     int
	retry             RetryPolicy
	earlyAck          bool
	processingTimeout time.Duration
	maxPriority       *uint8
	eventRetry        EventRetryPolicy
	eventConcurrency  int
	unaryInterceptors []UnaryServerInterceptor
	streamInterceptors []StreamServerInterceptor
}

func serviceOpt(f func(*serviceOptions)) ServiceOption { return serviceOption{option{service: f}} }

// WithMaxConcurrent sets how many requests the service handles at once: the
// consumer's prefetch, each delivery on its own goroutine. Default 1.
func WithMaxConcurrent(n int) ServiceOption {
	return serviceOpt(func(o *serviceOptions) { o.maxConcurrent = n })
}

// WithRetry replaces DefaultRetryPolicy. RetryPolicy{} disables retries and
// the dead-letter queue.
func WithRetry(p RetryPolicy) ServiceOption { return serviceOpt(func(o *serviceOptions) { o.retry = p }) }

// WithEarlyAck acknowledges each request on arrival instead of after its
// reply: at-most-once delivery, with no retries and no dead-letter queue. A
// failure is still reported to the caller. Under early ack the broker applies
// no prefetch, so WithMaxConcurrent bounds concurrency in-process instead.
func WithEarlyAck() ServiceOption { return serviceOpt(func(o *serviceOptions) { o.earlyAck = true }) }

// WithProcessingTimeout replaces Config.ProcessingTimeout for this service's
// unary methods. Streaming methods are bounded by their caller's idle timeout
// and cancellation instead.
func WithProcessingTimeout(d time.Duration) ServiceOption {
	return serviceOpt(func(o *serviceOptions) { o.processingTimeout = d })
}

// WithMaxPriority declares the service queue as a RabbitMQ priority queue with
// x-max-priority n (1..255; RecommendedMaxPriority is the sensible choice).
// It cannot be combined with WithEarlyAck: without a prefetch the broker
// hands the whole backlog to the consumer and leaves nothing to reorder.
//
// RabbitMQ fixes a queue's arguments at declaration, so adding or changing
// this on an existing queue fails with PRECONDITION_FAILED until an operator
// deletes the queue.
func WithMaxPriority(n uint8) ServiceOption {
	return serviceOpt(func(o *serviceOptions) { o.maxPriority = &n })
}

// ErrUnknownService reports a ServiceDesc whose service is not in the file
// registry, so there is no contract to validate requests against.
var ErrUnknownService = errors.New("protobus: unknown service")

// Service serves one protobus service on a Bus.
type Service struct {
	bus      *Bus
	desc     *ServiceDesc
	impl     any
	contract protoreflect.ServiceDescriptor
	name     string // runtime name: the queue, and the REQUEST.<name>.* binding
	opts     serviceOptions

	unary    map[string]MethodDesc
	streams  map[string]StreamDesc
	unaryIC  UnaryServerInterceptor
	streamIC StreamServerInterceptor

	requests *consumer

	mu      sync.Mutex
	events  *EventListener
	started bool
}

// Register prepares a service implementation for serving. Nothing touches the
// broker until Start.
//
// Generated code calls it from Register<Service>Server; it can be used
// directly with a hand-written ServiceDesc.
func (b *Bus) Register(desc *ServiceDesc, impl any, opts ...ServiceOption) (*Service, error) {
	o := serviceOptions{maxConcurrent: 1, retry: DefaultRetryPolicy(), eventConcurrency: b.cfg.DefaultPrefetch}
	for _, opt := range opts {
		opt.applyService(&o)
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	if impl == nil {
		return nil, errors.New("protobus: Register: nil implementation")
	}
	if desc.HandlerType != nil {
		want := reflect.TypeOf(desc.HandlerType).Elem()
		if got := reflect.TypeOf(impl); !got.Implements(want) {
			return nil, fmt.Errorf("protobus: Register: %v does not implement %v", got, want)
		}
	}
	d, err := b.files.FindDescriptorByName(protoreflect.FullName(desc.ServiceName))
	if err != nil {
		return nil, fmt.Errorf("%w %q: %v", ErrUnknownService, desc.ServiceName, err)
	}
	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("%w: %q is a %T, not a service", ErrUnknownService, desc.ServiceName, d)
	}

	s := &Service{
		bus: b, desc: desc, impl: impl, contract: sd, name: desc.ServiceName, opts: o,
		unary: map[string]MethodDesc{}, streams: map[string]StreamDesc{},
		unaryIC: chainUnary(o.unaryInterceptors), streamIC: chainStream(o.streamInterceptors),
	}
	if o.instance != "" {
		s.name += "." + o.instance
	}
	for _, m := range desc.Methods {
		md := sd.Methods().ByName(protoreflect.Name(m.MethodName))
		switch {
		case md == nil:
			return nil, fmt.Errorf("protobus: Register: %s declares no method %q", desc.ServiceName, m.MethodName)
		case md.IsStreamingServer() || md.IsStreamingClient():
			return nil, fmt.Errorf("protobus: Register: %s.%s is streaming but registered as unary", desc.ServiceName, m.MethodName)
		}
		s.unary[m.MethodName] = m
	}
	for _, st := range desc.Streams {
		md := sd.Methods().ByName(protoreflect.Name(st.MethodName))
		switch {
		case md == nil:
			return nil, fmt.Errorf("protobus: Register: %s declares no method %q", desc.ServiceName, st.MethodName)
		case !md.IsStreamingServer() || md.IsStreamingClient():
			return nil, fmt.Errorf("protobus: Register: %s.%s is not server-streaming", desc.ServiceName, st.MethodName)
		}
		s.streams[st.MethodName] = st
	}
	s.requests = newConsumer(b, s.requestSpec())
	return s, nil
}

func (o *serviceOptions) validate() error {
	if o.maxConcurrent < 1 || o.maxConcurrent > 65535 {
		return fmt.Errorf("protobus: WithMaxConcurrent must be within 1..65535, got %d", o.maxConcurrent)
	}
	if o.eventConcurrency < 1 || o.eventConcurrency > 65535 {
		return fmt.Errorf("protobus: WithEventConcurrency must be within 1..65535, got %d", o.eventConcurrency)
	}
	if o.retry.MaxRetries < 0 || o.retry.MaxRetries > 0 && o.retry.Delay <= 0 || o.retry.MessageTTL < 0 {
		return fmt.Errorf("protobus: invalid RetryPolicy %+v", o.retry)
	}
	if o.eventRetry.MaxRetries < 0 || o.eventRetry.MaxRetries > 0 && o.eventRetry.Delay <= 0 {
		return fmt.Errorf("protobus: invalid EventRetryPolicy %+v", o.eventRetry)
	}
	if o.processingTimeout < 0 {
		return fmt.Errorf("protobus: negative processing timeout %v", o.processingTimeout)
	}
	if err := validInstance(o.instance); err != nil {
		return err
	}
	if p := o.maxPriority; p != nil {
		if *p < 1 {
			return fmt.Errorf("%w: WithMaxPriority must be within 1..255, got 0; %d is recommended",
				ErrInvalidPriority, RecommendedMaxPriority)
		}
		if o.earlyAck {
			return fmt.Errorf("%w: WithMaxPriority requires late ack: under early ack the broker applies no "+
				"prefetch and hands over the whole backlog, leaving priority nothing to reorder", ErrInvalidPriority)
		}
	}
	return nil
}

// Name is the name the service runs under: its contract name, plus the
// instance name if it has one.
func (s *Service) Name() string { return s.name }

// Contract is the service's fully-qualified name in its .proto.
func (s *Service) Contract() string { return string(s.contract.FullName()) }

func (s *Service) requestSpec() consumerSpec {
	cfg := s.bus.cfg
	args := amqp.Table{}
	if s.opts.retry.MessageTTL > 0 {
		args["x-message-ttl"] = intHeader(s.opts.retry.MessageTTL.Milliseconds())
	}
	if p := s.opts.maxPriority; p != nil {
		args["x-max-priority"] = int32(*p)
	}
	timeout := cfg.ProcessingTimeout
	if s.opts.processingTimeout > 0 {
		timeout = s.opts.processingTimeout
	}
	spec := consumerSpec{
		queue:        s.name,
		queueArgs:    args,
		exchange:     cfg.BusExchange,
		bindings:     func() []string { return []string{"REQUEST." + s.name + ".*"} },
		lateAck:      !s.opts.earlyAck,
		prefetch:     s.opts.maxConcurrent,
		timeout:      timeout,
		timeoutReply: s.timeoutReply,
		handle:       s.handle,
		describe:     s.name,
	}
	if s.opts.earlyAck {
		spec.concurrency = s.opts.maxConcurrent
	}
	if r := s.opts.retry; r.MaxRetries > 0 && !s.opts.earlyAck {
		rs := &retrySpec{
			maxRetries: r.MaxRetries,
			exchange:   s.name + ".Retry.Exchange",
			queue:      s.name + ".Retry",
			dlq:        s.name + ".DLQ",
		}
		spec.retry = rs
		spec.declare = func(ch transport.Channel, _ string) error {
			return declareRetryTopology(ch, rs, r.Delay, cfg.BusExchange)
		}
	}
	return spec
}

// declareRetryTopology declares the dead-letter queue, the retry queue (whose
// TTL dead-letters back to deadLetterTo under the message's own routing key)
// and the retry exchange that preserves that key on the way in.
func declareRetryTopology(ch transport.Channel, rs *retrySpec, delay time.Duration, deadLetterTo string) error {
	if _, err := ch.QueueDeclare(rs.dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring %s: %w", rs.dlq, err)
	}
	_, err := ch.QueueDeclare(rs.queue, true, false, false, false, amqp.Table{
		"x-message-ttl":          intHeader(delay.Milliseconds()),
		"x-dead-letter-exchange": deadLetterTo,
	})
	if err != nil {
		var ae *amqp.Error
		if errors.As(err, &ae) && ae.Code == amqp.PreconditionFailed {
			return fmt.Errorf("%w: %s already exists with other arguments (most likely a different retry delay, "+
				"now %v). RabbitMQ cannot change a queue's x-message-ttl in place: drain and delete the queue, or "+
				"keep the original delay: %v", ErrRetryQueueMismatch, rs.queue, delay, err)
		}
		return fmt.Errorf("declaring %s: %w", rs.queue, err)
	}
	if err := ch.ExchangeDeclare(rs.exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring %s: %w", rs.exchange, err)
	}
	if err := ch.QueueBind(rs.queue, "#", rs.exchange, false, nil); err != nil {
		return fmt.Errorf("binding %s: %w", rs.queue, err)
	}
	return nil
}

// Start declares the service's topology and begins serving requests and
// event subscriptions. It waits for the connection if a reconnection is under
// way.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("protobus: service %s already started", s.name)
	}
	s.started = true
	events := s.events
	s.mu.Unlock()

	if err := s.requests.start(ctx); err != nil {
		return fmt.Errorf("protobus: starting %s: %w", s.name, err)
	}
	if events != nil {
		if err := events.Start(ctx); err != nil {
			return err
		}
	}
	if err := s.bus.ensureCancelListener(ctx); err != nil {
		return err
	}
	s.bus.mu.Lock()
	s.bus.services = append(s.bus.services, s)
	s.bus.mu.Unlock()
	s.bus.log.LogAttrs(ctx, slog.LevelInfo, "service ready", attrOperation("start"), attrService(s.name))
	return nil
}

// StopConsuming stops taking new requests and events, leaving channels open so
// work in hand can finish and settle. It is the first step of a graceful
// shutdown (Bus.Shutdown and Run do it for you), and it is final: a
// reconnection does not resume consumption.
func (s *Service) StopConsuming(context.Context) error {
	s.requests.stopConsuming()
	s.mu.Lock()
	events := s.events
	s.mu.Unlock()
	if events != nil {
		events.consumer.stopConsuming()
	}
	return nil
}

// Events returns the service's event listener, consuming the durable queue
// "<service>.Events". Subscribe to it before or after Start.
func (s *Service) Events() *EventListener {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.events == nil {
		s.events = s.bus.newEventListener(s.name+".Events", s.opts.eventConcurrency, s.opts.eventRetry)
		if s.started {
			// Subscriptions after Start start the listener on first use.
			s.events.lazy = true
		}
	}
	return s.events
}

// ---- request handling ----------------------------------------------------------

// payloadError marks a request payload that did not decode.
type payloadError struct{ err error }

func (e *payloadError) Error() string { return "payload did not decode: " + e.err.Error() }
func (e *payloadError) Unwrap() error { return e.err }

func (s *Service) encodeError(method string, err error) []byte {
	ce := sanitizeForCaller(err, s.bus.cfg.ExposeInternalErrors)
	body, _ := wire.AppendResponse(nil, wire.Response{Error: &wire.Error{Method: method, Message: ce.Message, Code: ce.Code}})
	return body
}

// rejection answers a request the service will not run. It is a handled
// PROTOCOL_ERROR: the same bytes would fail the same way on every
// redelivery, so retrying buys nothing.
func (s *Service) rejection(label, reason string) handlerResult {
	s.bus.log.LogAttrs(context.Background(), slog.LevelError, "rejected request", attrOperation("dispatch"),
		attrService(s.name), attrMethod(label), slog.String("reason", reason), attrOutcome(outcomeRejected))
	return handlerResult{reply: s.encodeError(label, newProtocolError(reason))}
}

func (s *Service) timeoutReply(d *amqp.Delivery, err error) []byte {
	return s.encodeError(s.Contract()+"."+lastSegment(d.RoutingKey), err)
}

func (s *Service) handle(ctx context.Context, d *amqp.Delivery, ctl *deliveryControl) handlerResult {
	contract := s.Contract()

	// The envelope first, the payload later. The envelope names the method,
	// and the method chooses the schema the payload is read with, so it must
	// be checked against this service's contract before the bytes are
	// interpreted.
	env, err := wire.DecodeRequest(d.Body)
	if err != nil {
		label := d.RoutingKey
		if label == "" {
			label = "unknown"
		}
		return s.rejection(label, "request envelope did not decode")
	}

	// Rejections are labelled with the method the ROUTING KEY names: the
	// body's method is exactly what is in dispute.
	keyMethod := lastSegment(d.RoutingKey)
	if d.RoutingKey == "" {
		keyMethod = lastSegment(env.Method)
	}
	label := contract + "." + keyMethod

	// Dispatch is bound to the routing key the broker delivered on, not to
	// the publisher-controlled body. Otherwise a client allowed to publish to
	// one method could have another executed, and RabbitMQ topic permissions
	// would mean nothing.
	if d.RoutingKey != "" {
		if !strings.HasPrefix(d.RoutingKey, "REQUEST."+s.name+".") {
			return s.rejection(label, fmt.Sprintf("routing key %s does not belong to service %s", d.RoutingKey, s.name))
		}
		if keyMethod != lastSegment(env.Method) {
			return s.rejection(label, fmt.Sprintf("request method %s contradicts routing key %s", env.Method, d.RoutingKey))
		}
	}
	// The body must name a method of THIS contract, in full: no extra
	// segments, no other service whose schema happens to be loaded.
	service, method, ok := splitMethodName(env.Method)
	if !ok {
		return s.rejection(label, fmt.Sprintf("request method %s is not a qualified method name", env.Method))
	}
	if service != contract {
		return s.rejection(label, fmt.Sprintf("request method %s is not a method of %s", env.Method, contract))
	}
	md := s.contract.Methods().ByName(protoreflect.Name(method))
	if md == nil {
		return s.rejection(label, fmt.Sprintf("%s declares no method %s", contract, method))
	}

	info := &CallInfo{
		Method: env.Method, Actor: env.Actor, CorrelationID: d.CorrelationId, MessageID: d.MessageId,
		RoutingKey: d.RoutingKey, Redelivered: d.Redelivered, Attempt: retryCount(d.Headers), Headers: copyHeaders(d.Headers),
	}
	ctx = withCallInfo(ctx, info)
	dec := func(m proto.Message) error {
		if err := proto.Unmarshal(env.Data, m); err != nil {
			return &payloadError{err}
		}
		return nil
	}

	if md.IsStreamingServer() {
		st, ok := s.streams[method]
		if !ok {
			return s.rejection(env.Method, "invalid service method "+method)
		}
		ctl.disarmTimeout()
		return s.serveStream(ctx, d, ctl, env.Method, st, dec)
	}
	m, ok := s.unary[method]
	if !ok {
		return s.rejection(env.Method, "invalid service method "+method)
	}
	resp, err := m.Handler(s.impl, ctx, dec, s.unaryIC)
	if err != nil {
		return s.failure(ctx, env.Method, err)
	}
	var data []byte
	if resp != nil {
		if data, err = proto.Marshal(resp); err != nil {
			return s.failure(ctx, env.Method, fmt.Errorf("encoding the response of %s: %w", env.Method, err))
		}
	}
	body, _ := wire.AppendResponse(nil, wire.Response{Result: &wire.Result{Method: env.Method, Data: data}})
	return handlerResult{reply: body}
}

// failure classifies a handler error. Malformed payloads and unimplemented
// methods are protocol errors and handled errors are answered at once; both
// are acknowledged, never retried. Anything else is an infrastructure failure:
// it is logged in full here and goes through the retry ladder, carrying the
// sanitised reply the caller gets once the ladder ends.
func (s *Service) failure(ctx context.Context, method string, err error) handlerResult {
	var pe *payloadError
	switch {
	case errors.As(err, &pe):
		return s.rejection(method, "payload did not decode as the request type of "+method)
	case errors.Is(err, ErrUnimplemented):
		return s.rejection(method, "invalid service method "+lastSegment(method))
	}
	if h, ok := AsHandled(err); ok {
		s.bus.log.LogAttrs(ctx, slog.LevelInfo, "handled error", attrOperation("handle"), attrService(s.name),
			attrMethod(method), slog.String("code", h.Code))
		return handlerResult{reply: s.encodeError(method, err)}
	}
	ci, _ := CallInfoFromContext(ctx)
	s.bus.log.LogAttrs(ctx, slog.LevelError, "handler failed", attrOperation("handle"), attrService(s.name),
		attrMethod(method), attrCorrelationID(ci.CorrelationID), attrError(err))
	return handlerResult{err: err, errReply: s.encodeError(method, err)}
}
