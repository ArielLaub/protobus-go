package protobus

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// Service is the interface that all services must implement.
type Service interface {
	ServiceName() string
	ProtoFileName() string
}

// MethodHandler is the signature for RPC method handlers.
// Parameters: ctx, data map, actor string, correlationID string
// Returns: result map, error
type MethodHandler func(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error)

// ServiceOptions configures a service.
type ServiceOptions struct {
	MaxConcurrent int
	RetryOptions  *RetryOptions
}

// BaseService provides common service functionality.
type BaseService struct {
	mu            sync.RWMutex
	ctx           *Context
	listener      *BaseListener
	eventListener *BaseListener
	handlers      map[string]MethodHandler
	// streamHandlers registers server-streaming methods. A method may live
	// in either `handlers` (unary) or `streamHandlers` (streaming) — never
	// both. The framework dispatches onMessage based on which map contains
	// the requested method name. See docs/advanced/streaming.md.
	streamHandlers map[string]StreamingHandler
	serviceName    string
	protoFileName  string
	options        *ServiceOptions
	initialized    bool
}

// NewBaseService creates a new BaseService.
func NewBaseService(ctx *Context, serviceName, protoFileName string, options *ServiceOptions) *BaseService {
	if options == nil {
		options = &ServiceOptions{}
	}

	lateAck := options.MaxConcurrent > 0

	return &BaseService{
		ctx:            ctx,
		serviceName:    serviceName,
		protoFileName:  protoFileName,
		options:        options,
		handlers:       make(map[string]MethodHandler),
		streamHandlers: make(map[string]StreamingHandler),
		listener:       NewBaseListener(ctx.Connection(), lateAck, options.MaxConcurrent, options.RetryOptions),
		eventListener:  NewBaseListener(ctx.Connection(), false, 0, nil),
	}
}

// ServiceName returns the service name.
func (s *BaseService) ServiceName() string {
	return s.serviceName
}

// ProtoFileName returns the proto file name.
func (s *BaseService) ProtoFileName() string {
	return s.protoFileName
}

// Handle registers a unary method handler.
func (s *BaseService) Handle(method string, handler MethodHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[strings.ToLower(method)] = handler
}

// HandleStream registers a server-streaming method handler. The handler
// produces zero or more response chunks via the `send` callback, then
// returns. Returning an error terminates the stream with that error as
// the terminal chunk's payload.
//
// See docs/advanced/streaming.md for the full contract and examples.
func (s *BaseService) HandleStream(method string, handler StreamingHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streamHandlers[strings.ToLower(method)] = handler
}

// RegisterHandlers uses reflection to auto-discover and register handlers.
// It looks for methods with signature:
// func(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error)
func (s *BaseService) RegisterHandlers(service interface{}) {
	val := reflect.ValueOf(service)
	typ := val.Type()

	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)

		// Check signature
		if !isValidHandler(method.Type) {
			continue
		}

		// Get method name (lowercase first char for consistency)
		name := method.Name
		if len(name) > 0 {
			name = strings.ToLower(name[:1]) + name[1:]
		}

		// Create wrapper
		methodVal := val.Method(i)
		handler := func(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
			args := []reflect.Value{
				reflect.ValueOf(ctx),
				reflect.ValueOf(data),
				reflect.ValueOf(actor),
				reflect.ValueOf(correlationID),
			}
			results := methodVal.Call(args)

			var result map[string]interface{}
			var err error

			if !results[0].IsNil() {
				result = results[0].Interface().(map[string]interface{})
			}
			if !results[1].IsNil() {
				err = results[1].Interface().(error)
			}

			return result, err
		}

		s.Handle(name, handler)
		logDebug("Registered handler: %s.%s", s.serviceName, name)
	}
}

func isValidHandler(t reflect.Type) bool {
	// Must have 5 inputs: receiver, ctx, data, actor, correlationID
	if t.NumIn() != 5 {
		return false
	}

	// Check parameter types
	if t.In(1) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return false
	}
	if t.In(2) != reflect.TypeOf(map[string]interface{}{}) {
		return false
	}
	if t.In(3) != reflect.TypeOf("") || t.In(4) != reflect.TypeOf("") {
		return false
	}

	// Must have 2 outputs: map[string]interface{}, error
	if t.NumOut() != 2 {
		return false
	}
	if t.Out(0) != reflect.TypeOf(map[string]interface{}{}) {
		return false
	}
	if !t.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return false
	}

	return true
}

// Init initializes the service.
func (s *BaseService) Init() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.initialized {
		return ErrAlreadyInitialized
	}

	// Initialize message listener
	if err := s.listener.Init(s.onMessage, s.serviceName, s.ctx.ExchangeName()); err != nil {
		return fmt.Errorf("failed to init listener: %w", err)
	}

	// Initialize event listener
	eventsQueue := s.serviceName + ".Events"
	if err := s.eventListener.Init(s.onEvent, eventsQueue, s.ctx.EventsExchangeName()); err != nil {
		return fmt.Errorf("failed to init event listener: %w", err)
	}

	// Subscribe to requests
	pattern := fmt.Sprintf("REQUEST.%s.*", s.serviceName)
	if err := s.listener.Subscribe(pattern); err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Start listeners
	if err := s.listener.Start(); err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}
	if err := s.eventListener.Start(); err != nil {
		return fmt.Errorf("failed to start event listener: %w", err)
	}

	s.initialized = true
	logInfo("Service %s initialized", s.serviceName)
	return nil
}

func (s *BaseService) onMessage(ctx context.Context, body []byte, correlationID string) ([]byte, error) {
	request, err := s.ctx.Factory().DecodeRequest(body)
	if err != nil {
		return nil, fmt.Errorf("failed to decode request: %w", err)
	}

	// Extract method name (last part of method path)
	parts := strings.Split(request.Method, ".")
	methodName := strings.ToLower(parts[len(parts)-1])

	logDebug("Received request %s (%s)", request.Method, correlationID)

	s.mu.RLock()
	streamHandler, isStream := s.streamHandlers[methodName]
	handler, isUnary := s.handlers[methodName]
	s.mu.RUnlock()

	// Streaming path. Sentinel response signals the listener to consume
	// chunks via the per-call stream sink installed on the listener.
	if isStream {
		return s.runStream(ctx, request, correlationID, streamHandler)
	}

	if !isUnary {
		return nil, fmt.Errorf("%w: %s", ErrInvalidMethod, methodName)
	}

	result, err := handler(ctx, request.Data, request.Actor, correlationID)
	if err != nil {
		logError("Handler error: %v", err)
		return s.ctx.Factory().BuildResponse(request.Method, nil, err)
	}

	logDebug("Sending result for %s", request.Method)
	return s.ctx.Factory().BuildResponse(request.Method, result, nil)
}

// runStream drives a server-streaming handler. The handler emits chunks via
// `send`, which the framework publishes individually as separate AMQP
// messages on the reply queue with x-protobus-final / x-protobus-seq headers.
//
// Returning nil from this function (with no reply-bytes) signals onMessage's
// caller to skip the unary reply path — we've already published everything
// ourselves.
func (s *BaseService) runStream(
	ctx context.Context,
	request *RequestContainer,
	correlationID string,
	handler StreamingHandler,
) ([]byte, error) {
	// The listener stashes the reply destination + publisher on ctx before
	// dispatching. For streaming we publish many chunks to that one queue.
	sink := streamSinkFromContext(ctx)
	if sink == nil || sink.ReplyTo == "" {
		// Request had no ReplyTo (one-way) — drain the handler for any
		// side-effects but don't try to publish.
		_ = handler(ctx, request.Data, request.Actor, correlationID,
			func(map[string]interface{}) error { return nil })
		return nil, nil
	}
	replyTo := sink.ReplyTo
	sender := sink.Publish

	// Look-ahead-by-one so we can mark the last chunk with x-protobus-final=true
	// without an extra empty terminal message.
	var (
		seq      uint32
		buffered map[string]interface{}
		mu       sync.Mutex
	)

	send := func(chunk map[string]interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		if buffered != nil {
			body, err := s.ctx.Factory().BuildResponse(request.Method, buffered, nil)
			if err != nil {
				return err
			}
			sender(replyTo, correlationID, body, seq, false)
			seq++
		}
		buffered = chunk
		return nil
	}

	handlerErr := handler(ctx, request.Data, request.Actor, correlationID, send)

	mu.Lock()
	defer mu.Unlock()

	if handlerErr != nil {
		// Terminal error: flush any buffered chunk as non-final, then send
		// the error as the final terminal.
		if buffered != nil {
			body, err := s.ctx.Factory().BuildResponse(request.Method, buffered, nil)
			if err == nil {
				sender(replyTo, correlationID, body, seq, false)
				seq++
			}
		}
		body, _ := s.ctx.Factory().BuildResponse(request.Method, nil, handlerErr)
		sender(replyTo, correlationID, body, seq, true)
	} else if buffered != nil {
		// Normal completion — last buffered chunk becomes final.
		body, err := s.ctx.Factory().BuildResponse(request.Method, buffered, nil)
		if err != nil {
			return nil, err
		}
		sender(replyTo, correlationID, body, seq, true)
	} else {
		// Empty stream — single terminal with an empty body so the client
		// iterator ends cleanly without yielding a spurious chunk. Matches
		// the Python and TS ports.
		sender(replyTo, correlationID, []byte{}, 0, true)
	}

	// Returning (nil, nil) tells the listener not to publish a unary reply.
	return nil, nil
}

func (s *BaseService) onEvent(ctx context.Context, body []byte, correlationID string) ([]byte, error) {
	// Events don't need responses
	event, err := s.ctx.Factory().DecodeEvent(body)
	if err != nil {
		logWarn("Failed to decode event: %v", err)
		return nil, nil
	}

	logDebug("Received event: %s", event.Type)
	// Event handling would be implemented by specific services
	return nil, nil
}

// PublishEvent publishes an event.
func (s *BaseService) PublishEvent(ctx context.Context, eventType string, data map[string]interface{}, topic string) error {
	return s.ctx.PublishEvent(ctx, eventType, data, topic)
}

// SubscribeEvent subscribes to events (would need event handler registration).
func (s *BaseService) SubscribeEvent(pattern string) error {
	return s.eventListener.Subscribe(pattern)
}

// Stop stops the service.
func (s *BaseService) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.listener.Stop(); err != nil {
		logWarn("Failed to stop listener: %v", err)
	}
	if err := s.eventListener.Stop(); err != nil {
		logWarn("Failed to stop event listener: %v", err)
	}

	s.initialized = false
	logInfo("Service %s stopped", s.serviceName)
	return nil
}

// Context returns the service's context.
func (s *BaseService) Context() *Context {
	return s.ctx
}
