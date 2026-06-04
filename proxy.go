package protobus

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ServiceProxy provides RPC client functionality.
type ServiceProxy struct {
	mu          sync.RWMutex
	ctx         *Context
	serviceName string
	channel     *amqp.Channel
	replyQueue  amqp.Queue
	pending     map[string]chan *ResponseContainer
	// pendingStreams holds chunk channels for in-flight server-streaming
	// RPCs. handleReplies pushes each chunk onto the right channel and
	// closes it when a message arrives with x-protobus-final=true.
	pendingStreams map[string]chan *ResponseContainer
	initialized    bool
	consumerTag    string
}

// NewServiceProxy creates a new ServiceProxy.
func NewServiceProxy(ctx *Context, serviceName string) *ServiceProxy {
	return &ServiceProxy{
		ctx:            ctx,
		serviceName:    serviceName,
		pending:        make(map[string]chan *ResponseContainer),
		pendingStreams: make(map[string]chan *ResponseContainer),
	}
}

// Init initializes the proxy.
func (p *ServiceProxy) Init() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.initialized {
		return ErrAlreadyInitialized
	}

	// Open channel
	ch, err := p.ctx.Connection().Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	p.channel = ch

	// Declare anonymous reply queue
	queue, err := p.ctx.Connection().DeclareQueue(ch, "", nil)
	if err != nil {
		return fmt.Errorf("failed to declare reply queue: %w", err)
	}
	p.replyQueue = queue

	// Start consuming replies
	p.consumerTag = fmt.Sprintf("proxy-%s-%d", p.serviceName, time.Now().UnixNano())
	deliveries, err := p.ctx.Connection().Consume(ch, queue.Name, p.consumerTag, true)
	if err != nil {
		return fmt.Errorf("failed to consume replies: %w", err)
	}

	go p.handleReplies(deliveries)

	p.initialized = true
	logDebug("ServiceProxy for %s initialized", p.serviceName)
	return nil
}

func (p *ServiceProxy) handleReplies(deliveries <-chan amqp.Delivery) {
	for delivery := range deliveries {
		// Streaming reply routing: distinguishable from unary because the
		// correlation_id was pre-registered in pendingStreams by OpenStream.
		// Multiple deliveries share one correlation_id; the framework closes
		// the channel when x-protobus-final=true arrives.
		p.mu.RLock()
		streamCh, isStream := p.pendingStreams[delivery.CorrelationId]
		unaryCh, isUnary := p.pending[delivery.CorrelationId]
		p.mu.RUnlock()

		if !isStream && !isUnary {
			logWarn("Received reply for unknown correlation ID: %s", delivery.CorrelationId)
			continue
		}

		// Empty-body terminals signal "end of stream, no payload" and can't
		// be decoded. Skip the decode in that case.
		var response *ResponseContainer
		if len(delivery.Body) > 0 {
			var derr error
			response, derr = p.ctx.Factory().DecodeResponse(delivery.Body)
			if derr != nil {
				logError("Failed to decode response: %v", derr)
				if !isStream {
					continue
				}
				// Streaming path falls through so we still honor the final
				// marker — losing one chunk is preferable to leaking the slot.
			}
		}

		if isStream {
			isFinal := parseFinalHeader(delivery.Headers)
			// Only push if we actually have a chunk to deliver. Empty-body
			// terminals just signal end-of-stream.
			if response != nil {
				select {
				case streamCh <- response:
				default:
					// Channel might be full or closed mid-iteration (caller
					// broke out). Drop the chunk silently.
				}
			}
			if isFinal {
				p.mu.Lock()
				if _, stillThere := p.pendingStreams[delivery.CorrelationId]; stillThere {
					delete(p.pendingStreams, delivery.CorrelationId)
					close(streamCh)
				}
				p.mu.Unlock()
			}
			continue
		}

		// Unary reply (existing behavior)
		unaryCh <- response
	}
}

// parseFinalHeader reads x-protobus-final from delivery headers tolerantly
// across the encodings amqp091-go may surface (bool, int, string).
func parseFinalHeader(headers amqp.Table) bool {
	if headers == nil {
		return false
	}
	v, ok := headers[HeaderProtobusFinal]
	if !ok || v == nil {
		return false
	}
	switch x := v.(type) {
	case bool:
		return x
	case int:
		return x != 0
	case int32:
		return x != 0
	case int64:
		return x != 0
	case string:
		return x == "true" || x == "1"
	}
	return false
}

// Call makes an RPC call to the service.
func (p *ServiceProxy) Call(ctx context.Context, method string, data map[string]interface{}, result interface{}) error {
	p.mu.RLock()
	if !p.initialized {
		p.mu.RUnlock()
		return ErrNotInitialized
	}
	ch := p.channel
	replyTo := p.replyQueue.Name
	p.mu.RUnlock()

	// Generate correlation ID
	correlationID := fmt.Sprintf("%d", time.Now().UnixNano())

	// Build request
	fullMethod := fmt.Sprintf("%s.%s", p.serviceName, method)
	body, err := p.ctx.Factory().BuildRequest(fullMethod, data, "")
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}

	// Create response channel
	responseCh := make(chan *ResponseContainer, 1)

	p.mu.Lock()
	p.pending[correlationID] = responseCh
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.pending, correlationID)
		p.mu.Unlock()
	}()

	// Publish request
	routingKey := fmt.Sprintf("REQUEST.%s.%s", p.serviceName, method)
	msg := amqp.Publishing{
		Body:          body,
		CorrelationId: correlationID,
		ReplyTo:       replyTo,
		DeliveryMode:  amqp.Persistent,
		Timestamp:     time.Now(),
	}

	if err := p.ctx.Connection().Publish(ctx, ch, p.ctx.ExchangeName(), routingKey, msg); err != nil {
		return fmt.Errorf("failed to publish request: %w", err)
	}

	// Wait for response with timeout
	timeout := GetConfig().DefaultRPCTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	select {
	case response := <-responseCh:
		if response.Error != nil {
			return NewHandledError(response.Error.Message, response.Error.Code)
		}

		// Copy result to output
		if result != nil && response.Result != nil {
			if resultMap, ok := result.(*map[string]interface{}); ok {
				*resultMap = response.Result
			}
		}
		return nil

	case <-time.After(timeout):
		return &TimeoutError{
			Operation: fmt.Sprintf("RPC call to %s", fullMethod),
			Duration:  timeout.String(),
		}

	case <-ctx.Done():
		return ctx.Err()
	}
}

// OpenStream initiates a server-streaming RPC. The returned ClientStream
// delivers chunks via Recv() until end-of-stream (io.EOF) or error.
//
// The caller MUST call Close() on the returned stream (typically via defer)
// to release the dispatcher slot, even after Recv returns io.EOF.
//
// See docs/advanced/streaming.md for the full contract.
func (p *ServiceProxy) OpenStream(
	ctx context.Context,
	method string,
	data map[string]interface{},
) (*ClientStream, error) {
	p.mu.RLock()
	if !p.initialized {
		p.mu.RUnlock()
		return nil, ErrNotInitialized
	}
	ch := p.channel
	replyTo := p.replyQueue.Name
	p.mu.RUnlock()

	correlationID := fmt.Sprintf("%d", time.Now().UnixNano())

	fullMethod := fmt.Sprintf("%s.%s", p.serviceName, method)
	body, err := p.ctx.Factory().BuildRequest(fullMethod, data, "")
	if err != nil {
		return nil, fmt.Errorf("failed to build streaming request: %w", err)
	}

	// Buffer the chunk channel generously so a slow Recv() doesn't block the
	// reply-listener goroutine. Bounded so a runaway producer can't OOM us.
	chunks := make(chan *ResponseContainer, 256)

	p.mu.Lock()
	p.pendingStreams[correlationID] = chunks
	p.mu.Unlock()

	// Publish the request. If publish fails we still return a stream — its
	// first Recv will surface the idle timeout (since no chunks will arrive).
	// More aggressive: clean up the pending slot here on failure.
	routingKey := fmt.Sprintf("REQUEST.%s.%s", p.serviceName, method)
	msg := amqp.Publishing{
		Body:          body,
		CorrelationId: correlationID,
		ReplyTo:       replyTo,
		DeliveryMode:  amqp.Persistent,
		Timestamp:     time.Now(),
	}
	if pubErr := p.ctx.Connection().Publish(ctx, ch, p.ctx.ExchangeName(), routingKey, msg); pubErr != nil {
		p.mu.Lock()
		delete(p.pendingStreams, correlationID)
		p.mu.Unlock()
		return nil, fmt.Errorf("failed to publish streaming request: %w", pubErr)
	}

	idleTimeout := GetConfig().StreamIdleTimeout
	if deadline, ok := ctx.Deadline(); ok {
		// Respect ctx deadline as an upper bound for any single Recv wait.
		if remaining := time.Until(deadline); remaining > 0 && remaining < idleTimeout {
			idleTimeout = remaining
		}
	}

	stream := &ClientStream{
		chunks:        chunks,
		idleTimeout:   idleTimeout,
		correlationID: correlationID,
	}
	stream.cleanup = func() {
		p.mu.Lock()
		if _, ok := p.pendingStreams[correlationID]; ok {
			delete(p.pendingStreams, correlationID)
			// Don't close the channel here — handleReplies may still be
			// trying to push. The garbage collector reclaims it once
			// handleReplies sees the slot is gone.
		}
		p.mu.Unlock()
	}
	return stream, nil
}

// Close closes the proxy.
func (p *ServiceProxy) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.initialized {
		return nil
	}

	if p.channel != nil {
		p.channel.Cancel(p.consumerTag, false)
		p.channel.Close()
	}

	p.initialized = false
	return nil
}

// ServiceName returns the service name.
func (p *ServiceProxy) ServiceName() string {
	return p.serviceName
}
