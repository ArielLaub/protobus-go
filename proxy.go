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
	initialized bool
	consumerTag string
}

// NewServiceProxy creates a new ServiceProxy.
func NewServiceProxy(ctx *Context, serviceName string) *ServiceProxy {
	return &ServiceProxy{
		ctx:         ctx,
		serviceName: serviceName,
		pending:     make(map[string]chan *ResponseContainer),
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
		p.mu.RLock()
		ch, exists := p.pending[delivery.CorrelationId]
		p.mu.RUnlock()

		if !exists {
			logWarn("Received reply for unknown correlation ID: %s", delivery.CorrelationId)
			continue
		}

		response, err := p.ctx.Factory().DecodeResponse(delivery.Body)
		if err != nil {
			logError("Failed to decode response: %v", err)
			continue
		}

		ch <- response
	}
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
