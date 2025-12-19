package protobus

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageHandler handles incoming messages.
// Returns response bytes (for RPC) or nil (for events).
type MessageHandler func(ctx context.Context, body []byte, correlationID string) ([]byte, error)

// BaseListener provides common listener functionality.
type BaseListener struct {
	mu            sync.RWMutex
	conn          *Connection
	channel       *amqp.Channel
	queue         amqp.Queue
	exchange      string
	handler       MessageHandler
	lateAck       bool
	maxConcurrent int
	retryOptions  *RetryOptions
	consumerTag   string
	started       bool
	patterns      []string
}

// NewBaseListener creates a new BaseListener.
func NewBaseListener(conn *Connection, lateAck bool, maxConcurrent int, retryOptions *RetryOptions) *BaseListener {
	if retryOptions == nil {
		retryOptions = DefaultRetryOptions()
	}
	return &BaseListener{
		conn:          conn,
		lateAck:       lateAck,
		maxConcurrent: maxConcurrent,
		retryOptions:  retryOptions,
		patterns:      make([]string, 0),
	}
}

// Init initializes the listener.
func (l *BaseListener) Init(handler MessageHandler, queueName, exchangeName string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.handler = handler
	l.exchange = exchangeName

	ch, err := l.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	l.channel = ch

	// Set QoS if max concurrent is set
	if l.maxConcurrent > 0 {
		if err := ch.Qos(l.maxConcurrent, 0, false); err != nil {
			return fmt.Errorf("failed to set QoS: %w", err)
		}
	}

	// Declare exchange
	if err := l.conn.DeclareExchange(ch, exchangeName, "topic"); err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Declare queue
	queue, err := l.conn.DeclareQueue(ch, queueName, nil)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}
	l.queue = queue

	return nil
}

// Subscribe binds the queue to a routing pattern.
func (l *BaseListener) Subscribe(pattern string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.conn.BindQueue(l.channel, l.queue.Name, pattern, l.exchange); err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	l.patterns = append(l.patterns, pattern)
	logDebug("Subscribed to pattern: %s", pattern)
	return nil
}

// Start begins consuming messages.
func (l *BaseListener) Start() error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return ErrAlreadyStarted
	}
	l.started = true
	l.mu.Unlock()

	l.consumerTag = fmt.Sprintf("protobus-%s-%d", l.queue.Name, time.Now().UnixNano())

	deliveries, err := l.conn.Consume(l.channel, l.queue.Name, l.consumerTag, !l.lateAck)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	go l.consumeLoop(deliveries)

	logDebug("Started consuming from queue: %s", l.queue.Name)
	return nil
}

func (l *BaseListener) consumeLoop(deliveries <-chan amqp.Delivery) {
	for delivery := range deliveries {
		go l.handleDelivery(delivery)
	}
}

func (l *BaseListener) handleDelivery(delivery amqp.Delivery) {
	ctx := context.Background()

	// Add timeout
	timeout := GetConfig().MessageProcessingTimeout
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	response, err := l.handler(ctx, delivery.Body, delivery.CorrelationId)

	if err != nil {
		if IsHandledError(err) {
			logDebug("Handled error, not retrying: %v", err)
			if l.lateAck {
				delivery.Ack(false)
			}
			// Send error response for RPC
			if delivery.ReplyTo != "" {
				l.sendErrorResponse(delivery, err)
			}
			return
		}

		// Check retry count
		retryCount := l.getRetryCount(delivery)
		if retryCount < l.retryOptions.MaxRetries {
			l.retryMessage(delivery, retryCount, err)
		} else {
			l.sendToDLQ(delivery, err)
		}

		if l.lateAck {
			delivery.Ack(false)
		}
		return
	}

	// Send RPC reply if needed
	if delivery.ReplyTo != "" && response != nil {
		l.sendReply(delivery, response)
	}

	if l.lateAck {
		delivery.Ack(false)
	}
}

func (l *BaseListener) getRetryCount(delivery amqp.Delivery) int {
	if delivery.Headers == nil {
		return 0
	}
	if count, ok := delivery.Headers["x-retry-count"].(int32); ok {
		return int(count)
	}
	if count, ok := delivery.Headers["x-retry-count"].(int64); ok {
		return int(count)
	}
	return 0
}

func (l *BaseListener) retryMessage(delivery amqp.Delivery, retryCount int, err error) {
	headers := amqp.Table{}
	if delivery.Headers != nil {
		for k, v := range delivery.Headers {
			headers[k] = v
		}
	}
	headers["x-retry-count"] = int32(retryCount + 1)
	headers["x-last-error"] = err.Error()
	if _, exists := headers["x-first-failure-time"]; !exists {
		headers["x-first-failure-time"] = time.Now().UnixMilli()
	}

	logDebug("Retrying message (attempt %d/%d)", retryCount+1, l.retryOptions.MaxRetries)

	msg := amqp.Publishing{
		Body:          delivery.Body,
		Headers:       headers,
		CorrelationId: delivery.CorrelationId,
		ReplyTo:       delivery.ReplyTo,
		Expiration:    fmt.Sprintf("%d", l.retryOptions.RetryDelayMs),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	retryKey := delivery.RoutingKey + ".retry"
	if err := l.conn.Publish(ctx, l.channel, l.exchange, retryKey, msg); err != nil {
		logError("Failed to retry message: %v", err)
	}
}

func (l *BaseListener) sendToDLQ(delivery amqp.Delivery, err error) {
	headers := amqp.Table{}
	if delivery.Headers != nil {
		for k, v := range delivery.Headers {
			headers[k] = v
		}
	}
	headers["x-death-reason"] = err.Error()
	headers["x-death-time"] = time.Now().UnixMilli()

	dlqName := delivery.RoutingKey + ".DLQ"
	logWarn("Message exhausted retries, sending to DLQ: %s", dlqName)

	// Declare DLQ if needed
	_, qErr := l.conn.DeclareQueue(l.channel, dlqName, nil)
	if qErr != nil {
		logError("Failed to declare DLQ: %v", qErr)
		return
	}

	msg := amqp.Publishing{
		Body:          delivery.Body,
		Headers:       headers,
		CorrelationId: delivery.CorrelationId,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := l.conn.Publish(ctx, l.channel, "", dlqName, msg); err != nil {
		logError("Failed to send to DLQ: %v", err)
	}
}

func (l *BaseListener) sendReply(delivery amqp.Delivery, response []byte) {
	msg := amqp.Publishing{
		Body:          response,
		CorrelationId: delivery.CorrelationId,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := l.conn.Publish(ctx, l.channel, "", delivery.ReplyTo, msg); err != nil {
		logError("Failed to send reply: %v", err)
	}
}

func (l *BaseListener) sendErrorResponse(delivery amqp.Delivery, err error) {
	factory := NewMessageFactory()
	factory.Init()

	response, buildErr := factory.BuildResponse(delivery.RoutingKey, nil, err)
	if buildErr != nil {
		logError("Failed to build error response: %v", buildErr)
		return
	}

	l.sendReply(delivery, response)
}

// Stop stops the listener.
func (l *BaseListener) Stop() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.started {
		return nil
	}

	if l.channel != nil {
		if err := l.channel.Cancel(l.consumerTag, false); err != nil {
			logWarn("Failed to cancel consumer: %v", err)
		}
		if err := l.channel.Close(); err != nil {
			logWarn("Failed to close channel: %v", err)
		}
	}

	l.started = false
	return nil
}
