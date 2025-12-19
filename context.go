package protobus

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ContextOptions configures the Context.
type ContextOptions struct {
	ConnectionOptions *ConnectionOptions
}

// Context manages the connection and provides access to protobus functionality.
type Context struct {
	mu         sync.RWMutex
	conn       *Connection
	factory    *MessageFactory
	channel    *amqp.Channel
	exchange   string
	eventsExch string
	initialized bool
}

// NewContext creates a new Context.
func NewContext(options *ContextOptions) *Context {
	var connOpts *ConnectionOptions
	if options != nil {
		connOpts = options.ConnectionOptions
	}

	return &Context{
		conn:       NewConnection(connOpts),
		factory:    NewMessageFactory(),
		exchange:   GetConfig().BusExchangeName,
		eventsExch: GetConfig().EventsExchangeName,
	}
}

// Init initializes the context and connects to RabbitMQ.
func (c *Context) Init(url string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.initialized {
		return ErrAlreadyInitialized
	}

	// Initialize factory
	if err := c.factory.Init(); err != nil {
		return fmt.Errorf("failed to init factory: %w", err)
	}

	// Connect
	if err := c.conn.Connect(url); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Open a channel for publishing
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	c.channel = ch

	// Declare exchanges
	if err := c.conn.DeclareExchange(ch, c.exchange, "topic"); err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}
	if err := c.conn.DeclareExchange(ch, c.eventsExch, "topic"); err != nil {
		return fmt.Errorf("failed to declare events exchange: %w", err)
	}

	c.initialized = true
	logInfo("Context initialized")
	return nil
}

// Close closes the context.
func (c *Context) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel != nil {
		c.channel.Close()
	}

	if err := c.conn.Close(); err != nil {
		return err
	}

	c.initialized = false
	logInfo("Context closed")
	return nil
}

// Connection returns the underlying connection.
func (c *Context) Connection() *Connection {
	return c.conn
}

// Factory returns the message factory.
func (c *Context) Factory() *MessageFactory {
	return c.factory
}

// IsConnected returns whether the context is connected.
func (c *Context) IsConnected() bool {
	return c.conn.IsConnected()
}

// PublishEvent publishes an event.
func (c *Context) PublishEvent(ctx context.Context, eventType string, data map[string]interface{}, topic string) error {
	c.mu.RLock()
	if !c.initialized {
		c.mu.RUnlock()
		return ErrNotInitialized
	}
	ch := c.channel
	c.mu.RUnlock()

	body, err := c.factory.BuildEvent(eventType, data, topic)
	if err != nil {
		return fmt.Errorf("failed to build event: %w", err)
	}

	routingKey := eventType
	if topic != "" {
		routingKey = topic
	}

	msg := amqp.Publishing{
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
	}

	return c.conn.Publish(ctx, ch, c.eventsExch, routingKey, msg)
}

// ExchangeName returns the main exchange name.
func (c *Context) ExchangeName() string {
	return c.exchange
}

// EventsExchangeName returns the events exchange name.
func (c *Context) EventsExchangeName() string {
	return c.eventsExch
}
