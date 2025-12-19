package protobus

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionOptions configures connection behavior.
type ConnectionOptions struct {
	MaxReconnectAttempts      int
	InitialReconnectDelayMs   int
	MaxReconnectDelayMs       int
	ReconnectBackoffMultiplier float64
	JitterPercent             float64
}

// DefaultConnectionOptions returns sensible defaults.
func DefaultConnectionOptions() *ConnectionOptions {
	return &ConnectionOptions{
		MaxReconnectAttempts:      10,
		InitialReconnectDelayMs:   1000,
		MaxReconnectDelayMs:       30000,
		ReconnectBackoffMultiplier: 2.0,
		JitterPercent:             0.3,
	}
}

// RetryOptions configures message retry behavior.
type RetryOptions struct {
	MaxRetries    int
	RetryDelayMs  int
	MessageTTLMs  *int
}

// DefaultRetryOptions returns sensible defaults.
func DefaultRetryOptions() *RetryOptions {
	return &RetryOptions{
		MaxRetries:   3,
		RetryDelayMs: 5000,
	}
}

// ConnectionEvent represents connection state changes.
type ConnectionEvent int

const (
	ConnectionEventReconnecting ConnectionEvent = iota
	ConnectionEventReconnected
	ConnectionEventDisconnected
	ConnectionEventError
)

// ConnectionListener is called on connection events.
type ConnectionListener func(event ConnectionEvent, data interface{})

// Connection manages the RabbitMQ connection with automatic reconnection.
type Connection struct {
	mu            sync.RWMutex
	conn          *amqp.Connection
	url           string
	options       *ConnectionOptions
	isConnected   bool
	isReconnecting bool
	isClosing     bool
	listeners     []ConnectionListener
	closeCh       chan struct{}
}

// NewConnection creates a new Connection.
func NewConnection(options *ConnectionOptions) *Connection {
	if options == nil {
		options = DefaultConnectionOptions()
	}
	return &Connection{
		options:   options,
		listeners: make([]ConnectionListener, 0),
		closeCh:   make(chan struct{}),
	}
}

// OnEvent registers a connection event listener.
func (c *Connection) OnEvent(listener ConnectionListener) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listeners = append(c.listeners, listener)
}

func (c *Connection) emit(event ConnectionEvent, data interface{}) {
	c.mu.RLock()
	listeners := make([]ConnectionListener, len(c.listeners))
	copy(listeners, c.listeners)
	c.mu.RUnlock()

	for _, l := range listeners {
		go l(event, data)
	}
}

// Connect establishes a connection to RabbitMQ.
func (c *Connection) Connect(url string) error {
	c.mu.Lock()
	if c.isConnected {
		c.mu.Unlock()
		return ErrAlreadyConnected
	}
	c.url = url
	c.mu.Unlock()

	logInfo("Connecting to bus: %s", url)

	conn, err := amqp.Dial(url)
	if err != nil {
		logError("Failed to connect: %v", err)
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.isConnected = true
	c.mu.Unlock()

	// Watch for connection close
	go c.watchConnection()

	logInfo("Connected to RabbitMQ")
	return nil
}

func (c *Connection) watchConnection() {
	closeCh := c.conn.NotifyClose(make(chan *amqp.Error))

	select {
	case err := <-closeCh:
		c.mu.Lock()
		if c.isClosing {
			c.mu.Unlock()
			return
		}
		c.isConnected = false
		c.mu.Unlock()

		logWarn("Connection to RabbitMQ lost: %v", err)
		c.emit(ConnectionEventDisconnected, err)

		c.mu.Lock()
		shouldReconnect := !c.isReconnecting && !c.isClosing
		c.mu.Unlock()

		if shouldReconnect {
			go c.reconnect()
		}

	case <-c.closeCh:
		return
	}
}

func (c *Connection) reconnect() {
	c.mu.Lock()
	if c.isReconnecting || c.isClosing {
		c.mu.Unlock()
		return
	}
	c.isReconnecting = true
	url := c.url
	c.mu.Unlock()

	delay := c.options.InitialReconnectDelayMs

	for attempt := 1; attempt <= c.options.MaxReconnectAttempts; attempt++ {
		c.mu.RLock()
		if c.isClosing {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()

		c.emit(ConnectionEventReconnecting, map[string]int{
			"attempt":     attempt,
			"maxAttempts": c.options.MaxReconnectAttempts,
		})
		logInfo("Reconnection attempt %d/%d", attempt, c.options.MaxReconnectAttempts)

		conn, err := amqp.Dial(url)
		if err == nil {
			c.mu.Lock()
			c.conn = conn
			c.isConnected = true
			c.isReconnecting = false
			c.mu.Unlock()

			go c.watchConnection()

			logInfo("Reconnected to RabbitMQ")
			c.emit(ConnectionEventReconnected, nil)
			return
		}

		logWarn("Reconnection attempt %d failed: %v", attempt, err)

		if attempt < c.options.MaxReconnectAttempts {
			// Add jitter to prevent thundering herd
			jitter := (rand.Float64()*2 - 1) * c.options.JitterPercent
			actualDelay := float64(delay) * (1 + jitter)
			time.Sleep(time.Duration(actualDelay) * time.Millisecond)

			delay = int(float64(delay) * c.options.ReconnectBackoffMultiplier)
			if delay > c.options.MaxReconnectDelayMs {
				delay = c.options.MaxReconnectDelayMs
			}
		}
	}

	c.mu.Lock()
	c.isReconnecting = false
	c.mu.Unlock()

	err := &ReconnectionError{
		Attempts: c.options.MaxReconnectAttempts,
		LastErr:  fmt.Errorf("max reconnection attempts reached"),
	}
	logError("%v", err)
	c.emit(ConnectionEventError, err)
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	c.isClosing = true
	conn := c.conn
	c.mu.Unlock()

	close(c.closeCh)

	if conn != nil {
		if err := conn.Close(); err != nil {
			return err
		}
	}

	c.mu.Lock()
	c.isConnected = false
	c.isReconnecting = false
	c.conn = nil
	c.mu.Unlock()

	logInfo("Connection closed")
	return nil
}

// IsConnected returns whether the connection is active.
func (c *Connection) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isConnected
}

// IsReconnecting returns whether reconnection is in progress.
func (c *Connection) IsReconnecting() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isReconnecting
}

// Channel opens a new channel.
func (c *Connection) Channel() (*amqp.Channel, error) {
	c.mu.RLock()
	conn := c.conn
	isConnected := c.isConnected
	c.mu.RUnlock()

	if !isConnected || conn == nil {
		return nil, ErrNotConnected
	}

	return conn.Channel()
}

// DeclareExchange declares an exchange.
func (c *Connection) DeclareExchange(ch *amqp.Channel, name, kind string) error {
	return ch.ExchangeDeclare(
		name,  // name
		kind,  // type (topic, direct, fanout, headers)
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
}

// DeclareQueue declares a queue.
func (c *Connection) DeclareQueue(ch *amqp.Channel, name string, args amqp.Table) (amqp.Queue, error) {
	durable := name != ""
	exclusive := name == ""
	autoDelete := name == ""

	return ch.QueueDeclare(
		name,       // name
		durable,    // durable
		autoDelete, // delete when unused
		exclusive,  // exclusive
		false,      // no-wait
		args,       // arguments
	)
}

// BindQueue binds a queue to an exchange.
func (c *Connection) BindQueue(ch *amqp.Channel, queueName, routingKey, exchangeName string) error {
	return ch.QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
}

// Publish publishes a message.
func (c *Connection) Publish(ctx context.Context, ch *amqp.Channel, exchange, routingKey string, msg amqp.Publishing) error {
	c.mu.RLock()
	isConnected := c.isConnected
	c.mu.RUnlock()

	if !isConnected {
		return ErrNotConnected
	}

	return ch.PublishWithContext(
		ctx,
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		msg,
	)
}

// Consume starts consuming messages from a queue.
func (c *Connection) Consume(ch *amqp.Channel, queueName, consumerTag string, autoAck bool) (<-chan amqp.Delivery, error) {
	return ch.Consume(
		queueName,   // queue
		consumerTag, // consumer tag
		autoAck,     // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
}
