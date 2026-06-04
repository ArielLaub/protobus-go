package protobus

import (
	"errors"
	"fmt"
)

// HandledError represents an expected error that shouldn't trigger retries.
// Use this for validation errors, business logic failures, and other
// expected error conditions.
type HandledError struct {
	Message string
	Code    string
}

func (e *HandledError) Error() string {
	return fmt.Sprintf("%s (code: %s)", e.Message, e.Code)
}

// NewHandledError creates a new HandledError.
func NewHandledError(message, code string) *HandledError {
	return &HandledError{
		Message: message,
		Code:    code,
	}
}

// IsHandledError checks if an error is a HandledError.
func IsHandledError(err error) bool {
	var he *HandledError
	return errors.As(err, &he)
}

// GetHandledError extracts the HandledError from an error if it is one.
func GetHandledError(err error) (*HandledError, bool) {
	var he *HandledError
	if errors.As(err, &he) {
		return he, true
	}
	return nil, false
}

// Common errors
var (
	ErrAlreadyConnected    = errors.New("already connected to RabbitMQ")
	ErrNotConnected        = errors.New("not connected to RabbitMQ")
	ErrDisconnected        = errors.New("disconnected from RabbitMQ")
	ErrReconnectionFailed  = errors.New("reconnection failed")
	ErrTimeout             = errors.New("operation timed out")
	ErrNotInitialized      = errors.New("not initialized")
	ErrAlreadyInitialized  = errors.New("already initialized")
	ErrAlreadyStarted      = errors.New("already started")
	ErrInvalidMessage      = errors.New("invalid message")
	ErrInvalidRequest      = errors.New("invalid request")
	ErrInvalidResponse     = errors.New("invalid response")
	ErrInvalidServiceName  = errors.New("invalid service name")
	ErrInvalidMethod       = errors.New("invalid method")
	ErrInvalidResult       = errors.New("invalid result")
	ErrPublishFailed       = errors.New("failed to publish message")
	ErrMissingProto        = errors.New("missing proto file")
	ErrMissingExchange     = errors.New("missing exchange")
	ErrMessageTypeRequired = errors.New("message type required")
)

// TimeoutError represents a timeout with additional context.
type TimeoutError struct {
	Operation string
	Duration  string
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("timeout after %s: %s", e.Duration, e.Operation)
}

// ReconnectionError represents a reconnection failure.
type ReconnectionError struct {
	Attempts int
	LastErr  error
}

func (e *ReconnectionError) Error() string {
	return fmt.Sprintf("failed to reconnect after %d attempts: %v", e.Attempts, e.LastErr)
}

func (e *ReconnectionError) Unwrap() error {
	return e.LastErr
}

// Streaming-related errors. See docs/advanced/streaming.md.
var (
	// ErrStreamIdleTimeout is returned when no chunk arrives within the idle
	// timeout. Streaming calls use an idle timeout, not a total-call timeout —
	// a stream is permitted to take far longer than any single chunk gap.
	ErrStreamIdleTimeout = errors.New("streaming idle timeout")

	// ErrStreamClosed is returned when receiving from a stream that has
	// already been closed (by Close(), connection loss, or terminal message).
	ErrStreamClosed = errors.New("stream closed")

	// ErrNotStreamingMethod is returned when calling OpenStream on a method
	// the server did not register as streaming (or vice versa).
	ErrNotStreamingMethod = errors.New("method not registered as streaming")
)
