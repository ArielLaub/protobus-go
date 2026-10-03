package protobus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// Error codes. The first group travels on the wire in a ResponseError and is
// shared with the other ports; the second names local failures, which can
// still reach a further caller when a service relays them.
const (
	CodeHandled           = "HANDLED_ERROR"
	CodeProtocol          = "PROTOCOL_ERROR"
	CodeInternal          = "INTERNAL_ERROR"
	CodeProcessingTimeout = "PROCESSING_TIMEOUT"

	CodeRPCTimeout            = "RPC_TIMEOUT"
	CodeNotReady              = "NOT_READY"
	CodePublishNacked         = "PUBLISH_NACKED"
	CodeUnroutable            = "UNROUTABLE"
	CodePublishConfirmTimeout = "PUBLISH_CONFIRM_TIMEOUT"
	CodeChannelClosed         = "CHANNEL_CLOSED"
)

// Sentinel errors, for errors.Is.
var (
	// ErrClosed reports use of a Bus, Service or listener after Close.
	ErrClosed = errors.New("protobus: closed")
	// ErrNotReady reports a publish that waited for a reconnection longer than
	// Config.ConnectionReadyTimeout, or one issued after the bus gave up or
	// was closed. Nothing was published.
	ErrNotReady = errors.New("protobus: connection not ready")
	// ErrDisconnected reports a call in flight when the connection was lost.
	// The request may or may not have been processed.
	ErrDisconnected = errors.New("protobus: connection lost during call")
	// ErrRPCTimeout reports a call that got no reply in time. It also
	// satisfies errors.Is(err, context.DeadlineExceeded).
	ErrRPCTimeout = errors.New("protobus: rpc timed out")

	// Publish outcomes, always wrapped in a *PublishError. Nacked and
	// Unroutable are definite: the message was not delivered and republishing
	// is safe. ConfirmTimeout and ChannelClosed are AMBIGUOUS: the broker may
	// have stored the message, so republishing can duplicate it; reuse the
	// same message id (WithMessageID) so consumers can deduplicate.
	ErrPublishNacked         = errors.New("protobus: broker nacked the publish")
	ErrUnroutable            = errors.New("protobus: message was unroutable")
	ErrPublishConfirmTimeout = errors.New("protobus: no broker confirm in time")
	ErrChannelClosed         = errors.New("protobus: channel closed before the broker confirmed")

	// Streaming failures, raised on the caller's side.
	ErrStreamTimeout      = errors.New("protobus: no stream chunk within the idle timeout")
	ErrStreamBackpressure = errors.New("protobus: stream buffer limit exceeded")
	ErrStreamSequence     = errors.New("protobus: stream lost a chunk")

	// Option validation.
	ErrInvalidPriority  = errors.New("protobus: invalid priority")
	ErrInvalidMessageID = errors.New("protobus: invalid message id")

	// ErrRetryQueueMismatch reports a retry queue that already exists with
	// different arguments, in practice a changed RetryDelay. RabbitMQ cannot
	// change a queue's TTL in place: drain and delete the queue, or keep the
	// original delay.
	ErrRetryQueueMismatch = errors.New("protobus: retry queue exists with different arguments")

	// ErrUnimplemented is returned by generated Unimplemented servers. The
	// caller receives a PROTOCOL_ERROR naming the method.
	ErrUnimplemented = errors.New("protobus: method not implemented")
)

var sentinelCodes = map[error]string{
	ErrRPCTimeout:            CodeRPCTimeout,
	ErrNotReady:              CodeNotReady,
	ErrPublishNacked:         CodePublishNacked,
	ErrUnroutable:            CodeUnroutable,
	ErrPublishConfirmTimeout: CodePublishConfirmTimeout,
	ErrChannelClosed:         CodeChannelClosed,
}

// HandledError is an error a service raises deliberately to tell its caller
// something: a validation failure, a business rule. It is answered at once and
// never retried, and its message always reaches the caller.
//
// Anything else a handler returns is treated as an infrastructure failure:
// the request is retried and, once retries run out, dead-lettered.
//
// A HandledError may be wrapped (fmt.Errorf("...: %w", herr)); the caller then
// receives the HandledError's own Code and Message, not the wrapping text.
type HandledError struct {
	Code    string
	Message string
}

// NewHandledError returns a HandledError. An empty code becomes HANDLED_ERROR.
func NewHandledError(code, message string) *HandledError {
	if code == "" {
		code = CodeHandled
	}
	return &HandledError{Code: code, Message: message}
}

func (e *HandledError) Error() string { return e.Message }

// ErrorCode implements the coded-error convention ErrorCode reads.
func (e *HandledError) ErrorCode() string { return e.Code }

func (e *HandledError) errorName() string { return "HandledError" }

// AsHandled reports whether err is, or wraps, a HandledError.
func AsHandled(err error) (*HandledError, bool) {
	var h *HandledError
	if errors.As(err, &h) {
		return h, true
	}
	return nil, false
}

func newProtocolError(message string) *HandledError {
	return &HandledError{Code: CodeProtocol, Message: message}
}

// RemoteError is a failure reported by the remote service: what its
// ResponseError carried. Code is empty when the service sent none.
type RemoteError struct {
	Method  string
	Code    string
	Message string
}

func (e *RemoteError) Error() string     { return e.Message }
func (e *RemoteError) ErrorCode() string { return e.Code }

// PublishError reports a publish the broker did not positively confirm. Err
// is one of ErrPublishNacked, ErrUnroutable, ErrPublishConfirmTimeout or
// ErrChannelClosed.
type PublishError struct {
	Err        error
	MessageID  string
	Exchange   string
	RoutingKey string
	// Detail is the broker's or the client library's explanation, if any.
	Detail string
}

func (e *PublishError) Error() string {
	dest := e.RoutingKey
	if e.Exchange != "" {
		dest = e.Exchange + " -> " + e.RoutingKey
	}
	msg := fmt.Sprintf("%v (%s, messageId %s)", e.Err, dest, e.MessageID)
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

func (e *PublishError) Unwrap() error { return e.Err }

// Ambiguous reports whether the message may have been stored despite the
// error, so that republishing could duplicate it.
func (e *PublishError) Ambiguous() bool {
	return errors.Is(e.Err, ErrPublishConfirmTimeout) || errors.Is(e.Err, ErrChannelClosed) ||
		errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded)
}

func (e *PublishError) ErrorCode() string { return sentinelCodes[e.Err] }

func (e *PublishError) errorName() string {
	switch e.Err {
	case ErrPublishNacked:
		return "PublishNackedError"
	case ErrUnroutable:
		return "UnroutableError"
	case ErrPublishConfirmTimeout:
		return "PublishConfirmTimeoutError"
	case ErrChannelClosed:
		return "ChannelClosedError"
	}
	return "PublishError"
}

// coder is the convention for errors that carry a machine-readable code.
// Errors from other libraries that expose Code() string are honoured too.
type coder interface{ ErrorCode() string }
type foreignCoder interface{ Code() string }

// ErrorCode returns the code carried by err or anything it wraps: a remote or
// handled error's code, or the code of a protobus local failure. It returns
// "" when there is none.
func ErrorCode(err error) string {
	for e := err; e != nil; {
		switch c := e.(type) {
		case coder:
			if code := c.ErrorCode(); code != "" {
				return code
			}
		case foreignCoder:
			if code := c.Code(); code != "" {
				return code
			}
		}
		if code, ok := sentinelCodes[e]; ok {
			return code
		}
		switch u := e.(type) {
		case interface{ Unwrap() error }:
			e = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, inner := range u.Unwrap() {
				if code := ErrorCode(inner); code != "" {
					return code
				}
			}
			return ""
		default:
			return ""
		}
	}
	return ""
}

// IsCode reports whether ErrorCode(err) == code.
func IsCode(err error, code string) bool { return code != "" && ErrorCode(err) == code }

func rpcTimeoutError(routingKey, correlationID string, cause error) error {
	return fmt.Errorf("%w: no reply for %s (correlationId %s): %w", ErrRPCTimeout, routingKey, correlationID, cause)
}

// processingTimeoutError reports a service-side attempt that overran
// Config.ProcessingTimeout. Its text is framework-generated, so it is safe to
// show a caller whatever the exposure setting.
type processingTimeoutError struct {
	correlationID string
	limit         time.Duration
}

func newProcessingTimeoutError(correlationID string, limit time.Duration) *processingTimeoutError {
	return &processingTimeoutError{correlationID: correlationID, limit: limit}
}

func (e *processingTimeoutError) Error() string {
	return fmt.Sprintf("message %s exceeded the %v processing timeout", e.correlationID, e.limit)
}
func (e *processingTimeoutError) ErrorCode() string { return CodeProcessingTimeout }
func (e *processingTimeoutError) errorName() string { return "TimeoutError" }

type named interface{ errorName() string }

// errorName gives the closest equivalent of a JavaScript error's `name`: the
// protobus class name for our own errors, else the Go type name.
func errorName(err error) string {
	if n, ok := err.(named); ok {
		return n.errorName()
	}
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch name := t.Name(); name {
	case "", "errorString", "wrapError", "wrapErrors", "joinError":
		return "Error"
	default:
		return name
	}
}

// safeErrorSummary describes err for places its text travels further than the
// process — the x-last-error header on retry and DLQ copies, which dashboards
// read and queues keep. It names the error and its code but never quotes an
// unhandled error's message, which routinely interpolates the data that caused
// it. A HandledError's message is kept: exposing it was the point.
func safeErrorSummary(err error) string {
	if err == nil {
		return "UnknownError"
	}
	if h, ok := AsHandled(err); ok {
		return fmt.Sprintf("HandledError[%s]: %s", h.Code, h.Message)
	}
	name := errorName(innermostNamed(err))
	if code := ErrorCode(err); code != "" {
		return fmt.Sprintf("%s[%s]", name, code)
	}
	return name
}

// innermostNamed finds the most specific error worth naming: the first in the
// chain that is not an anonymous fmt wrapper.
func innermostNamed(err error) error {
	for e := err; e != nil; {
		if _, ok := e.(named); ok {
			return e
		}
		name := errorName(e)
		if name != "Error" {
			return e
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return e
		}
		next := u.Unwrap()
		if next == nil {
			return e
		}
		e = next
	}
	return err
}

// callerError is what a caller is told about a failure.
type callerError struct {
	Code    string
	Message string
}

// sanitizeForCaller decides what an error looks like to the caller. A
// HandledError crosses as it is. A processing timeout crosses as a framework
// fact. Anything else crosses only when exposure is on; otherwise it becomes a
// generic internal error and the real one stays in the service's own log.
func sanitizeForCaller(err error, expose bool) callerError {
	if h, ok := AsHandled(err); ok {
		return callerError{Code: h.Code, Message: h.Message}
	}
	var pt *processingTimeoutError
	if errors.As(err, &pt) {
		return callerError{Code: CodeProcessingTimeout, Message: pt.Error()}
	}
	if !expose {
		return callerError{Code: CodeInternal, Message: "internal service error"}
	}
	return callerError{Code: ErrorCode(err), Message: err.Error()}
}
