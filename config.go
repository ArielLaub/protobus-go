package protobus

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Message priority levels. The names and values match the Config constants of
// the TypeScript and Python ports, so a mixed deployment speaks one
// vocabulary.
//
// PriorityNormal is 0 because that is how RabbitMQ sorts a message carrying no
// priority at all: an un-upgraded publisher and one passing PriorityNormal
// sort identically on the same queue.
const (
	PriorityNormal  uint8 = 0
	PriorityHigh    uint8 = 1
	PriorityControl uint8 = 2

	// RecommendedMaxPriority is the queue depth that gives the three levels
	// above. RabbitMQ keeps internal structures per level, so keep it small.
	RecommendedMaxPriority uint8 = 2
)

// Headers of the server-streaming wire protocol.
const (
	HeaderFinal = "x-protobus-final"
	HeaderSeq   = "x-protobus-seq"
)

// ReconnectPolicy shapes automatic reconnection after the broker connection is
// lost: exponential backoff, capped, with up to 30% random jitter.
type ReconnectPolicy struct {
	// MaxRetries bounds consecutive failed attempts; 0 retries forever.
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// Config holds every tunable of a Bus. Start from DefaultConfig or
// ConfigFromEnv and change fields; Dial validates the result.
//
// Exchange names are part of the wire protocol: every process on one bus,
// whatever its language, must agree on them.
type Config struct {
	BusExchange       string // RPC requests (topic). BUS_EXCHANGE_NAME
	CallbacksExchange string // RPC replies (direct). CALLBACKS_EXCHANGE_NAME
	EventsExchange    string // events (topic). EVENTS_EXCHANGE_NAME
	CancelExchange    string // stream cancellation (fanout). CANCEL_EXCHANGE_NAME

	// ProcessingTimeout caps how long a service spends on one unary request
	// before the attempt is failed (and retried). MESSAGE_PROCESSING_TIMEOUT
	ProcessingTimeout time.Duration
	// RPCTimeout is how long a caller waits for a reply when its context
	// carries no earlier deadline. RPC_CALL_TIMEOUT_MS
	RPCTimeout time.Duration
	// StreamIdleTimeout is the longest gap a streaming caller tolerates
	// between chunks. STREAM_IDLE_TIMEOUT_MS
	StreamIdleTimeout time.Duration
	// DefaultPrefetch bounds unacknowledged deliveries for consumers that set
	// no concurrency of their own (event listeners). DEFAULT_PREFETCH
	DefaultPrefetch int
	// PublishConfirmTimeout bounds the wait for a broker confirm. Expiry is an
	// AMBIGUOUS outcome. PUBLISH_CONFIRM_TIMEOUT_MS
	PublishConfirmTimeout time.Duration
	// Heartbeat is the AMQP heartbeat interval. A `heartbeat` parameter in
	// the broker URL wins, which is also how heartbeats are turned off
	// (heartbeat=0). AMQP_HEARTBEAT_SECONDS
	Heartbeat time.Duration
	// ConnectionReadyTimeout bounds how long a publish parked on a
	// reconnection waits before failing with ErrNotReady.
	// CONNECTION_READY_TIMEOUT_MS
	ConnectionReadyTimeout time.Duration
	// MaxOutstandingConfirms bounds unconfirmed publishes per channel; further
	// publishes wait for a slot. MAX_OUTSTANDING_CONFIRMS
	MaxOutstandingConfirms int

	// Streaming caller buffer bounds. Crossing one fails the stream with
	// ErrStreamBackpressure rather than growing without limit.
	StreamMaxBufferedChunks     int   // per call. STREAM_MAX_BUFFERED_CHUNKS
	StreamMaxBufferedBytes      int64 // per call. STREAM_MAX_BUFFERED_BYTES
	StreamMaxTotalBufferedBytes int64 // per Bus. STREAM_MAX_TOTAL_BUFFERED_BYTES

	// ExposeInternalErrors sends the text of an UNHANDLED service error back
	// to the caller. A HandledError always crosses. Turn this off for a
	// service whose callers relay errors to untrusted clients; callers then
	// see "internal service error" with code INTERNAL_ERROR.
	// PROTOBUS_EXPOSE_INTERNAL_ERRORS
	ExposeInternalErrors bool

	// ShutdownDrainTimeout bounds how long Run waits for in-flight work to
	// finish on shutdown. SHUTDOWN_DRAIN_TIMEOUT_MS
	ShutdownDrainTimeout time.Duration

	Reconnect ReconnectPolicy
}

// DefaultConfig returns the defaults shared with the TypeScript and Python
// ports.
func DefaultConfig() Config {
	return Config{
		BusExchange:                 "proto.bus",
		CallbacksExchange:           "proto.bus.callback",
		EventsExchange:              "proto.bus.events",
		CancelExchange:              "proto.bus.cancel",
		ProcessingTimeout:           600 * time.Second,
		RPCTimeout:                  600 * time.Second,
		StreamIdleTimeout:           60 * time.Second,
		DefaultPrefetch:             1,
		PublishConfirmTimeout:       30 * time.Second,
		Heartbeat:                   30 * time.Second,
		ConnectionReadyTimeout:      30 * time.Second,
		MaxOutstandingConfirms:      256,
		StreamMaxBufferedChunks:     1024,
		StreamMaxBufferedBytes:      64 << 20,
		StreamMaxTotalBufferedBytes: 256 << 20,
		ExposeInternalErrors:        true,
		ShutdownDrainTimeout:        30 * time.Second,
		Reconnect: ReconnectPolicy{
			MaxRetries:   10,
			InitialDelay: time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2,
		},
	}
}

// ConfigFromEnv returns DefaultConfig overridden by the environment variables
// the other ports read, with the same parsing rules: an integer must be all
// digits and positive, a boolean one of 1/true/yes/on or 0/false/no/off
// (case-insensitive). Anything else keeps the default rather than becoming a
// surprising zero.
func ConfigFromEnv() Config {
	c := DefaultConfig()
	envString("BUS_EXCHANGE_NAME", &c.BusExchange)
	envString("CALLBACKS_EXCHANGE_NAME", &c.CallbacksExchange)
	envString("EVENTS_EXCHANGE_NAME", &c.EventsExchange)
	envString("CANCEL_EXCHANGE_NAME", &c.CancelExchange)
	envDuration("MESSAGE_PROCESSING_TIMEOUT", time.Millisecond, &c.ProcessingTimeout)
	envDuration("RPC_CALL_TIMEOUT_MS", time.Millisecond, &c.RPCTimeout)
	envDuration("STREAM_IDLE_TIMEOUT_MS", time.Millisecond, &c.StreamIdleTimeout)
	envInt("DEFAULT_PREFETCH", &c.DefaultPrefetch)
	envDuration("PUBLISH_CONFIRM_TIMEOUT_MS", time.Millisecond, &c.PublishConfirmTimeout)
	envDuration("AMQP_HEARTBEAT_SECONDS", time.Second, &c.Heartbeat)
	envDuration("CONNECTION_READY_TIMEOUT_MS", time.Millisecond, &c.ConnectionReadyTimeout)
	envInt("MAX_OUTSTANDING_CONFIRMS", &c.MaxOutstandingConfirms)
	envInt("STREAM_MAX_BUFFERED_CHUNKS", &c.StreamMaxBufferedChunks)
	envInt64("STREAM_MAX_BUFFERED_BYTES", &c.StreamMaxBufferedBytes)
	envInt64("STREAM_MAX_TOTAL_BUFFERED_BYTES", &c.StreamMaxTotalBufferedBytes)
	envBool("PROTOBUS_EXPOSE_INTERNAL_ERRORS", &c.ExposeInternalErrors)
	envDuration("SHUTDOWN_DRAIN_TIMEOUT_MS", time.Millisecond, &c.ShutdownDrainTimeout)
	return c
}

func envString(name string, dst *string) {
	if v := os.Getenv(name); v != "" {
		*dst = v
	}
}

// envPositive parses the TS envInt grammar: trimmed, ^\d+$, > 0, in range.
func envPositive(name string, max uint64) (uint64, bool) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0, false
	}
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || v == 0 || v > max {
		return 0, false
	}
	return v, true
}

func envInt(name string, dst *int) {
	if v, ok := envPositive(name, math.MaxInt32); ok {
		*dst = int(v)
	}
}

func envInt64(name string, dst *int64) {
	if v, ok := envPositive(name, math.MaxInt64); ok {
		*dst = int64(v)
	}
}

func envDuration(name string, unit time.Duration, dst *time.Duration) {
	if v, ok := envPositive(name, uint64(math.MaxInt64/int64(unit))); ok {
		*dst = time.Duration(v) * unit
	}
}

func envBool(name string, dst *bool) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		*dst = true
	case "0", "false", "no", "off":
		*dst = false
	}
}

// Validate reports a configuration that cannot work.
func (c Config) Validate() error {
	var errs []error
	bad := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("protobus: invalid config: "+format, args...))
	}
	names := map[string]string{}
	for field, name := range map[string]string{
		"BusExchange": c.BusExchange, "CallbacksExchange": c.CallbacksExchange,
		"EventsExchange": c.EventsExchange, "CancelExchange": c.CancelExchange,
	} {
		if name == "" {
			bad("%s is empty", field)
			continue
		}
		if other, dup := names[name]; dup {
			bad("%s and %s are both %q", other, field, name)
		}
		names[name] = field
	}
	positive := map[string]time.Duration{
		"ProcessingTimeout": c.ProcessingTimeout, "RPCTimeout": c.RPCTimeout,
		"StreamIdleTimeout": c.StreamIdleTimeout, "PublishConfirmTimeout": c.PublishConfirmTimeout,
		"ConnectionReadyTimeout": c.ConnectionReadyTimeout, "Reconnect.InitialDelay": c.Reconnect.InitialDelay,
	}
	for field, d := range positive {
		if d <= 0 {
			bad("%s must be positive, got %v", field, d)
		}
	}
	if c.Heartbeat < 0 {
		bad("Heartbeat must not be negative, got %v", c.Heartbeat)
	}
	if c.ShutdownDrainTimeout < 0 {
		bad("ShutdownDrainTimeout must not be negative, got %v", c.ShutdownDrainTimeout)
	}
	if c.DefaultPrefetch < 1 || c.DefaultPrefetch > math.MaxUint16 {
		bad("DefaultPrefetch must be within 1..%d, got %d", math.MaxUint16, c.DefaultPrefetch)
	}
	if c.MaxOutstandingConfirms < 1 {
		bad("MaxOutstandingConfirms must be positive, got %d", c.MaxOutstandingConfirms)
	}
	if c.StreamMaxBufferedChunks < 1 {
		bad("StreamMaxBufferedChunks must be positive, got %d", c.StreamMaxBufferedChunks)
	}
	if c.StreamMaxBufferedBytes < 1 {
		bad("StreamMaxBufferedBytes must be positive, got %d", c.StreamMaxBufferedBytes)
	}
	if c.StreamMaxTotalBufferedBytes < c.StreamMaxBufferedBytes {
		bad("StreamMaxTotalBufferedBytes (%d) must be at least StreamMaxBufferedBytes (%d)",
			c.StreamMaxTotalBufferedBytes, c.StreamMaxBufferedBytes)
	}
	if c.Reconnect.MaxRetries < 0 {
		bad("Reconnect.MaxRetries must not be negative, got %d", c.Reconnect.MaxRetries)
	}
	if c.Reconnect.MaxDelay < c.Reconnect.InitialDelay {
		bad("Reconnect.MaxDelay (%v) must be at least InitialDelay (%v)", c.Reconnect.MaxDelay, c.Reconnect.InitialDelay)
	}
	if c.Reconnect.Multiplier < 1 {
		bad("Reconnect.Multiplier must be at least 1, got %v", c.Reconnect.Multiplier)
	}
	return errors.Join(errs...)
}
