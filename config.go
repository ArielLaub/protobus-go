// Package protobus provides a lightweight, scalable microservices message bus.
//
// It leverages RabbitMQ for message routing and load balancing, combined with
// Protocol Buffers for efficient, type-safe serialization.
package protobus

import (
	"os"
	"strconv"
	"time"
)

// Config holds global configuration values.
type Config struct {
	// BusExchangeName is the name of the main exchange
	BusExchangeName string

	// EventsExchangeName is the name of the events exchange
	EventsExchangeName string

	// MessageProcessingTimeout is the default timeout for processing messages
	MessageProcessingTimeout time.Duration

	// DefaultRPCTimeout is the default timeout for RPC calls
	DefaultRPCTimeout time.Duration

	// StreamIdleTimeout is the idle timeout between chunks for streaming
	// RPC calls. A streaming call returns ErrStreamIdleTimeout if no chunk
	// arrives within this window. The standard RPC timeout does NOT apply
	// to streams. See docs/advanced/streaming.md.
	StreamIdleTimeout time.Duration
}

// Streaming wire-protocol header names. Identical to the TS and Python ports.
// See docs/advanced/streaming.md for the wire protocol.
const (
	HeaderProtobusFinal = "x-protobus-final"
	HeaderProtobusSeq   = "x-protobus-seq"
)

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		BusExchangeName:          getEnvOrDefault("PROTOBUS_EXCHANGE", "protobus"),
		EventsExchangeName:       getEnvOrDefault("PROTOBUS_EVENTS_EXCHANGE", "protobus.events"),
		MessageProcessingTimeout: getDurationEnvOrDefault("PROTOBUS_MESSAGE_TIMEOUT", 30*time.Second),
		DefaultRPCTimeout:        getDurationEnvOrDefault("PROTOBUS_RPC_TIMEOUT", 30*time.Second),
		StreamIdleTimeout:        getDurationEnvOrDefault("PROTOBUS_STREAM_IDLE_TIMEOUT", 60*time.Second),
	}
}

// globalConfig is the global configuration instance
var globalConfig = DefaultConfig()

// GetConfig returns the global configuration.
func GetConfig() *Config {
	return globalConfig
}

// SetConfig sets the global configuration.
func SetConfig(cfg *Config) {
	globalConfig = cfg
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getDurationEnvOrDefault(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return defaultValue
}
