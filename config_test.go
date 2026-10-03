package protobus

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigMatchesTheOtherPorts(t *testing.T) {
	c := DefaultConfig()
	checks := []struct {
		name      string
		got, want any
	}{
		{"BusExchange", c.BusExchange, "proto.bus"},
		{"CallbacksExchange", c.CallbacksExchange, "proto.bus.callback"},
		{"EventsExchange", c.EventsExchange, "proto.bus.events"},
		{"CancelExchange", c.CancelExchange, "proto.bus.cancel"},
		{"ProcessingTimeout", c.ProcessingTimeout, 600 * time.Second},
		{"RPCTimeout", c.RPCTimeout, 600 * time.Second},
		{"StreamIdleTimeout", c.StreamIdleTimeout, 60 * time.Second},
		{"DefaultPrefetch", c.DefaultPrefetch, 1},
		{"PublishConfirmTimeout", c.PublishConfirmTimeout, 30 * time.Second},
		{"Heartbeat", c.Heartbeat, 30 * time.Second},
		{"ConnectionReadyTimeout", c.ConnectionReadyTimeout, 30 * time.Second},
		{"MaxOutstandingConfirms", c.MaxOutstandingConfirms, 256},
		{"StreamMaxBufferedChunks", c.StreamMaxBufferedChunks, 1024},
		{"StreamMaxBufferedBytes", c.StreamMaxBufferedBytes, int64(64 << 20)},
		{"StreamMaxTotalBufferedBytes", c.StreamMaxTotalBufferedBytes, int64(256 << 20)},
		{"ExposeInternalErrors", c.ExposeInternalErrors, true},
		{"ShutdownDrainTimeout", c.ShutdownDrainTimeout, 30 * time.Second},
		{"Reconnect.MaxRetries", c.Reconnect.MaxRetries, 10},
		{"Reconnect.InitialDelay", c.Reconnect.InitialDelay, time.Second},
		{"Reconnect.MaxDelay", c.Reconnect.MaxDelay, 30 * time.Second},
		{"Reconnect.Multiplier", c.Reconnect.Multiplier, 2.0},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s = %v, want %v", ch.name, ch.got, ch.want)
		}
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
}

func TestConfigFromEnvHonoursOverrides(t *testing.T) {
	t.Setenv("BUS_EXCHANGE_NAME", "custom.bus")
	t.Setenv("MESSAGE_PROCESSING_TIMEOUT", "1500")
	t.Setenv("RPC_CALL_TIMEOUT_MS", "2500")
	t.Setenv("STREAM_IDLE_TIMEOUT_MS", "100")
	t.Setenv("DEFAULT_PREFETCH", "8")
	t.Setenv("AMQP_HEARTBEAT_SECONDS", "5")
	t.Setenv("STREAM_MAX_TOTAL_BUFFERED_BYTES", "1024")
	t.Setenv("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "off")
	t.Setenv("SHUTDOWN_DRAIN_TIMEOUT_MS", "750")

	c := ConfigFromEnv()
	if c.BusExchange != "custom.bus" || c.ProcessingTimeout != 1500*time.Millisecond ||
		c.RPCTimeout != 2500*time.Millisecond || c.StreamIdleTimeout != 100*time.Millisecond ||
		c.DefaultPrefetch != 8 || c.Heartbeat != 5*time.Second ||
		c.StreamMaxTotalBufferedBytes != 1024 || c.ExposeInternalErrors ||
		c.ShutdownDrainTimeout != 750*time.Millisecond {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestConfigFromEnvIgnoresMalformedIntegers(t *testing.T) {
	// The TS rule: only ^\d+$, positive and in range; anything else silently
	// keeps the default. A typo must never become 0, which would make every
	// message time out at once.
	for _, raw := range []string{"", "   ", "abc", "6oo000", "123abc", "0", "-5", "1e3", "1.5", "99999999999999999999999"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("MESSAGE_PROCESSING_TIMEOUT", raw)
			t.Setenv("SHUTDOWN_DRAIN_TIMEOUT_MS", raw)
			c := ConfigFromEnv()
			if c.ProcessingTimeout != 600*time.Second {
				t.Fatalf("ProcessingTimeout = %v for %q", c.ProcessingTimeout, raw)
			}
			// Unlike TS, the shutdown budget gets the same strict parsing.
			if c.ShutdownDrainTimeout != 30*time.Second {
				t.Fatalf("ShutdownDrainTimeout = %v for %q", c.ShutdownDrainTimeout, raw)
			}
		})
	}
}

func TestConfigFromEnvTrimsWhitespace(t *testing.T) {
	t.Setenv("DEFAULT_PREFETCH", "  4 ")
	if got := ConfigFromEnv().DefaultPrefetch; got != 4 {
		t.Fatalf("got %d", got)
	}
}

func TestConfigFromEnvBooleans(t *testing.T) {
	for raw, want := range map[string]bool{
		"1": true, "true": true, "YES": true, "On": true,
		"0": false, "false": false, "no": false, "OFF": false,
		"maybe": true, "": true, // unrecognised keeps the default
	} {
		t.Setenv("PROTOBUS_EXPOSE_INTERNAL_ERRORS", raw)
		if got := ConfigFromEnv().ExposeInternalErrors; got != want {
			t.Errorf("%q -> %v, want %v", raw, got, want)
		}
	}
}

func TestConfigFromEnvEmptyExchangeNameKeepsDefault(t *testing.T) {
	t.Setenv("EVENTS_EXCHANGE_NAME", "")
	if got := ConfigFromEnv().EventsExchange; got != "proto.bus.events" {
		t.Fatalf("got %q", got)
	}
}

func TestConfigValidate(t *testing.T) {
	mutate := map[string]func(*Config){
		"empty bus exchange":       func(c *Config) { c.BusExchange = "" },
		"zero processing timeout":  func(c *Config) { c.ProcessingTimeout = 0 },
		"negative rpc timeout":     func(c *Config) { c.RPCTimeout = -time.Second },
		"zero idle timeout":        func(c *Config) { c.StreamIdleTimeout = 0 },
		"zero prefetch":            func(c *Config) { c.DefaultPrefetch = 0 },
		"prefetch beyond uint16":   func(c *Config) { c.DefaultPrefetch = 70000 },
		"zero confirm timeout":     func(c *Config) { c.PublishConfirmTimeout = 0 },
		"negative heartbeat":       func(c *Config) { c.Heartbeat = -1 },
		"zero ready timeout":       func(c *Config) { c.ConnectionReadyTimeout = 0 },
		"zero outstanding":         func(c *Config) { c.MaxOutstandingConfirms = 0 },
		"zero chunks":              func(c *Config) { c.StreamMaxBufferedChunks = 0 },
		"zero bytes":               func(c *Config) { c.StreamMaxBufferedBytes = 0 },
		"zero total":               func(c *Config) { c.StreamMaxTotalBufferedBytes = 0 },
		"too many outstanding":     func(c *Config) { c.MaxOutstandingConfirms = 1 << 20 },
		"negative retries":         func(c *Config) { c.Reconnect.MaxRetries = -1 },
		"zero initial delay":       func(c *Config) { c.Reconnect.InitialDelay = 0 },
		"max delay below initial":  func(c *Config) { c.Reconnect.MaxDelay = c.Reconnect.InitialDelay / 2 },
		"multiplier below one":     func(c *Config) { c.Reconnect.Multiplier = 0.5 },
		"exchange names collide":   func(c *Config) { c.EventsExchange = c.BusExchange },
		"negative shutdown budget": func(c *Config) { c.ShutdownDrainTimeout = -1 },
	}
	for name, m := range mutate {
		t.Run(name, func(t *testing.T) {
			c := DefaultConfig()
			m(&c)
			err := c.Validate()
			if err == nil {
				t.Fatal("expected a validation error")
			}
			if !strings.Contains(err.Error(), "protobus: invalid config") {
				t.Fatalf("error should name the problem: %v", err)
			}
		})
	}
}

func TestPriorityConstantsMatchTheOtherPorts(t *testing.T) {
	if PriorityNormal != 0 || PriorityHigh != 1 || PriorityControl != 2 || RecommendedMaxPriority != 2 {
		t.Fatal("priority levels must match protobus TS/Python Config")
	}
	if headerFinal != "x-protobus-final" || headerSeq != "x-protobus-seq" {
		t.Fatal("stream header names are wire protocol")
	}
}
