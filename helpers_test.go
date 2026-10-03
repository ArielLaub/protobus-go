package protobus

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fastConfig is DefaultConfig with every wait shortened for tests.
func fastConfig() Config {
	c := DefaultConfig()
	c.Reconnect.InitialDelay = 5 * time.Millisecond
	c.Reconnect.MaxDelay = 20 * time.Millisecond
	c.ConnectionReadyTimeout = 2 * time.Second
	c.PublishConfirmTimeout = 2 * time.Second
	c.RPCTimeout = 5 * time.Second
	c.StreamIdleTimeout = 5 * time.Second
	c.ProcessingTimeout = 5 * time.Second
	return c
}

func testLogger(t testing.TB) *slog.Logger {
	if os.Getenv("PROTOBUS_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testCtx(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// eventually polls cond until it holds or the deadline passes.
func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func recvWithin[T any](t testing.TB, c <-chan T, d time.Duration) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(d):
		t.Fatalf("nothing received within %v", d)
	}
	var zero T
	return zero
}
