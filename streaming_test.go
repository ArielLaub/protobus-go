// Integration tests for server-streaming RPC against a real RabbitMQ broker.
//
// These verify the wire-protocol guarantees documented in
// docs/advanced/streaming.md and confirm that the Go port speaks the same
// wire format as the TypeScript and Python ports (multi-message reply on
// one correlation_id with x-protobus-final on the terminal message).
//
// Run with a live broker:
//
//	docker-compose up -d
//	go test -run TestStreaming -v ./...
//
// Tests requiring RabbitMQ are skipped if the broker isn't reachable.

package protobus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 127.0.0.1 instead of localhost: Docker port-forwards only listen on IPv4
// and Go's resolver may try [::1] first.
const amqpURL = "amqp://guest:guest@127.0.0.1:5672/"

// counterService is the Go equivalent of the Counter test service used by
// the TS and Python streaming integration tests. It owns one unary method
// (add) and one streaming method (tick) covering all the paths the wire
// protocol must support: multi-chunk, empty, single, mid-stream error.
type counterService struct {
	*BaseService
}

func newCounterService(ctx *Context) *counterService {
	s := &counterService{
		BaseService: NewBaseService(ctx, "streaming_test.Counter", "", nil),
	}

	// Unary handler — verifies the streaming work didn't regress unary RPC.
	s.Handle("add", func(_ context.Context, data map[string]interface{}, _, _ string) (map[string]interface{}, error) {
		a, _ := data["a"].(float64)
		b, _ := data["b"].(float64)
		return map[string]interface{}{"sum": a + b}, nil
	})

	// Streaming handler — drives every scenario the proto exercises.
	s.HandleStream("tick", func(_ context.Context, data map[string]interface{}, _, _ string, send func(map[string]interface{}) error) error {
		count := intOf(data["count"])
		failAt := intOf(data["fail_at"])
		emitNothing, _ := data["emit_nothing"].(bool)

		if emitNothing {
			return nil
		}

		for i := 0; i < count; i++ {
			if failAt > 0 && i >= failAt {
				return NewHandledError(fmt.Sprintf("deliberate failure at chunk %d", i), "TEST_FAIL")
			}
			if err := send(map[string]interface{}{
				"seq":     float64(i),
				"payload": fmt.Sprintf("chunk-%d", i),
			}); err != nil {
				return err
			}
		}
		return nil
	})

	return s
}

// intOf coerces a JSON-decoded number (always float64) to int.
func intOf(v interface{}) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	}
	return 0
}

// brokerReachable skips the test if RabbitMQ isn't reachable. Mirrors the
// Python/TS suites which assume a docker-compose broker is up.
func brokerReachable(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:5672", 500*time.Millisecond)
	if err != nil {
		t.Skipf("RabbitMQ not reachable on localhost:5672 — skip (%v)", err)
		return
	}
	_ = conn.Close()
}

// streamingFixture spins up a Context + counterService + ServiceProxy.
// Returns a cleanup func that must be deferred.
func streamingFixture(t *testing.T) (*counterService, *ServiceProxy, func()) {
	t.Helper()
	brokerReachable(t)

	ctx := NewContext(nil)
	if err := ctx.Init(amqpURL); err != nil {
		t.Fatalf("ctx.Init: %v", err)
	}

	svc := newCounterService(ctx)
	if err := svc.Init(); err != nil {
		ctx.Close()
		t.Fatalf("svc.Init: %v", err)
	}

	proxy := NewServiceProxy(ctx, svc.ServiceName())
	if err := proxy.Init(); err != nil {
		svc.Stop()
		ctx.Close()
		t.Fatalf("proxy.Init: %v", err)
	}

	cleanup := func() {
		_ = proxy.Close()
		_ = svc.Stop()
		_ = ctx.Close()
	}
	return svc, proxy, cleanup
}

// ── Backward compat ─────────────────────────────────────────────────────────

func TestStreaming_UnaryStillWorks(t *testing.T) {
	_, proxy, cleanup := streamingFixture(t)
	defer cleanup()

	var result map[string]interface{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := proxy.Call(ctx, "add", map[string]interface{}{"a": 5.0, "b": 7.0}, &result); err != nil {
		t.Fatalf("unary call failed: %v", err)
	}
	if got := result["sum"].(float64); got != 12 {
		t.Errorf("sum = %v, want 12", got)
	}
}

// ── Happy path ──────────────────────────────────────────────────────────────

func TestStreaming_FiveChunksInOrder(t *testing.T) {
	_, proxy, cleanup := streamingFixture(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := proxy.OpenStream(ctx, "tick", map[string]interface{}{"count": 5.0})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer stream.Close()

	var chunks []map[string]interface{}
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		chunks = append(chunks, chunk)
	}

	if len(chunks) != 5 {
		t.Fatalf("len(chunks) = %d, want 5", len(chunks))
	}
	for i, c := range chunks {
		if got := c["payload"].(string); got != fmt.Sprintf("chunk-%d", i) {
			t.Errorf("chunks[%d].payload = %q, want chunk-%d", i, got, i)
		}
	}
}

func TestStreaming_SingleChunk(t *testing.T) {
	_, proxy, cleanup := streamingFixture(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := proxy.OpenStream(ctx, "tick", map[string]interface{}{"count": 1.0})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer stream.Close()

	n := 0
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		n++
	}
	if n != 1 {
		t.Errorf("n = %d, want 1", n)
	}
}

func TestStreaming_EmptyStream(t *testing.T) {
	_, proxy, cleanup := streamingFixture(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := proxy.OpenStream(ctx, "tick", map[string]interface{}{"emit_nothing": true})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer stream.Close()

	n := 0
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		n++
	}
	if n != 0 {
		t.Errorf("n = %d, want 0 chunks for empty stream", n)
	}
}

// ── Errors ──────────────────────────────────────────────────────────────────

func TestStreaming_MidStreamError(t *testing.T) {
	_, proxy, cleanup := streamingFixture(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := proxy.OpenStream(ctx, "tick", map[string]interface{}{
		"count": 10.0, "fail_at": 2.0,
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer stream.Close()

	var chunks []map[string]interface{}
	var recvErr error
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			recvErr = err
			break
		}
		chunks = append(chunks, chunk)
	}

	if len(chunks) != 2 {
		t.Errorf("expected 2 chunks before error, got %d", len(chunks))
	}
	if recvErr == nil {
		t.Fatal("expected error after partial stream, got nil")
	}
	if he, ok := GetHandledError(recvErr); !ok {
		t.Errorf("expected HandledError, got %T: %v", recvErr, recvErr)
	} else if he.Code != "TEST_FAIL" {
		t.Errorf("error code = %q, want TEST_FAIL", he.Code)
	}
}

func TestStreaming_ErrorAfterOneChunk(t *testing.T) {
	_, proxy, cleanup := streamingFixture(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := proxy.OpenStream(ctx, "tick", map[string]interface{}{
		"count": 10.0, "fail_at": 1.0,
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer stream.Close()

	n := 0
	var recvErr error
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			recvErr = err
			break
		}
		n++
	}
	if n != 1 {
		t.Errorf("expected 1 chunk before error, got %d", n)
	}
	if recvErr == nil {
		t.Fatal("expected error, got nil")
	}
}

// ── Early termination ───────────────────────────────────────────────────────

func TestStreaming_CloseReleasesSlot(t *testing.T) {
	_, proxy, cleanup := streamingFixture(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := proxy.OpenStream(ctx, "tick", map[string]interface{}{"count": 100.0})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	// Consume a few chunks then close early.
	for i := 0; i < 3; i++ {
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("Recv[%d]: %v", i, err)
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Give the dispatcher a moment to react.
	time.Sleep(50 * time.Millisecond)

	proxy.mu.RLock()
	left := len(proxy.pendingStreams)
	proxy.mu.RUnlock()
	if left != 0 {
		t.Errorf("pendingStreams = %d after Close, want 0", left)
	}
}

// ── Concurrent streams ──────────────────────────────────────────────────────

func TestStreaming_TwoConcurrentStreams(t *testing.T) {
	_, proxy, cleanup := streamingFixture(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	consume := func(req map[string]interface{}) ([]string, error) {
		stream, err := proxy.OpenStream(ctx, "tick", req)
		if err != nil {
			return nil, err
		}
		defer stream.Close()

		var payloads []string
		for {
			chunk, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return payloads, nil
			}
			if err != nil {
				return payloads, err
			}
			payloads = append(payloads, chunk["payload"].(string))
		}
	}

	var (
		wg               sync.WaitGroup
		aOK, bOK         atomic.Bool
		aResult, bResult []string
		aErr, bErr       error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		aResult, aErr = consume(map[string]interface{}{"count": 5.0})
		aOK.Store(aErr == nil)
	}()
	go func() {
		defer wg.Done()
		bResult, bErr = consume(map[string]interface{}{"count": 8.0})
		bOK.Store(bErr == nil)
	}()
	wg.Wait()

	if !aOK.Load() {
		t.Fatalf("stream a failed: %v", aErr)
	}
	if !bOK.Load() {
		t.Fatalf("stream b failed: %v", bErr)
	}
	if len(aResult) != 5 {
		t.Errorf("stream a: len=%d, want 5", len(aResult))
	}
	if len(bResult) != 8 {
		t.Errorf("stream b: len=%d, want 8", len(bResult))
	}
	for i, p := range aResult {
		if p != fmt.Sprintf("chunk-%d", i) {
			t.Errorf("stream a[%d] = %q", i, p)
		}
	}
	for i, p := range bResult {
		if p != fmt.Sprintf("chunk-%d", i) {
			t.Errorf("stream b[%d] = %q", i, p)
		}
	}
}
