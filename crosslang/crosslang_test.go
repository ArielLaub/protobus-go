// Package crosslang_test checks protobus-go against the TypeScript and Python
// ports over a real broker, in both directions: the Go client against TS and
// Python servers, their clients against a Go server, and replicas of one
// service in all three languages sharing a queue and its retry ladder.
//
// It needs the broker of internal/brokertest, plus the sibling checkouts:
// PROTOBUS_TS (default ../protobus, built with `npm run build`) and
// PROTOBUS_PY (default ../protobus-py, with a venv/). A missing peer skips its
// tests rather than failing them.
package crosslang_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/crosslang/gen/interop"
	"github.com/ArielLaub/protobus-go/v2/crosslang/gopeer"
	"github.com/ArielLaub/protobus-go/v2/internal/brokertest"
)

func here() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}

func siblings(env, def string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return filepath.Join(here(), "..", "..", def)
}

// peer describes how to run one language's participant.
type peer struct {
	lang string
	cmd  func(mode string) (*exec.Cmd, error)
}

var peers = []peer{
	{lang: "ts", cmd: func(mode string) (*exec.Cmd, error) {
		ts := siblings("PROTOBUS_TS", "protobus")
		if _, err := os.Stat(filepath.Join(ts, "dist", "lib", "context.js")); err != nil {
			return nil, fmt.Errorf("TypeScript protobus not built at %s (set PROTOBUS_TS)", ts)
		}
		node, err := exec.LookPath("node")
		if err != nil {
			return nil, err
		}
		c := exec.Command(node, filepath.Join(here(), "peers", "ts", "peer.js"), mode)
		c.Env = append(os.Environ(), "PROTOBUS_TS="+ts)
		return c, nil
	}},
	{lang: "py", cmd: func(mode string) (*exec.Cmd, error) {
		py := siblings("PROTOBUS_PY", "protobus-py")
		interp := filepath.Join(py, "venv", "bin", "python")
		if _, err := os.Stat(interp); err != nil {
			return nil, fmt.Errorf("protobus-py venv not found at %s (set PROTOBUS_PY)", py)
		}
		c := exec.Command(interp, filepath.Join(here(), "peers", "py", "peer.py"), mode)
		c.Env = append(os.Environ(), "PYTHONPATH="+py)
		return c, nil
	}},
}

func peerEnv(c *exec.Cmd, amqpURL, target string) {
	c.Env = append(c.Env, "PROTOBUS_TEST_AMQP="+amqpURL, "PROTOBUS_TEST_PROTO_DIR="+filepath.Join(here(), "proto"),
		"PEER_TARGET="+target)
}

// peerUnavailable skips a test whose peer is missing, unless the peer was
// configured explicitly (as CI does): then its absence is a failure.
func peerUnavailable(t *testing.T, lang string, err error) {
	t.Helper()
	if os.Getenv(map[string]string{"ts": "PROTOBUS_TS", "py": "PROTOBUS_PY"}[lang]) != "" {
		t.Fatal(err)
	}
	t.Skip(err)
}

// startServer runs a peer's server until the test ends.
func startServer(t *testing.T, p peer, amqpURL string) {
	t.Helper()
	c, err := p.cmd("server")
	if err != nil {
		peerUnavailable(t, p.lang, err)
	}
	peerEnv(c, amqpURL, p.lang)
	var stderr syncBuffer
	c.Stderr = &stderr
	stdout, err := c.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "READY" {
				ready <- nil
				_, _ = io.Copy(io.Discard, stdout)
				return
			}
		}
		ready <- errors.New("server exited before READY")
	}()
	t.Cleanup(func() {
		_ = c.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = c.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = c.Process.Kill()
			<-done
		}
	})
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("%s server: %v\n%s", p.lang, err, stderr.String())
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("%s server did not become ready\n%s", p.lang, stderr.String())
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func logger() *slog.Logger {
	if os.Getenv("PROTOBUS_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func dial(t *testing.T, url string) *protobus.Bus {
	t.Helper()
	cfg := protobus.DefaultConfig()
	cfg.RPCTimeout = 20 * time.Second
	cfg.StreamIdleTimeout = 20 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bus, err := protobus.Dial(ctx, url, protobus.WithConfig(cfg), protobus.WithLogger(logger()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return bus
}

func callCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func serveGo(t *testing.T, bus *protobus.Bus) {
	t.Helper()
	if _, err := gopeer.Serve(callCtx(t), bus); err != nil {
		t.Fatal(err)
	}
}

// ---- the Go client against every server ----------------------------------------

func TestGoClient(t *testing.T) {
	broker := brokertest.Require(t)
	targets := append([]peer{{lang: "go"}}, peers...)
	for _, target := range targets {
		t.Run("against "+target.lang, func(t *testing.T) {
			vh := broker.NewVHost(t)
			if target.lang == "go" {
				serveGo(t, dial(t, vh.URL()))
			} else {
				startServer(t, target, vh.URL())
			}
			goClientScenario(t, dial(t, vh.URL()), target.lang)
		})
	}
}

func goClientScenario(t *testing.T, bus *protobus.Bus, target string) {
	counter := interop.NewCounterClient(bus)
	wallet := interop.NewWalletClient(bus)

	t.Run("unary", func(t *testing.T) {
		out, err := counter.Add(callCtx(t), &interop.AddRequest{A: 2, B: 3})
		if err != nil || out.Sum != 5 {
			t.Fatalf("%v %v", out, err)
		}
	})
	t.Run("priority on a plain queue", func(t *testing.T) {
		if _, err := counter.Add(callCtx(t), &interop.AddRequest{A: 1}, protobus.WithPriority(2)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("stream in order", func(t *testing.T) {
		var seqs []int32
		for tick, err := range counter.Tick(callCtx(t), &interop.TickRequest{Count: 5}) {
			if err != nil {
				t.Fatal(err)
			}
			if tick.Payload != fmt.Sprintf("chunk-%d", tick.Seq) {
				t.Fatalf("payload %q", tick.Payload)
			}
			seqs = append(seqs, tick.Seq)
		}
		if fmt.Sprint(seqs) != "[0 1 2 3 4]" {
			t.Fatal(seqs)
		}
	})
	t.Run("empty stream", func(t *testing.T) {
		for _, err := range counter.Tick(callCtx(t), &interop.TickRequest{EmitNothing: true}) {
			t.Fatalf("unexpected item or error: %v", err)
		}
	})
	t.Run("mid-stream handled error", func(t *testing.T) {
		n := 0
		var last error
		for _, err := range counter.Tick(callCtx(t), &interop.TickRequest{Count: 10, FailAt: 2}) {
			if err != nil {
				last = err
				break
			}
			n++
		}
		var re *protobus.RemoteError
		if n != 2 || !errors.As(last, &re) || re.Code != "TEST_FAIL" || !strings.Contains(re.Message, "deliberate failure at chunk 2") {
			t.Fatalf("%d chunks, %v", n, last)
		}
	})
	t.Run("mid-stream unhandled error", func(t *testing.T) {
		var last error
		for _, err := range counter.Tick(callCtx(t), &interop.TickRequest{Count: 10, FailAt: 1, Unhandled: true}) {
			last = err
		}
		var re *protobus.RemoteError
		if !errors.As(last, &re) || re.Message != "stream broke" {
			t.Fatalf("%v", last)
		}
	})
	t.Run("cancellation reaches the producer", func(t *testing.T) {
		n := 0
		for _, err := range counter.Tick(callCtx(t), &interop.TickRequest{Count: 500, DelayMs: 10}) {
			if err != nil {
				t.Fatal(err)
			}
			if n++; n == 3 {
				break
			}
		}
		var p *interop.Produced
		for range 100 {
			var err error
			if p, err = counter.Produced(callCtx(t), &interop.Nothing{}); err != nil {
				t.Fatal(err)
			}
			if p.StoppedEarly {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !p.StoppedEarly || p.Finished || p.Yielded >= 500 {
			t.Fatalf("the %s producer never saw the cancellation: %v", target, p)
		}
	})
	t.Run("custom types, defaults and maps", func(t *testing.T) {
		got, err := wallet.Balance(callCtx(t), &interop.Query{Account: "acc"})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(gopeer.Canonical(), got, protocmp.Transform()); diff != "" {
			t.Fatalf("balance from %s differs (-want +got):\n%s", target, diff)
		}
	})
	t.Run("echo round trip", func(t *testing.T) {
		got, err := wallet.Echo(callCtx(t), gopeer.Canonical())
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(gopeer.Canonical(), got, protocmp.Transform()); diff != "" {
			t.Fatalf("echo through %s differs (-want +got):\n%s", target, diff)
		}
	})
	t.Run("handled error", func(t *testing.T) {
		_, err := wallet.Balance(callCtx(t), &interop.Query{Account: "boom"})
		var re *protobus.RemoteError
		if !errors.As(err, &re) || re.Code != "NOT_FOUND" || re.Message != "no such account" {
			t.Fatalf("%#v", err)
		}
	})
	t.Run("unhandled error", func(t *testing.T) {
		_, err := wallet.Balance(callCtx(t), &interop.Query{Account: "crash"})
		var re *protobus.RemoteError
		if !errors.As(err, &re) || re.Message != "kaboom" {
			t.Fatalf("%#v", err)
		}
	})
	t.Run("unimplemented method", func(t *testing.T) {
		_, err := counter.Unimplemented(callCtx(t), &interop.Nothing{})
		if !protobus.IsCode(err, protobus.CodeProtocol) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("call metadata", func(t *testing.T) {
		who, err := counter.Whoami(callCtx(t), &interop.Nothing{}, protobus.WithActor("client-go"), protobus.WithMessageID("mid-go-1"))
		if err != nil {
			t.Fatal(err)
		}
		want := &interop.Who{Actor: "client-go", MessageId: "mid-go-1", RoutingKey: "REQUEST.interop.Counter.whoami", Lang: target}
		if !proto.Equal(who, want) {
			t.Fatalf("got %v want %v", who, want)
		}
	})
	t.Run("instance routing", func(t *testing.T) {
		who, err := interop.NewCounterClient(bus, protobus.WithInstance("inst1")).Whoami(callCtx(t), &interop.Nothing{})
		if err != nil || who.RoutingKey != "REQUEST.interop.Counter.inst1.whoami" {
			t.Fatalf("%v %v", who, err)
		}
	})
	t.Run("events both ways", func(t *testing.T) {
		l, err := bus.NewEventListener("")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = l.Close() }()
		got := make(chan *interop.Ping, 1)
		err = protobus.Subscribe(callCtx(t), l, func(_ context.Context, p *interop.Ping, _ protobus.EventInfo) error {
			got <- p
			return nil
		}, protobus.WithTopic("EVENT.pong."+target))
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Start(callCtx(t)); err != nil {
			t.Fatal(err)
		}
		n, _ := new(big.Int).SetString("1180591620717411303424", 10) // 2^70
		if err := bus.PublishEvent(callCtx(t), &interop.Ping{Id: "go-1", N: mustBigint(n), From: "go"}, protobus.WithTopic("EVENT.ping."+target)); err != nil {
			t.Fatal(err)
		}
		select {
		case p := <-got:
			v, _ := p.N.BigInt()
			if p.Id != "pong:go-1" || p.From != target || v.Cmp(new(big.Int).Add(n, big.NewInt(1))) != 0 {
				t.Fatalf("pong %v (n=%s)", p, v)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no pong")
		}
	})
}

// ---- every other language's client against a Go server ------------------------

func TestPeerClientsAgainstGoServer(t *testing.T) {
	broker := brokertest.Require(t)
	for _, client := range peers {
		t.Run(client.lang+" client", func(t *testing.T) {
			c, err := client.cmd("client")
			if err != nil {
				peerUnavailable(t, client.lang, err)
			}
			vh := broker.NewVHost(t)
			serveGo(t, dial(t, vh.URL()))
			peerEnv(c, vh.URL(), "go")
			var stderr bytes.Buffer
			c.Stderr = &stderr
			out, runErr := c.Output()
			passed := 0
			for _, line := range strings.Split(string(out), "\n") {
				switch {
				case strings.HasPrefix(line, "PASS "):
					passed++
					t.Run(strings.TrimPrefix(line, "PASS "), func(*testing.T) {})
				case strings.HasPrefix(line, "FAIL "):
					name, reason, _ := strings.Cut(strings.TrimPrefix(line, "FAIL "), ": ")
					t.Run(name, func(t *testing.T) { t.Error(strings.ReplaceAll(reason, " | ", "\n")) })
				}
			}
			if !strings.Contains(string(out), "DONE") || runErr != nil && passed == 0 {
				t.Fatalf("%s client did not finish: %v\nstdout:\n%s\nstderr:\n%s", client.lang, runErr, out, stderr.String())
			}
			if passed < 15 {
				t.Errorf("only %d checks passed", passed)
			}
		})
	}
}

func mustBigint(v *big.Int) *interopBigint {
	b, err := newBigint(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ---- replicas in three languages share one service -------------------------

// TestMixedReplicasShareOneRetryLadder runs interop.Flaky in Go, TypeScript
// and Python at once, competing on one queue. Every attempt fails, so each
// message climbs the retry ladder across replicas of different languages:
// the x-retry-count one writes is read by the others, the queue arguments
// each declares must be equivalent to the rest's, and the message must end
// in the dead-letter queue exactly once, after exactly MaxRetries retries.
func TestMixedReplicasShareOneRetryLadder(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	for _, p := range peers {
		startServer(t, p, vh.URL()) // skips the test if a peer is unavailable
	}
	serveGo(t, dial(t, vh.URL()))

	bus := dial(t, vh.URL())
	attempts := make(chan *interop.Attempted, 256)
	l, err := bus.NewEventListener("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	err = protobus.Subscribe(callCtx(t), l, func(_ context.Context, a *interop.Attempted, _ protobus.EventInfo) error {
		attempts <- a
		return nil
	}, protobus.WithTopic("EVENT.attempted"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(callCtx(t)); err != nil {
		t.Fatal(err)
	}

	const messages = 9
	retries := gopeer.FlakyRetry.MaxRetries
	client := interop.NewFlakyClient(bus)
	var wg sync.WaitGroup
	for i := range messages {
		wg.Go(func() {
			_, err := client.Fail(callCtx(t), &interop.FailRequest{Id: fmt.Sprint(i)}, protobus.WithMessageID(fmt.Sprintf("flaky-%d", i)))
			var re *protobus.RemoteError
			if !errors.As(err, &re) || !strings.HasPrefix(re.Message, "flaky ") {
				t.Errorf("message %d: the caller must be answered with the final failure, got %v", i, err)
			}
		})
	}
	wg.Wait()

	perMessage := map[string]int{}
	langs := map[string]int{}
	deadline := time.After(10 * time.Second)
	for total := 0; total < messages*(retries+1); {
		select {
		case a := <-attempts:
			perMessage[a.MessageId]++
			langs[a.Lang]++
			total++
		case <-deadline:
			t.Fatalf("saw %d attempts, want %d: %v", total, messages*(retries+1), perMessage)
		}
	}
	for id, n := range perMessage {
		if n != retries+1 {
			t.Errorf("%s was attempted %d times, want %d", id, n, retries+1)
		}
	}
	t.Logf("attempts by language: %v", langs)
	if len(langs) < 2 {
		t.Errorf("the ladder never crossed languages (%v); the test proves nothing", langs)
	}

	vh.WaitQueueDepth(t, "interop.Flaky.DLQ", messages)
	seen := map[string]bool{}
	for _, m := range vh.Peek(t, "interop.Flaky.DLQ", messages) {
		h := m.Properties.Headers
		if fmt.Sprint(h["x-retry-count"]) != fmt.Sprint(retries) || h["x-original-queue"] != "interop.Flaky" ||
			h["x-original-routing-key"] != "REQUEST.interop.Flaky.fail" {
			t.Errorf("DLQ headers %v", h)
		}
		if last, _ := h["x-last-error"].(string); last == "" || strings.Contains(last, "flaky") {
			t.Errorf("x-last-error must name the error class, never its message: %q", last)
		}
		seen[m.Properties.MessageID] = true
	}
	if len(seen) != messages {
		t.Fatalf("the DLQ holds %d distinct messages, want %d (each exactly once)", len(seen), messages)
	}
}
