// Package integration_test runs protobus against a real RabbitMQ: the
// behaviours the in-memory fake models, checked at the source. Each test gets
// its own virtual host. See internal/brokertest for the environment.
package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/internal/brokertest"
	"github.com/ArielLaub/protobus-go/v2/internal/gentest"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// amqp091 closes its connection reader asynchronously.
		goleak.IgnoreTopFunction("github.com/rabbitmq/amqp091-go.(*Connection).reader"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

func logger() *slog.Logger {
	if os.Getenv("PROTOBUS_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func config() protobus.Config {
	c := protobus.DefaultConfig()
	c.Reconnect.InitialDelay = 50 * time.Millisecond
	c.Reconnect.MaxDelay = 500 * time.Millisecond
	c.Reconnect.MaxRetries = 0
	return c
}

func dial(t *testing.T, url string, opts ...protobus.Option) *protobus.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opts = append([]protobus.Option{protobus.WithConfig(config()), protobus.WithLogger(logger())}, opts...)
	bus, err := protobus.Dial(ctx, url, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return bus
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

// calc implements GenTest.Calc.
type calc struct {
	gentest.UnimplementedCalcServer
	attempts sync.Map
	order    chan string
	stopped  chan bool // count streams report whether they were cancelled
}

func newCalc() *calc { return &calc{order: make(chan string, 64), stopped: make(chan bool, 8)} }

func (c *calc) Add(_ context.Context, in *gentest.AddRequest) (*gentest.AddResponse, error) {
	return &gentest.AddResponse{Sum: in.A + in.B}, nil
}

func (c *calc) Double(_ context.Context, in *pbtypes.Bigint) (*pbtypes.Bigint, error) {
	v, err := in.BigInt()
	if err != nil {
		return nil, err
	}
	return pbtypes.NewBigint(v.Lsh(v, 1))
}

func (c *calc) Fail(ctx context.Context, in *gentest.FailRequest) (*gentest.AddResponse, error) {
	switch in.Mode {
	case "handled":
		return nil, protobus.NewHandledError(in.Code, in.Message)
	case "transient":
		ci, _ := protobus.CallInfoFromContext(ctx)
		v, _ := c.attempts.LoadOrStore(ci.MessageID, new(atomic.Int32))
		if v.(*atomic.Int32).Add(1) <= in.SucceedAfter {
			return nil, errors.New(in.Message)
		}
		return &gentest.AddResponse{Sum: int32(ci.Attempt)}, nil
	}
	return nil, errors.New(in.Message)
}

func (c *calc) Slow(ctx context.Context, in *gentest.SlowRequest) (*gentest.AddResponse, error) {
	c.order <- in.Tag
	select {
	case <-time.After(time.Duration(in.Ms) * time.Millisecond):
		return &gentest.AddResponse{Sum: in.Ms}, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (c *calc) Count(ctx context.Context, in *gentest.CountRequest, s protobus.ServerStream[*gentest.CountChunk]) error {
	for i := range in.N {
		if err := s.Send(&gentest.CountChunk{I: i}); err != nil {
			c.stopped <- errors.Is(context.Cause(ctx), protobus.ErrCancelled)
			return err
		}
		select {
		case <-time.After(time.Duration(in.DelayMs) * time.Millisecond):
		case <-ctx.Done():
			c.stopped <- errors.Is(context.Cause(ctx), protobus.ErrCancelled)
			return context.Cause(ctx)
		}
	}
	c.stopped <- false
	return nil
}

func serve(t *testing.T, bus *protobus.Bus, impl gentest.CalcServer, opts ...protobus.ServiceOption) *protobus.Service {
	t.Helper()
	svc, err := gentest.RegisterCalcServer(bus, impl, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx(t)); err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestRoundTripAndTopology(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL(), protobus.WithConnectionName("integration"))
	serve(t, bus, newCalc(), protobus.WithRetry(protobus.RetryPolicy{MaxRetries: 3, Delay: 5 * time.Second}))
	client := gentest.NewCalcClient(bus)
	out, err := client.Add(ctx(t), &gentest.AddRequest{A: 1, B: 2})
	if err != nil || out.Sum != 3 {
		t.Fatalf("%v %v", out, err)
	}
	d, err := client.Double(ctx(t), pbtypes.BigintFromUint64(21))
	if v, _ := d.BigInt(); err != nil || v.Int64() != 42 {
		t.Fatalf("double: %v %v", v, err)
	}

	q, ok := vh.Queue("GenTest.Calc")
	if !ok || !q.Durable || q.Exclusive || q.AutoDelete || len(q.Arguments) != 0 {
		t.Fatalf("service queue %+v", q)
	}
	vh.WaitConsumers(t, "GenTest.Calc", 1)
	retry, _ := vh.Queue("GenTest.Calc.Retry")
	if fmt.Sprint(retry.Arguments["x-message-ttl"]) != "5000" || retry.Arguments["x-dead-letter-exchange"] != "proto.bus" {
		t.Fatalf("retry queue %+v", retry)
	}
	if _, ok := vh.Queue("GenTest.Calc.DLQ"); !ok {
		t.Fatal("DLQ")
	}
	conns := vh.WaitConnections(t, 1)
	if conns[0].Timeout != 30 {
		t.Errorf("negotiated heartbeat %ds, want 30", conns[0].Timeout)
	}
	if conns[0].Properties["product"] != "protobus-go" || conns[0].Properties["connection_name"] != "integration" {
		t.Errorf("client properties %v", conns[0].Properties)
	}
}

func TestHeartbeatFromTheURLWins(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	dial(t, vh.URL()+"?heartbeat=7")
	if c := vh.WaitConnections(t, 1); c[0].Timeout != 7 {
		t.Fatalf("heartbeat %d", c[0].Timeout)
	}
}

func TestUnroutableFailsFast(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL())
	start := time.Now()
	_, err := gentest.NewCalcClient(bus).Add(ctx(t), &gentest.AddRequest{})
	if !errors.Is(err, protobus.ErrUnroutable) || time.Since(start) > 2*time.Second {
		t.Fatalf("got %v after %v", err, time.Since(start))
	}
}

func TestRetryLadderAndDeadLetterOnARealBroker(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL())
	serve(t, bus, newCalc(), protobus.WithRetry(protobus.RetryPolicy{MaxRetries: 2, Delay: 50 * time.Millisecond}))
	start := time.Now()
	_, err := gentest.NewCalcClient(bus).Fail(ctx(t), &gentest.FailRequest{Mode: "unhandled", Message: "kaput"}, protobus.WithMessageID("job-1"))
	var re *protobus.RemoteError
	if !errors.As(err, &re) || re.Message != "kaput" {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("two retry delays must have elapsed before the caller heard back")
	}
	vh.WaitQueueDepth(t, "GenTest.Calc.DLQ", 1)
	m := vh.Peek(t, "GenTest.Calc.DLQ", 1)[0]
	h := m.Properties.Headers
	if fmt.Sprint(h["x-retry-count"]) != "2" || h["x-original-queue"] != "GenTest.Calc" ||
		h["x-original-routing-key"] != "REQUEST.GenTest.Calc.fail" || h["x-last-error"] != "Error" ||
		h["x-first-failure-time"] == nil || h["x-dlq-time"] == nil || h["x-death"] == nil {
		t.Fatalf("DLQ headers %v", h)
	}
	if m.Properties.MessageID != "job-1" || m.Properties.ReplyTo != "" || m.Properties.DeliveryMode != 2 {
		t.Fatalf("DLQ properties %+v", m.Properties)
	}
}

func TestTransientFailureRecoversThroughTheRetryQueue(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL())
	serve(t, bus, newCalc(), protobus.WithRetry(protobus.RetryPolicy{MaxRetries: 3, Delay: 50 * time.Millisecond}))
	out, err := gentest.NewCalcClient(bus).Fail(ctx(t), &gentest.FailRequest{Mode: "transient", Message: "flaky", SucceedAfter: 2})
	if err != nil || out.Sum != 2 {
		t.Fatalf("%v %v", out, err)
	}
}

func TestHandledErrorOnARealBroker(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL())
	serve(t, bus, newCalc())
	_, err := gentest.NewCalcClient(bus).Fail(ctx(t), &gentest.FailRequest{Mode: "handled", Code: "NOPE", Message: "no"})
	if !protobus.IsCode(err, "NOPE") {
		t.Fatal(err)
	}
}

func TestStreamingAndCancellation(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL())
	impl := newCalc()
	serve(t, bus, impl)
	client := gentest.NewCalcClient(bus)

	var got []int32
	for c, err := range client.Count(ctx(t), &gentest.CountRequest{N: 50}) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, c.I)
	}
	if len(got) != 50 || got[49] != 49 {
		t.Fatalf("got %d chunks", len(got))
	}
	if cancelled := <-impl.stopped; cancelled {
		t.Fatal("a completed stream was not cancelled")
	}

	n := 0
	for _, err := range client.Count(ctx(t), &gentest.CountRequest{N: 10000, DelayMs: 2}) {
		if err != nil {
			t.Fatal(err)
		}
		if n++; n == 5 {
			break
		}
	}
	select {
	case cancelled := <-impl.stopped:
		if !cancelled {
			t.Fatal("the producer ended without seeing a cancellation")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the cancel notice never reached the producer")
	}
}

func TestEventsWithConfinedRetry(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL())
	svc, _ := gentest.RegisterCalcServer(bus, newCalc(), protobus.WithEventRetry(protobus.EventRetryPolicy{MaxRetries: 3, Delay: 50 * time.Millisecond}))
	var attempts atomic.Int32
	failing := make(chan int, 8)
	err := protobus.Subscribe(ctx(t), svc.Events(), func(_ context.Context, p *gentest.Ping, info protobus.EventInfo) error {
		failing <- info.Attempt
		if attempts.Add(1) == 1 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(ctx(t)); err != nil {
		t.Fatal(err)
	}
	other, _ := bus.NewEventListener("Other.Events")
	steady := make(chan string, 8)
	_ = protobus.Subscribe(ctx(t), other, func(_ context.Context, p *gentest.Ping, _ protobus.EventInfo) error {
		steady <- p.Id
		return nil
	})
	if err := other.Start(ctx(t)); err != nil {
		t.Fatal(err)
	}
	if err := bus.PublishEvent(ctx(t), &gentest.Ping{Id: "p1", N: pbtypes.BigintFromUint64(1)}); err != nil {
		t.Fatal(err)
	}
	if a := <-failing; a != 0 {
		t.Fatalf("first attempt %d", a)
	}
	select {
	case a := <-failing:
		if a != 1 {
			t.Fatalf("redelivery attempt %d", a)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no redelivery")
	}
	<-steady
	select {
	case id := <-steady:
		t.Fatalf("the redelivery reached the subscriber that had succeeded (%s)", id)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestPriorityOvertakesTheBacklog(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL())
	impl := newCalc()
	serve(t, bus, impl, protobus.WithMaxPriority(protobus.RecommendedMaxPriority), protobus.WithMaxConcurrent(1))
	client := gentest.NewCalcClient(bus)
	if q, _ := vh.Queue("GenTest.Calc"); fmt.Sprint(q.Arguments["x-max-priority"]) != "2" {
		t.Fatalf("args %v", q.Arguments)
	}

	var wg sync.WaitGroup
	call := func(tag string, p uint8, ms int32) {
		defer wg.Done()
		if _, err := client.Slow(context.Background(), &gentest.SlowRequest{Ms: ms, Tag: tag}, protobus.WithPriority(p)); err != nil {
			t.Error(err)
		}
	}
	// The first call holds the only prefetch slot while the backlog builds.
	wg.Add(1)
	go call("first", protobus.PriorityNormal, 1500)
	if tag := <-impl.order; tag != "first" {
		t.Fatal(tag)
	}
	for i := range 5 {
		wg.Add(1)
		go call(fmt.Sprintf("low%d", i), protobus.PriorityNormal, 10)
	}
	vh.WaitQueueDepth(t, "GenTest.Calc", 5)
	wg.Add(1)
	go call("urgent", protobus.PriorityControl, 10)
	vh.WaitQueueDepth(t, "GenTest.Calc", 6)
	if tag := <-impl.order; tag != "urgent" {
		t.Fatalf("served %s before the urgent call", tag)
	}
	wg.Wait()
}

func TestRetryDelayChangeIsReported(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	serve(t, dial(t, vh.URL()), newCalc(), protobus.WithRetry(protobus.RetryPolicy{MaxRetries: 1, Delay: time.Second}))
	svc, _ := gentest.RegisterCalcServer(dial(t, vh.URL()), newCalc(), protobus.WithRetry(protobus.RetryPolicy{MaxRetries: 1, Delay: 2 * time.Second}))
	if err := svc.Start(ctx(t)); !errors.Is(err, protobus.ErrRetryQueueMismatch) {
		t.Fatalf("got %v", err)
	}
}

func TestReconnectsAfterTheBrokerDropsTheConnection(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	events := make(chan protobus.ConnectionEvent, 32)
	bus := dial(t, vh.URL(), protobus.WithConnectionObserver(func(e protobus.ConnectionEvent) { events <- e }))
	serve(t, bus, newCalc())
	client := gentest.NewCalcClient(bus)
	vh.WaitConnections(t, 1)
	vh.KillConnections(t)
	for e := range events {
		if e.Kind == protobus.EventReconnected {
			break
		}
	}
	out, err := client.Add(ctx(t), &gentest.AddRequest{A: 2, B: 2})
	if err != nil || out.Sum != 4 {
		t.Fatalf("after reconnecting: %v %v", out, err)
	}
	vh.WaitConsumers(t, "GenTest.Calc", 1)
}

func TestCallsParkThroughAnOutage(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	server := dial(t, vh.URL())
	serve(t, server, newCalc())
	caller := dial(t, vh.URL())
	client := gentest.NewCalcClient(caller)
	vh.WaitConnections(t, 2)
	vh.KillConnections(t)
	out, err := client.Add(ctx(t), &gentest.AddRequest{A: 10, B: 1})
	if err != nil || out.Sum != 11 {
		t.Fatalf("a call made during the outage must complete once both sides are back: %v %v", out, err)
	}
}

func TestManyConcurrentCalls(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL())
	serve(t, bus, newCalc(), protobus.WithMaxConcurrent(64))
	client := gentest.NewCalcClient(bus)
	const n = 2000
	start := time.Now()
	var wg sync.WaitGroup
	var failures atomic.Int32
	sem := make(chan struct{}, 256)
	for i := range int32(n) {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			out, err := client.Add(context.Background(), &gentest.AddRequest{A: i, B: 1})
			if err != nil || out.Sum != i+1 {
				failures.Add(1)
			}
		})
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d of %d calls failed", failures.Load(), n)
	}
	t.Logf("%d round trips in %v (%.0f/s)", n, time.Since(start), n/time.Since(start).Seconds())
}

func TestShutdownLetsWorkFinish(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	server := dial(t, vh.URL())
	impl := newCalc()
	serve(t, server, impl)
	client := gentest.NewCalcClient(dial(t, vh.URL()))
	res := make(chan error, 1)
	go func() {
		_, err := client.Slow(context.Background(), &gentest.SlowRequest{Ms: 300, Tag: "drain"})
		res <- err
	}()
	<-impl.order
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-res; err != nil {
		t.Fatalf("the in-flight call must be answered before shutdown completes: %v", err)
	}
}

func TestStopConsumingLeavesTheQueueToOthers(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	a := dial(t, vh.URL())
	svcA := serve(t, a, newCalc())
	b := dial(t, vh.URL())
	serve(t, b, newCalc())
	if err := svcA.StopConsuming(ctx(t)); err != nil {
		t.Fatal(err)
	}
	vh.WaitConsumers(t, "GenTest.Calc", 1)
	for range 5 {
		if _, err := gentest.NewCalcClient(a).Add(ctx(t), &gentest.AddRequest{A: 1}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInstanceNamesRouteIndividually(t *testing.T) {
	vh := brokertest.Require(t).NewVHost(t)
	bus := dial(t, vh.URL())
	serve(t, bus, newCalc(), protobus.AsInstance("i1"))
	serve(t, bus, newCalc(), protobus.AsInstance("i2"))
	for _, inst := range []string{"i1", "i2"} {
		if _, err := gentest.NewCalcClient(bus, protobus.ForInstance(inst)).Add(ctx(t), &gentest.AddRequest{}); err != nil {
			t.Fatalf("%s: %v", inst, err)
		}
	}
	names := []string{}
	for _, inst := range []string{"i1", "i2"} {
		if q, ok := vh.Queue("GenTest.Calc." + inst); ok {
			names = append(names, q.Name)
		}
	}
	if !slices.Equal(names, []string{"GenTest.Calc.i1", "GenTest.Calc.i2"}) {
		t.Fatal(names)
	}
}
