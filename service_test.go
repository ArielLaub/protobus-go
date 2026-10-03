package protobus

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
	"github.com/ArielLaub/protobus-go/v2/internal/transport"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

// rawPeer publishes hand-built requests and collects replies, standing in for
// a peer on another port (or a hostile publisher).
type rawPeer struct {
	t       *testing.T
	ch      transport.Channel
	replyTo string
	mu      sync.Mutex
	replies map[string][]amqp.Delivery
	signal  chan struct{}
}

func newRawPeer(t *testing.T, b *fakebroker.Broker) *rawPeer {
	t.Helper()
	conn := b.DialPeer()
	ch, _ := conn.Channel()
	_ = ch.ExchangeDeclare("proto.bus", "topic", true, false, false, false, nil)
	_ = ch.ExchangeDeclare("proto.bus.callback", "direct", true, false, false, false, nil)
	q, _ := ch.QueueDeclare("", false, true, true, false, nil)
	_ = ch.QueueBind(q.Name, q.Name, "proto.bus.callback", false, nil)
	ds, _ := ch.ConsumeWithContext(context.Background(), q.Name, "", true, true, false, false, nil)
	p := &rawPeer{t: t, ch: ch, replyTo: q.Name, replies: map[string][]amqp.Delivery{}, signal: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for d := range ds {
			p.mu.Lock()
			p.replies[d.CorrelationId] = append(p.replies[d.CorrelationId], d)
			p.mu.Unlock()
			select {
			case p.signal <- struct{}{}:
			default:
			}
		}
	}()
	t.Cleanup(func() { _ = conn.Close(); <-done })
	return p
}

func (p *rawPeer) send(rk string, msg amqp.Publishing) string {
	p.t.Helper()
	if msg.CorrelationId == "" {
		msg.CorrelationId = fmt.Sprintf("cid-%d", time.Now().UnixNano())
	}
	if msg.ReplyTo == "" {
		msg.ReplyTo = p.replyTo
	}
	if err := p.ch.PublishWithContext(context.Background(), "proto.bus", rk, false, false, msg); err != nil {
		p.t.Fatal(err)
	}
	return msg.CorrelationId
}

func (p *rawPeer) request(method string, m proto.Message) []byte {
	data, _ := proto.Marshal(m)
	return wire.AppendRequest(nil, wire.Request{Method: method, Data: data})
}

// await waits for n replies to cid.
func (p *rawPeer) await(cid string, n int) []amqp.Delivery {
	p.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		p.mu.Lock()
		got := slices.Clone(p.replies[cid])
		p.mu.Unlock()
		if len(got) >= n {
			return got
		}
		select {
		case <-p.signal:
		case <-deadline:
			p.t.Fatalf("waited for %d replies to %s, got %d", n, cid, len(got))
		}
	}
}

func decodeErrorReply(t *testing.T, body []byte) *wire.Error {
	t.Helper()
	resp, err := wire.DecodeResponse(body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil {
		t.Fatalf("expected an error reply, got result for %s", resp.Result.Method)
	}
	return resp.Error
}

func noRetryCfg() []ServiceOption { return []ServiceOption{WithRetry(RetryPolicy{})} }

func fastRetry(n int) ServiceOption {
	return WithRetry(RetryPolicy{MaxRetries: n, Delay: 10 * time.Millisecond})
}

func TestServiceUnaryRoundTrip(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	out, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{A: 20, B: 22})
	if err != nil || out.Sum != 42 {
		t.Fatalf("got %v, %v", out, err)
	}
}

func TestServiceRoundTripsEveryFieldKind(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	amount, _ := new(big.Int).SetString("1000000000000000000000000000000", 10)
	in := &testpb.Order{
		Id: "o-1", Amount: pbtypes.MustBigint(amount), PlacedAt: pbtypes.NewTimestamp(time.UnixMilli(1577836800000)),
		Big: 9007199254740993, Tags: []string{"a", "b"}, Counts: map[string]int32{"x": 1, "y": 0},
		Balances: map[string]*pbtypes.Bigint{"k": pbtypes.BigintFromUint64(1)},
		Parts:    []*pbtypes.Bigint{pbtypes.BigintFromUint64(1), pbtypes.BigintFromUint64(2)},
		Status:   testpb.Status_STATUS_CLOSED,
	}
	out, err := newCalcClient(bus).Echo(testCtx(t), in)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(in, out, protocmp.Transform()); diff != "" {
		t.Fatalf("round trip changed the message (-in +out):\n%s", diff)
	}
}

func TestServiceHandledErrorIsAnsweredNotRetried(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), fastRetry(3))
	_, err := newCalcClient(bus).Fail(testCtx(t), &testpb.FailRequest{Mode: "handled", Code: "VALIDATION", Message: "name is required"})
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != "VALIDATION" || re.Message != "name is required" {
		t.Fatalf("got %#v", err)
	}
	if len(b.OpsOf("expire")) != 0 || b.QueueDepth("Test.Calc.Retry") != 0 {
		t.Fatal("a handled error must not touch the retry ladder")
	}
	eventually(t, "acked", func() bool { return len(b.OpsOf("ack")) >= 1 })
}

func TestServiceHandledErrorDefaultCode(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	_, err := newCalcClient(bus).Fail(testCtx(t), &testpb.FailRequest{Mode: "handled", Message: "nope"})
	if !IsCode(err, CodeHandled) {
		t.Fatalf("got %v (code %q)", err, ErrorCode(err))
	}
}

func TestServiceUnhandledErrorClimbsTheRetryLadderThenDeadLetters(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), fastRetry(2))
	peer := newRawPeer(t, b)

	cid := peer.send("REQUEST.Test.Calc.fail", amqp.Publishing{
		Body:        peer.request("Test.Calc.fail", &testpb.FailRequest{Mode: "unhandled", Message: "db password=hunter2 rejected"}),
		ContentType: "application/x-custom", ContentEncoding: "identity", Type: "t", AppId: "billing",
		MessageId: "logical-1", Priority: 1, Timestamp: time.Unix(1700000000, 0), DeliveryMode: amqp.Persistent,
		Expiration: "60000", Headers: amqp.Table{"x-trace": "abc"},
	})
	replies := peer.await(cid, 1)
	er := decodeErrorReply(t, replies[0].Body)
	if er.Message != "db password=hunter2 rejected" || er.Method != "Test.Calc.fail" {
		t.Fatalf("unexpected terminal reply %+v", er)
	}

	// The reply goes out first, the DLQ copy right after it.
	eventually(t, "the DLQ copy", func() bool { return b.QueueDepth("Test.Calc.DLQ") == 1 })
	retries := b.OpsOf("publish")
	var hops, dlq []fakebroker.Op
	for _, op := range retries {
		switch {
		case op.Exchange == "Test.Calc.Retry.Exchange":
			hops = append(hops, op)
		case op.Exchange == "" && op.Key == "Test.Calc.DLQ":
			dlq = append(dlq, op)
		}
	}
	if len(hops) != 2 || len(dlq) != 1 {
		t.Fatalf("want 2 retry hops and 1 DLQ copy, got %d and %d", len(hops), len(dlq))
	}
	first := hops[0].Msg.Headers[headerFirstFailure]
	for i, hop := range hops {
		h := hop.Msg.Headers
		if hop.Key != "REQUEST.Test.Calc.fail" || !hop.Mandatory {
			t.Errorf("hop %d routed %q mandatory=%v", i, hop.Key, hop.Mandatory)
		}
		if n, _ := headerInt(h[headerRetryCount]); n != int64(i+1) {
			t.Errorf("hop %d x-retry-count %v", i, h[headerRetryCount])
		}
		if h[headerOriginalKey] != "REQUEST.Test.Calc.fail" || h["x-trace"] != "abc" {
			t.Errorf("hop %d headers %v", i, h)
		}
		if h[headerFirstFailure] != first {
			t.Errorf("x-first-failure-time must carry forward unchanged")
		}
		if strings.Contains(fmt.Sprint(h[headerLastError]), "hunter2") || h[headerLastError] != "Error" {
			t.Errorf("x-last-error must name the error, never quote it: %v", h[headerLastError])
		}
		m := hop.Msg
		if m.MessageId != "logical-1" || m.CorrelationId != cid || m.ReplyTo != peer.replyTo {
			t.Errorf("hop %d identity %q/%q/%q", i, m.MessageId, m.CorrelationId, m.ReplyTo)
		}
		if m.ContentType != "application/x-custom" || m.ContentEncoding != "identity" || m.Type != "t" ||
			m.AppId != "billing" || m.Priority != 1 || !m.Timestamp.Equal(time.Unix(1700000000, 0)) {
			t.Errorf("hop %d dropped a carried property: %+v", i, m)
		}
		if m.Expiration != "" || m.DeliveryMode != amqp.Persistent {
			t.Errorf("hop %d: expiration must be dropped and delivery made persistent", i)
		}
	}
	d := dlq[0].Msg
	if n, _ := headerInt(d.Headers[headerRetryCount]); n != 2 {
		t.Errorf("the DLQ copy records the count it arrived with: %v", d.Headers[headerRetryCount])
	}
	if d.Headers[headerOriginalQueue] != "Test.Calc" || d.Headers[headerDeadLetterTime] == nil {
		t.Errorf("DLQ metadata %v", d.Headers)
	}
	if d.ReplyTo != "" || d.MessageId != "logical-1" || d.ContentType != "application/x-custom" {
		t.Errorf("DLQ copy %+v", d)
	}
	if b.QueueDepth("Test.Calc.DLQ") != 1 {
		t.Fatal("the message must rest in the DLQ")
	}

	// Ordering on the terminal attempt: error reply, DLQ copy, then ack.
	ops := b.Ops()
	idx := func(pred func(fakebroker.Op) bool) int { return slices.IndexFunc(ops, pred) }
	reply := idx(func(o fakebroker.Op) bool { return o.Kind == "publish" && o.Exchange == "proto.bus.callback" })
	dl := idx(func(o fakebroker.Op) bool { return o.Kind == "publish" && o.Key == "Test.Calc.DLQ" })
	eventually(t, "final ack", func() bool { return len(b.OpsOf("ack")) == 3 })
	ops = b.Ops()
	ackAfterDLQ := slices.ContainsFunc(ops[dl+1:], func(o fakebroker.Op) bool { return o.Kind == "ack" })
	if !(reply < dl) || !ackAfterDLQ {
		t.Fatalf("terminal order must be reply (%d), DLQ copy (%d), then ack", reply, dl)
	}
}

func TestServiceTransientFailureSucceedsOnRetry(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), fastRetry(3))
	out, err := newCalcClient(bus).Fail(testCtx(t), &testpb.FailRequest{Mode: "transient", Message: "flaky", SucceedAfter: 2})
	if err != nil {
		t.Fatal(err)
	}
	if out.Sum != 2 {
		t.Fatalf("the success came on attempt %d, want 2 (CallInfo.Attempt)", out.Sum)
	}
	if b.QueueDepth("Test.Calc.DLQ") != 0 {
		t.Fatal("nothing dead-lettered")
	}
}

func TestServiceHidesInternalErrorsWhenExposureIsOff(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.ExposeInternalErrors = false
	bus := dialTest(t, b, cfg)
	startCalc(t, bus, newCalcImpl(), noRetryCfg()...)
	c := newCalcClient(bus)
	_, err := c.Fail(testCtx(t), &testpb.FailRequest{Mode: "unhandled", Message: "secret detail"})
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != CodeInternal || re.Message != "internal service error" {
		t.Fatalf("got %#v", err)
	}
	_, err = c.Fail(testCtx(t), &testpb.FailRequest{Mode: "handled", Code: "V", Message: "visible"})
	if !errors.As(err, &re) || re.Message != "visible" {
		t.Fatalf("a HandledError crosses regardless: %#v", err)
	}
}

func TestServiceWithoutRetriesRejectsAndAnswers(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), noRetryCfg()...)
	_, err := newCalcClient(bus).Fail(testCtx(t), &testpb.FailRequest{Mode: "unhandled", Message: "boom"})
	var re *RemoteError
	if !errors.As(err, &re) || re.Message != "boom" {
		t.Fatalf("got %v", err)
	}
	eventually(t, "reject", func() bool { return len(b.OpsOf("reject")) == 1 })
	if b.HasQueue("Test.Calc.Retry") || b.HasQueue("Test.Calc.DLQ") {
		t.Fatal("no retry topology without retries")
	}
	if r := b.OpsOf("reject")[0]; r.Requeue {
		t.Fatal("rejected without requeue, so it cannot loop")
	}
}

func TestServiceSurvivesAPanickingHandler(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), noRetryCfg()...)
	c := newCalcClient(bus)
	_, err := c.Fail(testCtx(t), &testpb.FailRequest{Mode: "panic", Message: "kaboom"})
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("a panic is answered as a failure: %v", err)
	}
	if out, err := c.Add(testCtx(t), &testpb.AddRequest{A: 1, B: 1}); err != nil || out.Sum != 2 {
		t.Fatal("the service must keep serving after a panic")
	}
}

func TestServiceProcessingTimeoutAnswersTheCaller(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl, noRetryCfg()[0], WithProcessingTimeout(30*time.Millisecond))
	_, err := newCalcClient(bus).Slow(testCtx(t), &testpb.SlowRequest{Ms: 300, IgnoreCancel: true})
	if !IsCode(err, CodeProcessingTimeout) {
		t.Fatalf("got %v", err)
	}
	// The abandoned handler is still counted until it actually returns.
	if bus.InFlight() == 0 {
		t.Fatal("a handler outliving its timeout must still count as in flight")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := bus.Drain(ctx); err != nil {
		t.Fatalf("drain must wait for the abandoned handler: %v", err)
	}
	recvWithin(t, impl.slowDone, time.Second)
}

func TestServiceProcessingTimeoutCancelsTheHandlerContext(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl, noRetryCfg()[0], WithProcessingTimeout(30*time.Millisecond))
	start := time.Now()
	_, _ = newCalcClient(bus).Slow(testCtx(t), &testpb.SlowRequest{Ms: 5000})
	recvWithin(t, impl.slowDone, time.Second)
	if time.Since(start) > 2*time.Second {
		t.Fatal("a cooperative handler must stop when its context is cancelled")
	}
}

func TestServiceRepliesBeforeAcknowledging(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	if _, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "ack", func() bool { return len(b.OpsOf("ack")) == 1 })
	ops := b.Ops()
	reply := slices.IndexFunc(ops, func(o fakebroker.Op) bool { return o.Kind == "publish" && o.Exchange == "proto.bus.callback" })
	ack := slices.IndexFunc(ops, func(o fakebroker.Op) bool { return o.Kind == "ack" })
	if reply < 0 || ack < reply {
		t.Fatalf("the reply (op %d) must be published before the ack (op %d)", reply, ack)
	}
}

func TestServiceRequeuesWhenTheReplyCannotBePublished(t *testing.T) {
	old := requeueDelay
	requeueDelay = 10 * time.Millisecond
	t.Cleanup(func() { requeueDelay = old })

	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	var mu sync.Mutex
	refused := 0
	b.SetConfirmPolicy(func(p fakebroker.Published) fakebroker.ConfirmAction {
		mu.Lock()
		defer mu.Unlock()
		if p.Exchange == "proto.bus.callback" && refused == 0 {
			refused++
			return fakebroker.Nack
		}
		return fakebroker.Ack
	})
	out, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{A: 2, B: 2})
	if err != nil || out.Sum != 4 {
		t.Fatalf("the requeued request must be served again: %v %v", out, err)
	}
	nacks := b.OpsOf("nack")
	if len(nacks) != 1 || !nacks[0].Requeue {
		t.Fatalf("expected one requeue, got %+v", nacks)
	}
}

func TestServiceRunsHandlersInParallel(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl, WithMaxConcurrent(8))
	c := newCalcClient(bus)
	start := time.Now()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := c.Slow(context.Background(), &testpb.SlowRequest{Ms: 150}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 900*time.Millisecond {
		t.Fatalf("8 x 150ms handlers took %v: they did not run in parallel", elapsed)
	}
	if impl.maxSeen.Load() != 8 {
		t.Fatalf("peak concurrency %d, want 8", impl.maxSeen.Load())
	}
}

func TestServicePrefetchBoundsConcurrency(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl, WithMaxConcurrent(2))
	c := newCalcClient(bus)
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() { _, _ = c.Slow(context.Background(), &testpb.SlowRequest{Ms: 30}) })
	}
	wg.Wait()
	if impl.maxSeen.Load() != 2 {
		t.Fatalf("peak concurrency %d, want exactly the prefetch of 2", impl.maxSeen.Load())
	}
}

func TestServiceEarlyAck(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), WithEarlyAck())
	c := newCalcClient(bus)
	if _, err := c.Add(testCtx(t), &testpb.AddRequest{A: 1}); err != nil {
		t.Fatal(err)
	}
	ops := b.Ops()
	ack := slices.IndexFunc(ops, func(o fakebroker.Op) bool { return o.Kind == "ack" })
	reply := slices.IndexFunc(ops, func(o fakebroker.Op) bool { return o.Kind == "publish" && o.Exchange == "proto.bus.callback" })
	if ack < 0 || ack > reply {
		t.Fatal("early ack acknowledges before the reply")
	}
	_, err := c.Fail(testCtx(t), &testpb.FailRequest{Mode: "unhandled", Message: "x"})
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("an early-acked failure is still reported to the caller: %v", err)
	}
	if b.HasQueue("Test.Calc.Retry") {
		t.Fatal("early ack has no retry topology")
	}
}

func TestServiceQueueArguments(t *testing.T) {
	cases := []struct {
		name string
		opts []ServiceOption
		want amqp.Table
	}{
		{"nothing configured declares no arguments", nil, amqp.Table{}},
		{"priority", []ServiceOption{WithMaxPriority(2)}, amqp.Table{"x-max-priority": int32(2)}},
		{"ttl and priority", []ServiceOption{WithMaxPriority(5), WithRetry(RetryPolicy{MaxRetries: 1, Delay: time.Second, MessageTTL: 60 * time.Second})},
			amqp.Table{"x-max-priority": int32(5), "x-message-ttl": int32(60000)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := fakebroker.New()
			bus := dialTest(t, b, fastConfig())
			startCalc(t, bus, newCalcImpl(), tc.opts...)
			info, _ := b.Queue("Test.Calc")
			if !cmp.Equal(info.Args, tc.want) || !info.Durable || info.Exclusive || info.AutoDelete {
				t.Fatalf("queue %+v, want args %v", info, tc.want)
			}
			if b.Bindings("proto.bus", "Test.Calc")[0] != "REQUEST.Test.Calc.*" {
				t.Fatal("binding")
			}
			retry, _ := b.Queue("Test.Calc.Retry")
			if _, has := retry.Args["x-max-priority"]; has {
				t.Fatal("the retry queue never gets a priority")
			}
		})
	}
}

func TestServiceRetryTopology(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), WithRetry(RetryPolicy{MaxRetries: 3, Delay: 5 * time.Second}))
	retry, ok := b.Queue("Test.Calc.Retry")
	if !ok || !cmp.Equal(retry.Args, amqp.Table{"x-message-ttl": int32(5000), "x-dead-letter-exchange": "proto.bus"}) {
		t.Fatalf("retry queue %+v", retry)
	}
	if b.ExchangeKind("Test.Calc.Retry.Exchange") != "topic" || b.Bindings("Test.Calc.Retry.Exchange", "Test.Calc.Retry")[0] != "#" {
		t.Fatal("retry exchange")
	}
	if dlq, ok := b.Queue("Test.Calc.DLQ"); !ok || len(dlq.Args) != 0 || !dlq.Durable {
		t.Fatal("DLQ")
	}
}

func TestServiceRetryQueueMismatch(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	svc, _ := bus.Register(&calcServiceDesc, newCalcImpl(), WithRetry(RetryPolicy{MaxRetries: 3, Delay: 5 * time.Second}))
	if err := svc.Start(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	bus2 := dialTest(t, b, fastConfig())
	svc2, _ := bus2.Register(&calcServiceDesc, newCalcImpl(), WithRetry(RetryPolicy{MaxRetries: 3, Delay: time.Second}))
	if err := svc2.Start(testCtx(t)); !errors.Is(err, ErrRetryQueueMismatch) {
		t.Fatalf("got %v", err)
	}
}

func TestServiceOptionValidation(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	bad := map[string][]ServiceOption{
		"priority 0":            {WithMaxPriority(0)},
		"priority + early ack":  {WithMaxPriority(2), WithEarlyAck()},
		"zero concurrency":      {WithMaxConcurrent(0)},
		"negative retries":      {WithRetry(RetryPolicy{MaxRetries: -1})},
		"retries without delay": {WithRetry(RetryPolicy{MaxRetries: 1})},
		"dotted instance":       {WithInstance("a.b")},
		"event retry no delay":  {WithEventRetry(EventRetryPolicy{MaxRetries: 1})},
	}
	for name, opts := range bad {
		if _, err := bus.Register(&calcServiceDesc, newCalcImpl(), opts...); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := bus.Register(&calcServiceDesc, newCalcImpl(), WithMaxPriority(2), WithEarlyAck()); !errors.Is(err, ErrInvalidPriority) {
		t.Errorf("priority with early ack must be ErrInvalidPriority: %v", err)
	}
}

func TestRegisterValidatesTheDescriptor(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	unknown := calcServiceDesc
	unknown.ServiceName = "Nope.Service"
	if _, err := bus.Register(&unknown, newCalcImpl()); !errors.Is(err, ErrUnknownService) {
		t.Errorf("unknown service: %v", err)
	}
	wrongKind := calcServiceDesc
	wrongKind.Methods = append(slices.Clone(calcServiceDesc.Methods), MethodDesc{MethodName: "count"})
	if _, err := bus.Register(&wrongKind, newCalcImpl()); err == nil {
		t.Error("a streaming method registered as unary must be refused")
	}
	missing := calcServiceDesc
	missing.Methods = []MethodDesc{{MethodName: "nope"}}
	if _, err := bus.Register(&missing, newCalcImpl()); err == nil {
		t.Error("a method the contract does not declare must be refused")
	}
	if _, err := bus.Register(&calcServiceDesc, struct{}{}); err == nil {
		t.Error("an implementation missing the interface must be refused")
	}
}

// ---- dispatch security ------------------------------------------------------

func TestDispatchRejectsMalformedAndMisdirectedRequests(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), fastRetry(3))
	peer := newRawPeer(t, b)
	add := &testpb.AddRequest{A: 1, B: 2}

	cases := []struct {
		name, rk  string
		body      []byte
		wantLabel string
		wantMsg   string
	}{
		{"undecodable envelope", "REQUEST.Test.Calc.add", []byte{0xff, 0xff, 0xff}, "REQUEST.Test.Calc.add", "request envelope did not decode"},
		{"body contradicts routing key", "REQUEST.Test.Calc.add", peer.request("Test.Calc.fail", add), "Test.Calc.add", "contradicts routing key"},
		{"method of another service", "REQUEST.Test.Calc.add", peer.request("Test.Other.add", add), "Test.Calc.add", "is not a method of Test.Calc"},
		{"extra segments", "REQUEST.Test.Calc.add", peer.request("Test.Calc.fail.add", add), "Test.Calc.add", "is not a method of Test.Calc"},
		{"unqualified", "REQUEST.Test.Calc.add", peer.request("add", add), "Test.Calc.add", "not a qualified method name"},
		{"undeclared method", "REQUEST.Test.Calc.nope", peer.request("Test.Calc.nope", add), "Test.Calc.nope", "declares no method nope"},
		{"declared but unimplemented", "REQUEST.Test.Calc.missing", peer.request("Test.Calc.missing", add), "Test.Calc.missing", "invalid service method missing"},
		{"undecodable payload", "REQUEST.Test.Calc.add", wire.AppendRequest(nil, wire.Request{Method: "Test.Calc.add", Data: []byte{0x08}}), "Test.Calc.add", "payload did not decode as the request type of Test.Calc.add"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cid := peer.send(tc.rk, amqp.Publishing{Body: tc.body})
			er := decodeErrorReply(t, peer.await(cid, 1)[0].Body)
			if er.Code != CodeProtocol || er.Method != tc.wantLabel || !strings.Contains(er.Message, tc.wantMsg) {
				t.Fatalf("got %+v", er)
			}
		})
	}
	eventually(t, "every rejection acked", func() bool { return len(b.OpsOf("ack")) == len(cases) })
	for _, op := range b.OpsOf("publish") {
		if op.Exchange == "Test.Calc.Retry.Exchange" {
			t.Fatal("a protocol error must never be retried")
		}
	}
}

func TestDispatchRejectsAForeignRoutingKey(t *testing.T) {
	// A message reaching this queue under another service's key (a stray
	// binding, an operator's shovel) is refused rather than run.
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	peer := newRawPeer(t, b)
	conn := b.DialPeer()
	t.Cleanup(func() { _ = conn.Close() })
	ch, _ := conn.Channel()
	_ = ch.QueueBind("Test.Calc", "REQUEST.Elsewhere.*", "proto.bus", false, nil)
	cid := peer.send("REQUEST.Elsewhere.add", amqp.Publishing{Body: peer.request("Test.Calc.add", &testpb.AddRequest{})})
	er := decodeErrorReply(t, peer.await(cid, 1)[0].Body)
	if er.Code != CodeProtocol || !strings.Contains(er.Message, "does not belong to service Test.Calc") {
		t.Fatalf("got %+v", er)
	}
}

func TestDispatchServesARuntimeNameOtherThanTheContract(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), WithInstance("player6"))
	info, ok := b.Queue("Test.Calc.player6")
	if !ok || b.Bindings("proto.bus", info.Name)[0] != "REQUEST.Test.Calc.player6.*" {
		t.Fatal("instance queue and binding")
	}
	out, err := newCalcClient(bus, WithInstance("player6")).Add(testCtx(t), &testpb.AddRequest{A: 3, B: 4})
	if err != nil || out.Sum != 7 {
		t.Fatalf("got %v %v", out, err)
	}
	if _, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{}); !errors.Is(err, ErrUnroutable) {
		t.Fatalf("the contract name alone does not reach an instance: %v", err)
	}
}

func TestCallInfoReachesTheHandler(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	got := make(chan CallInfo, 1)
	impl := &infoCalc{calcImpl: newCalcImpl(), got: got}
	startCalc(t, bus, impl)
	_, err := newCalcClient(bus).Add(testCtx(t), &testpb.AddRequest{}, WithActor("svc:billing"), WithMessageID("m-9"))
	if err != nil {
		t.Fatal(err)
	}
	ci := recvWithin(t, got, time.Second)
	if ci.Actor != "svc:billing" || ci.MessageID != "m-9" || ci.Method != "Test.Calc.add" ||
		ci.RoutingKey != "REQUEST.Test.Calc.add" || ci.Redelivered || ci.Attempt != 0 || ci.CorrelationID == "" {
		t.Fatalf("CallInfo %+v", ci)
	}
}

type infoCalc struct {
	*calcImpl
	got chan CallInfo
}

func (c *infoCalc) Add(ctx context.Context, in *testpb.AddRequest) (*testpb.AddResponse, error) {
	ci, _ := CallInfoFromContext(ctx)
	c.got <- ci
	return &testpb.AddResponse{}, nil
}
