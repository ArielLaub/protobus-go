package protobus

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
	"github.com/ArielLaub/protobus-go/v2/internal/wire"
)

func collect(t *testing.T, seq func(func(*testpb.CountChunk, error) bool)) ([]int32, error) {
	t.Helper()
	var got []int32
	for c, err := range seq {
		if err != nil {
			return got, err
		}
		got = append(got, c.I)
	}
	return got, nil
}

// frames returns the reply frames published for one stream, in order.
func frames(b *fakebroker.Broker, correlationID string) []amqp.Publishing {
	var out []amqp.Publishing
	for _, op := range b.OpsOf("publish") {
		if op.Exchange == "proto.bus.callback" && op.CorrelationID == correlationID {
			out = append(out, op.Msg)
		}
	}
	return out
}

func streamRequestID(t *testing.T, b *fakebroker.Broker) string {
	t.Helper()
	for _, op := range b.OpsOf("publish") {
		if op.Exchange == "proto.bus" && op.Key == "REQUEST.Test.Calc.count" {
			return op.CorrelationID
		}
	}
	t.Fatal("no stream request published")
	return ""
}

func TestStreamDeliversChunksInOrderWithFraming(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	got, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 5}))
	if err != nil || !slices.Equal(got, []int32{0, 1, 2, 3, 4}) {
		t.Fatalf("got %v, %v", got, err)
	}
	fs := frames(b, streamRequestID(t, b))
	if len(fs) != 5 {
		t.Fatalf("five chunks are five frames (look-ahead, no extra terminal); got %d", len(fs))
	}
	for i, f := range fs {
		seq, ok := streamSeq(f.Headers)
		if !ok || seq != int64(i) {
			t.Errorf("frame %d seq %v", i, f.Headers[headerSeq])
		}
		if final, isBool := f.Headers[headerFinal].(bool); !isBool || final != (i == 4) {
			t.Errorf("frame %d final %#v", i, f.Headers[headerFinal])
		}
		if f.ContentType != contentTypeOctetStream || f.DeliveryMode == amqp.Persistent {
			t.Errorf("frame %d properties %q/%d", i, f.ContentType, f.DeliveryMode)
		}
	}
	req := b.OpsOf("publish")[0]
	for _, op := range b.OpsOf("publish") {
		if op.Key == "REQUEST.Test.Calc.count" {
			req = op
		}
	}
	if !req.Mandatory {
		t.Fatal("a streaming request is mandatory, so an unbound method fails fast")
	}
}

func TestStreamToAnUnboundServiceFailsFast(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	start := time.Now()
	_, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 1}))
	if !errors.Is(err, ErrUnroutable) || time.Since(start) > time.Second {
		t.Fatalf("got %v after %v", err, time.Since(start))
	}
}

func TestStreamEmpty(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	got, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 0}))
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v %v", got, err)
	}
	fs := frames(b, streamRequestID(t, b))
	if len(fs) != 1 || len(fs[0].Body) != 0 || !streamFinal(fs[0].Headers) {
		t.Fatalf("an empty stream is one empty final frame: %+v", fs)
	}
	if seq, _ := streamSeq(fs[0].Headers); seq != 0 {
		t.Fatal("its seq is 0")
	}
}

func TestStreamMidStreamHandledError(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), fastRetry(3))
	got, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 5, FailAt: 2}))
	if !slices.Equal(got, []int32{0, 1}) || !IsCode(err, "TEST_FAIL") {
		t.Fatalf("got %v, %v", got, err)
	}
	fs := frames(b, streamRequestID(t, b))
	if len(fs) != 3 || !streamFinal(fs[2].Headers) || streamFinal(fs[1].Headers) {
		t.Fatalf("two chunks then the error as the final frame: %d frames", len(fs))
	}
	eventually(t, "ack", func() bool { return len(b.OpsOf("ack")) == 1 })
	for _, op := range b.OpsOf("publish") {
		if op.Exchange == "Test.Calc.Retry.Exchange" {
			t.Fatal("a stream that failed mid-way is not retried")
		}
	}
}

func TestStreamUnhandledErrorFollowsExposure(t *testing.T) {
	b := fakebroker.New()
	cfg := fastConfig()
	cfg.ExposeInternalErrors = false
	bus := dialTest(t, b, cfg)
	startCalc(t, bus, newCalcImpl())
	_, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 5, FailAt: 1, Unhandled: true}))
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != CodeInternal || re.Message != "internal service error" {
		t.Fatalf("got %#v", err)
	}
}

func TestStreamBreakCancelsTheProducer(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl, fastRetry(3))
	n := 0
	for _, err := range newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 1000, DelayMs: 5}) {
		if err != nil {
			t.Fatal(err)
		}
		if n++; n == 3 {
			break
		}
	}
	id := streamRequestID(t, b)
	end := recvWithin(t, impl.ended, 3*time.Second)
	if !end.cancelled || end.sent >= 1000 {
		t.Fatalf("the producer must observe ErrCancelled and stop early: %+v", end)
	}
	var cancels []fakebroker.Op
	for _, op := range b.OpsOf("publish") {
		if op.Exchange == "proto.bus.cancel" {
			cancels = append(cancels, op)
		}
	}
	if len(cancels) != 1 || cancels[0].CorrelationID != id || len(cancels[0].Msg.Body) != 0 {
		t.Fatalf("exactly one cancel notice carrying the stream id: %+v", cancels)
	}
	eventually(t, "ack", func() bool { return len(b.OpsOf("ack")) == 1 })
	time.Sleep(20 * time.Millisecond)
	for _, f := range frames(b, id) {
		if streamFinal(f.Headers) {
			t.Fatal("a cancelled stream publishes no final frame")
		}
	}
	for _, op := range b.OpsOf("publish") {
		if op.Exchange == "Test.Calc.Retry.Exchange" {
			t.Fatal("a cancelled delivery is not retried")
		}
	}
	if bus.cancels.size() != 0 {
		t.Fatal("cancel registrations must be released")
	}
}

func TestStreamContextCancellation(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var lastErr error
	n := 0
	for _, err := range newCalcClient(bus).Count(ctx, &testpb.CountRequest{N: 1000, DelayMs: 5}) {
		if err != nil {
			lastErr = err
			break
		}
		if n++; n == 2 {
			cancel()
		}
	}
	if !errors.Is(lastErr, context.Canceled) {
		t.Fatalf("ranging ends with the context's error, got %v", lastErr)
	}
	if end := recvWithin(t, impl.ended, 3*time.Second); !end.cancelled {
		t.Fatalf("producer not cancelled: %+v", end)
	}
}

func TestStreamIdleTimeout(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	impl := newCalcImpl()
	startCalc(t, bus, impl)
	_, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 3, DelayMs: 300}, WithIdleTimeout(50*time.Millisecond)))
	if !errors.Is(err, ErrStreamTimeout) {
		t.Fatalf("got %v", err)
	}
	if end := recvWithin(t, impl.ended, 3*time.Second); !end.cancelled {
		t.Fatalf("an idle timeout tells the producer to stop: %+v", end)
	}
}

func TestStreamOutlivesTheProcessingTimeout(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), WithProcessingTimeout(30*time.Millisecond))
	got, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 4, DelayMs: 20}))
	if err != nil || len(got) != 4 {
		t.Fatalf("a stream is bounded by its caller, not the per-message timeout: %v %v", got, err)
	}
}

func TestStreamNotRangedPublishesNothing(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	_ = newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 1})
	time.Sleep(20 * time.Millisecond)
	for _, op := range b.OpsOf("publish") {
		if op.Key == "REQUEST.Test.Calc.count" {
			t.Fatal("nothing is sent until the sequence is ranged over")
		}
	}
}

func TestStreamsDoNotCrossTalk(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), WithMaxConcurrent(8))
	c := newCalcClient(bus)
	var wg sync.WaitGroup
	for n := int32(1); n <= 8; n++ {
		wg.Go(func() {
			got, err := collect(t, c.Count(context.Background(), &testpb.CountRequest{N: n * 3}))
			if err != nil || len(got) != int(n*3) {
				t.Errorf("stream %d: %v %v", n, got, err)
			}
			for i, v := range got {
				if v != int32(i) {
					t.Errorf("stream %d out of order: %v", n, got)
					return
				}
			}
		})
	}
	wg.Wait()
	if bus.dispatcher.bufferedBytes.Load() != 0 {
		t.Fatalf("buffered bytes must return to zero, got %d", bus.dispatcher.bufferedBytes.Load())
	}
}

func TestStreamUnimplementedMethodEndsWithAProtocolError(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, &onlyAdd{})
	_, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 3}))
	if !IsCode(err, CodeProtocol) {
		t.Fatalf("got %v", err)
	}
}

type onlyAdd struct{ UnimplementedCalcServer }

// ---- client-side protocol handling against a raw producer -------------------

// rawStreamer answers every streaming request with the frames frames(d)
// returns, published as fast as possible.
func rawStreamer(t *testing.T, b *fakebroker.Broker, frames func(d amqp.Delivery, method string) []amqp.Publishing) {
	newResponder(t, b, "Test.Calc", "REQUEST.Test.Calc.*", func(d amqp.Delivery, req wire.Request) []amqp.Publishing {
		return frames(d, req.Method)
	})
}

func frame(d amqp.Delivery, method string, i int32, headers amqp.Table) amqp.Publishing {
	p := resultReply(d, method, &testpb.CountChunk{I: i})
	p.Headers = headers
	return p
}

func TestStreamDetectsALostChunk(t *testing.T) {
	b := fakebroker.New()
	rawStreamer(t, b, func(d amqp.Delivery, m string) []amqp.Publishing {
		return []amqp.Publishing{
			frame(d, m, 0, amqp.Table{headerSeq: int8(0), headerFinal: false}),
			frame(d, m, 2, amqp.Table{headerSeq: int8(2), headerFinal: true}),
		}
	})
	bus := dialTest(t, b, fastConfig())
	got, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{}))
	if !errors.Is(err, ErrStreamSequence) {
		t.Fatalf("a gap must fail the stream, not yield a short one: %v %v", got, err)
	}
}

func TestStreamDropsDuplicateFrames(t *testing.T) {
	b := fakebroker.New()
	rawStreamer(t, b, func(d amqp.Delivery, m string) []amqp.Publishing {
		return []amqp.Publishing{
			frame(d, m, 0, amqp.Table{headerSeq: int16(0), headerFinal: false}),
			frame(d, m, 0, amqp.Table{headerSeq: int32(0), headerFinal: false}),
			frame(d, m, 1, amqp.Table{headerSeq: int64(1), headerFinal: "true"}),
		}
	})
	bus := dialTest(t, b, fastConfig())
	got, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{}))
	if err != nil || !slices.Equal(got, []int32{0, 1}) {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestStreamAcceptsAPeerThatSendsNoSequence(t *testing.T) {
	b := fakebroker.New()
	rawStreamer(t, b, func(d amqp.Delivery, m string) []amqp.Publishing {
		return []amqp.Publishing{
			frame(d, m, 0, amqp.Table{headerFinal: false}),
			frame(d, m, 1, amqp.Table{headerFinal: int8(1)}),
		}
	})
	bus := dialTest(t, b, fastConfig())
	got, err := collect(t, newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{}))
	if err != nil || !slices.Equal(got, []int32{0, 1}) {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestStreamBackpressure(t *testing.T) {
	b := fakebroker.New()
	rawStreamer(t, b, func(d amqp.Delivery, m string) []amqp.Publishing {
		var out []amqp.Publishing
		for i := range int32(20) {
			out = append(out, frame(d, m, i, amqp.Table{headerSeq: i, headerFinal: i == 19}))
		}
		return out
	})
	cfg := fastConfig()
	cfg.StreamMaxBufferedChunks = 3
	bus := dialTest(t, b, cfg)
	var err error
	for _, e := range newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{}) {
		if e != nil {
			err = e
			break
		}
		time.Sleep(50 * time.Millisecond) // a slow consumer
	}
	if !errors.Is(err, ErrStreamBackpressure) {
		t.Fatalf("got %v", err)
	}
	if bus.dispatcher.bufferedBytes.Load() != 0 {
		t.Fatal("a failed stream returns its bytes to the allowance")
	}
}

func TestStreamTotalAllowanceAcrossCalls(t *testing.T) {
	b := fakebroker.New()
	chunk := &testpb.CountChunk{I: 1}
	rawStreamer(t, b, func(d amqp.Delivery, m string) []amqp.Publishing {
		var out []amqp.Publishing
		for i := range int32(10) {
			out = append(out, frame(d, m, i, amqp.Table{headerSeq: i, headerFinal: i == 9}))
		}
		return out
	})
	cfg := fastConfig()
	one := int64(len(resultReply(amqp.Delivery{}, "Test.Calc.count", chunk).Body))
	cfg.StreamMaxBufferedBytes = one * 100
	cfg.StreamMaxTotalBufferedBytes = one * 100
	bus := dialTest(t, b, cfg)
	// Pre-load the allowance as if other streams held most of it.
	bus.dispatcher.bufferedBytes.Add(one * 95)
	defer bus.dispatcher.bufferedBytes.Add(-one * 95)
	var err error
	for _, e := range newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{}) {
		if e != nil {
			err = e
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !errors.Is(err, ErrStreamBackpressure) {
		t.Fatalf("the process-wide bound must apply: %v", err)
	}
}

func TestStreamDisconnectFailsTheStream(t *testing.T) {
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl())
	var err error
	n := 0
	for _, e := range newCalcClient(bus).Count(testCtx(t), &testpb.CountRequest{N: 1000, DelayMs: 5}) {
		if e != nil {
			err = e
			break
		}
		if n++; n == 2 {
			b.KillConnections()
		}
	}
	if !errors.Is(err, ErrDisconnected) {
		t.Fatalf("got %v", err)
	}
}
