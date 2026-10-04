package protobus

import (
	"math"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHeaderIntAcceptsEveryEncodingPeersUse(t *testing.T) {
	// amqplib picks the integer width by magnitude; Python's pamqp and Go's
	// amqp091 pick their own; some peers stringify.
	ok := []struct {
		in   any
		want int64
	}{
		{int8(3), 3}, {int16(300), 300}, {int32(70000), 70000}, {int64(1 << 40), 1 << 40}, {7, 7},
		{uint8(9), 9}, {uint16(9), 9}, {uint32(9), 9}, {uint64(9), 9},
		{float64(4), 4}, {float32(2), 2}, {"12", 12}, {" 5 ", 5}, {[]byte("6"), 6},
	}
	for _, c := range ok {
		if got, valid := headerInt(c.in); !valid || got != c.want {
			t.Errorf("headerInt(%#v) = %d, %v", c.in, got, valid)
		}
	}
	for _, bad := range []any{nil, 1.5, "x", []byte("1e3"), uint64(math.MaxUint64), true, amqp.Table{}} {
		if _, valid := headerInt(bad); valid {
			t.Errorf("headerInt(%#v) must be refused", bad)
		}
	}
}

func TestIntHeaderUsesTheNarrowestWidth(t *testing.T) {
	if _, is32 := intHeader(5).(int32); !is32 {
		t.Fatal("small values go out as int32")
	}
	if _, is64 := intHeader(time.Now().UnixMilli()).(int64); !is64 {
		t.Fatal("epoch milliseconds need int64")
	}
}

func TestStreamFinalIsTolerant(t *testing.T) {
	cases := []struct {
		in   any
		want bool
	}{
		{nil, false}, {true, true}, {false, false}, {"true", true}, {"TRUE", true}, {"1", true}, {"false", false},
		{"0", false}, {[]byte("true"), true}, {int8(1), true}, {int32(0), false}, {float64(1), true},
	}
	for _, c := range cases {
		in, want := c.in, c.want
		h := amqp.Table{}
		if in != nil {
			h[headerFinal] = in
		}
		if got := streamFinal(h); got != want {
			t.Errorf("final %#v -> %v, want %v", in, got, want)
		}
	}
}

func TestStreamSeqIgnoresWhatItCannotRead(t *testing.T) {
	for _, bad := range []any{"x", -1, 1.5} {
		if _, ok := streamSeq(amqp.Table{headerSeq: bad}); ok {
			t.Errorf("seq %#v must disable checking, not fail", bad)
		}
	}
	if n, ok := streamSeq(amqp.Table{headerSeq: "3"}); !ok || n != 3 {
		t.Error("a numeric string is accepted")
	}
	if _, ok := streamSeq(nil); ok {
		t.Error("absent")
	}
}

func TestRetryCountTreatsGarbageAsZero(t *testing.T) {
	cases := map[any]int{nil: 0, "x": 0, int8(-2): 0, int8(2): 2, "3": 3, int64(math.MaxInt64): math.MaxInt32}
	for in, want := range cases {
		h := amqp.Table{}
		if in != nil {
			h[headerRetryCount] = in
		}
		if got := retryCount(h); got != want {
			t.Errorf("retryCount(%#v) = %d, want %d", in, got, want)
		}
	}
}

func TestOriginalRoutingKeyPrefersTheHeader(t *testing.T) {
	d := &amqp.Delivery{RoutingKey: "REQUEST.A.b", Headers: amqp.Table{headerOriginalKey: []byte("REQUEST.A.c")}}
	if got := originalRoutingKey(d); got != "REQUEST.A.c" {
		t.Fatal(got)
	}
	d.Headers = amqp.Table{headerOriginalKey: ""}
	if got := originalRoutingKey(d); got != "REQUEST.A.b" {
		t.Fatal(got)
	}
}
