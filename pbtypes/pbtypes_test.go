package pbtypes

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math/big"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func hexOf(t *testing.T, m proto.Message) string {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func TestBigintFullNameMatchesOtherPorts(t *testing.T) {
	// TS and Python register the type as a root-level message called "bigint".
	// Any other full name would make a Go schema incompatible on the wire's
	// type references (events carry the type name) and in shared .proto files.
	if got := (&Bigint{}).ProtoReflect().Descriptor().FullName(); got != "bigint" {
		t.Fatalf("full name %q, want bigint", got)
	}
	if got := (&Timestamp{}).ProtoReflect().Descriptor().FullName(); got != "timestamp" {
		t.Fatalf("full name %q, want timestamp", got)
	}
}

func TestBigintEncodesAs32ByteBigEndian(t *testing.T) {
	// Measured from the TS reference: 5n -> inner value of exactly 32 bytes.
	want := "0a20" + "00000000000000000000000000000000000000000000000000000000000000" + "05"
	if got := hexOf(t, MustBigint(big.NewInt(5))); got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
	// Zero is still 32 explicit bytes, not an empty field.
	want = "0a20" + "0000000000000000000000000000000000000000000000000000000000000000"
	if got := hexOf(t, MustBigint(new(big.Int))); got != want {
		t.Fatalf("zero: got %s\nwant %s", got, want)
	}
}

func TestBigintRangeIsEnforcedOnEncode(t *testing.T) {
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	if _, err := NewBigint(max); err != nil {
		t.Fatalf("2^256-1 must encode: %v", err)
	}
	over := new(big.Int).Add(max, big.NewInt(1))
	if _, err := NewBigint(over); !errors.Is(err, ErrBigintRange) {
		t.Fatalf("2^256 must be refused with ErrBigintRange, got %v", err)
	}
	if _, err := NewBigint(big.NewInt(-1)); !errors.Is(err, ErrBigintRange) {
		t.Fatalf("a negative value must be refused, got %v", err)
	}
	if _, err := NewBigint(nil); err == nil {
		t.Fatal("a nil *big.Int must be refused")
	}
}

func TestBigintDecodeAcceptsShortEncodings(t *testing.T) {
	// TS decodes whatever length <= 32 is present, big-endian.
	v, err := (&Bigint{Value: []byte{0x01, 0x00}}).BigInt()
	if err != nil {
		t.Fatal(err)
	}
	if v.Int64() != 256 {
		t.Fatalf("got %s want 256", v)
	}
}

func TestBigintDecodeOfEmptyOrNilIsZero(t *testing.T) {
	for name, x := range map[string]*Bigint{"nil message": nil, "empty value": {}} {
		v, err := x.BigInt()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if v.Sign() != 0 {
			t.Fatalf("%s: got %s want 0", name, v)
		}
	}
}

func TestBigintDecodeRefusesOverlongValues(t *testing.T) {
	// Anything wider than the wire format is malformed; refusing it before
	// decoding is also what keeps a hostile value from costing CPU.
	_, err := (&Bigint{Value: bytes.Repeat([]byte{1}, 33)}).BigInt()
	if !errors.Is(err, ErrBigintRange) {
		t.Fatalf("want ErrBigintRange, got %v", err)
	}
}

func TestBigintRoundTrip(t *testing.T) {
	for _, s := range []string{"0", "1", "255", "256", "18446744073709551615", "1000000000000000000000000000000",
		"115792089237316195423570985008687907853269984665640564039457584007913129639935"} {
		in, _ := new(big.Int).SetString(s, 10)
		b, err := proto.Marshal(MustBigint(in))
		if err != nil {
			t.Fatal(err)
		}
		var out Bigint
		if err := proto.Unmarshal(b, &out); err != nil {
			t.Fatal(err)
		}
		got, err := out.BigInt()
		if err != nil {
			t.Fatal(err)
		}
		if got.Cmp(in) != 0 {
			t.Fatalf("round trip %s -> %s", in, got)
		}
	}
}

func TestParseBigint(t *testing.T) {
	cases := map[string]int64{"42": 42, "0x2a": 42, "0X2A": 42, "0": 0}
	for s, want := range cases {
		x, err := ParseBigint(s)
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		v, _ := x.BigInt()
		if v.Int64() != want {
			t.Fatalf("%q: got %s want %d", s, v, want)
		}
	}
	for _, bad := range []string{"", "abc", "-1", "0x", "1.5", " 1"} {
		if _, err := ParseBigint(bad); err == nil {
			t.Fatalf("%q must not parse", bad)
		}
	}
}

func TestBigintFromUint64(t *testing.T) {
	v, err := BigintFromUint64(^uint64(0)).BigInt()
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "18446744073709551615" {
		t.Fatalf("got %s", v)
	}
}

func TestBigintDoesNotRetainCallerValue(t *testing.T) {
	in := big.NewInt(7)
	x := MustBigint(in)
	in.SetInt64(9)
	v, _ := x.BigInt()
	if v.Int64() != 7 {
		t.Fatal("NewBigint must snapshot the value")
	}
	v.SetInt64(11)
	again, _ := x.BigInt()
	if again.Int64() != 7 {
		t.Fatal("BigInt must return a fresh value")
	}
}

func TestTimestampEncodesMillisecondsAsInt64(t *testing.T) {
	// Measured from TS: Date(1000) -> 08 e8 07; Date(0) -> 08 00 (explicitly
	// present); Date(-1000) -> ten-byte two's-complement varint.
	cases := map[int64]string{
		1000:  "08e807",
		0:     "0800",
		-1000: "0898f8ffffffffffffff01",
	}
	for ms, want := range cases {
		if got := hexOf(t, NewTimestamp(time.UnixMilli(ms))); got != want {
			t.Fatalf("%dms: got %s want %s", ms, got, want)
		}
	}
}

func TestTimestampTruncatesToMilliseconds(t *testing.T) {
	ts := time.Date(2024, 2, 29, 12, 0, 0, 123_456_789, time.UTC)
	got := NewTimestamp(ts).AsTime()
	if !got.Equal(ts.Truncate(time.Millisecond)) {
		t.Fatalf("got %v", got)
	}
	if got.Location() != time.UTC {
		t.Fatal("AsTime must return UTC")
	}
}

func TestTimestampBeforeEpoch(t *testing.T) {
	ts := time.Date(1969, 7, 20, 20, 17, 40, 0, time.UTC)
	b, _ := proto.Marshal(NewTimestamp(ts))
	var out Timestamp
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !out.AsTime().Equal(ts) {
		t.Fatalf("got %v want %v", out.AsTime(), ts)
	}
}

func TestTimestampNilAndAbsent(t *testing.T) {
	var x *Timestamp
	if !x.AsTime().Equal(time.UnixMilli(0)) {
		t.Fatal("a nil timestamp reads as the epoch, like an unset int64")
	}
	if !(&Timestamp{}).AsTime().Equal(time.UnixMilli(0)) {
		t.Fatal("an empty timestamp reads as the epoch")
	}
}
