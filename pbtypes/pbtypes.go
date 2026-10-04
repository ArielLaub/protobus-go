// Package pbtypes holds the protobus built-in custom types, bigint and
// timestamp, as generated protobuf messages plus the conversions that give
// them their meaning.
//
// On the wire each is a one-field embedded message ({value = 1}), declared at
// the root of the type namespace, exactly as the TypeScript and Python ports
// declare it. A schema field written `bigint amount = 1;` therefore has the Go
// type *pbtypes.Bigint, and `timestamp at = 2;` has *pbtypes.Timestamp.
//
//	amount, err := pbtypes.NewBigint(wei)   // refuses negatives and > 2^256-1
//	v, err := msg.GetAmount().BigInt()      // refuses wire values > 32 bytes
//	msg.At = pbtypes.NewTimestamp(time.Now())
//	when := msg.GetAt().AsTime()
package pbtypes

//go:generate protoc -I proto --go_out=.. --go_opt=module=github.com/ArielLaub/protobus-go/v2 proto/protobus/types.proto

import (
	_ "embed"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// TypesProtoPath is the import path of the built-in types' .proto file.
const TypesProtoPath = "protobus/types.proto"

// TypesProto returns the source of the built-in types' .proto file, for
// tooling that compiles schemas at runtime.
func TypesProto() string { return typesProto }

//go:embed proto/protobus/types.proto
var typesProto string

// BigintBytes is the width of the bigint wire format.
const BigintBytes = 32

// ErrBigintRange reports a value outside [0, 2^256-1], or a wire value wider
// than BigintBytes.
var ErrBigintRange = errors.New("protobus: bigint out of range")

var bigintMax = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 8*BigintBytes), big.NewInt(1))

// NewBigint encodes v. The wire format is unsigned, so a negative value or one
// above 2^256-1 is refused rather than wrapped or truncated: for amounts, a
// silently wrong number is worse than a loud failure.
func NewBigint(v *big.Int) (*Bigint, error) {
	if v == nil {
		return nil, fmt.Errorf("%w: nil value", ErrBigintRange)
	}
	if v.Sign() < 0 {
		return nil, fmt.Errorf("%w: %s is negative; the wire format is unsigned", ErrBigintRange, v)
	}
	if v.Cmp(bigintMax) > 0 {
		return nil, fmt.Errorf("%w: %s exceeds 2^256-1", ErrBigintRange, v)
	}
	buf := make([]byte, BigintBytes)
	v.FillBytes(buf)
	return &Bigint{Value: buf}, nil
}

// MustBigint is NewBigint for values known to be in range. It panics
// otherwise.
func MustBigint(v *big.Int) *Bigint {
	x, err := NewBigint(v)
	if err != nil {
		panic(err)
	}
	return x
}

// BigintFromUint64 encodes u, which is always in range.
func BigintFromUint64(u uint64) *Bigint {
	return MustBigint(new(big.Int).SetUint64(u))
}

// ParseBigint encodes a non-negative decimal or 0x-prefixed hexadecimal
// string, the two forms the TypeScript port accepts.
func ParseBigint(s string) (*Bigint, error) {
	digits, base := s, 10
	if len(s) > 2 && (strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X")) {
		digits, base = s[2:], 16
	}
	// SetString accepts a sign and underscores; neither is a valid spelling
	// of a wire value.
	for _, r := range digits {
		if !(r >= '0' && r <= '9' || base == 16 && (r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F')) {
			return nil, fmt.Errorf("protobus: %q is not a non-negative integer", s)
		}
	}
	v, ok := new(big.Int).SetString(digits, base)
	if !ok {
		return nil, fmt.Errorf("protobus: %q is not a non-negative integer", s)
	}
	return NewBigint(v)
}

// BigInt decodes x into a new *big.Int. A nil message or an empty value is 0,
// as on the other ports. A value wider than BigintBytes is refused before it
// is interpreted.
func (x *Bigint) BigInt() (*big.Int, error) {
	b := x.GetValue()
	if len(b) > BigintBytes {
		return nil, fmt.Errorf("%w: wire value is %d bytes, at most %d allowed", ErrBigintRange, len(b), BigintBytes)
	}
	return new(big.Int).SetBytes(b), nil
}

// NewTimestamp encodes t with millisecond precision; anything finer is
// truncated, as the wire format cannot carry it.
func NewTimestamp(t time.Time) *Timestamp {
	ms := t.UnixMilli()
	return &Timestamp{Value: &ms}
}

// AsTime decodes x as a UTC time. A nil message or an unset value is the Unix
// epoch, the reading every port gives an int64 that was never written.
func (x *Timestamp) AsTime() time.Time {
	return time.UnixMilli(x.GetValue()).UTC()
}
