package protobus

import (
	"math/big"
	"testing"
	"time"
)

func TestBigIntEncodeSmall(t *testing.T) {
	bt := &BigIntType{}

	encoded, err := bt.Encode(int64(42))
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	bytes, ok := encoded.([]byte)
	if !ok {
		t.Fatalf("Expected []byte, got %T", encoded)
	}

	if len(bytes) != 32 {
		t.Errorf("Expected 32 bytes, got %d", len(bytes))
	}

	// Decode back
	decoded, err := bt.Decode(bytes)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	bi, ok := decoded.(*big.Int)
	if !ok {
		t.Fatalf("Expected *big.Int, got %T", decoded)
	}

	if bi.Int64() != 42 {
		t.Errorf("Expected 42, got %d", bi.Int64())
	}
}

func TestBigIntEncodeLarge(t *testing.T) {
	bt := &BigIntType{}

	// Large number
	large := new(big.Int)
	large.SetString("123456789012345678901234567890", 10)

	encoded, err := bt.Encode(large)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := bt.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	bi := decoded.(*big.Int)
	if bi.Cmp(large) != 0 {
		t.Errorf("Expected %s, got %s", large.String(), bi.String())
	}
}

func TestBigIntEncodeHexString(t *testing.T) {
	bt := &BigIntType{}

	encoded, err := bt.Encode("0xFF")
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := bt.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	bi := decoded.(*big.Int)
	if bi.Int64() != 255 {
		t.Errorf("Expected 255, got %d", bi.Int64())
	}
}

func TestTimestampEncode(t *testing.T) {
	tt := &TimestampType{}

	now := time.Now()
	encoded, err := tt.Encode(now)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := tt.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result, ok := decoded.(time.Time)
	if !ok {
		t.Fatalf("Expected time.Time, got %T", decoded)
	}

	// Compare milliseconds (we lose some precision)
	if result.UnixMilli() != now.UnixMilli() {
		t.Errorf("Expected %d, got %d", now.UnixMilli(), result.UnixMilli())
	}
}

func TestTimestampEncodeInt(t *testing.T) {
	tt := &TimestampType{}

	ms := int64(1703000000000) // Some timestamp
	encoded, err := tt.Encode(ms)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := tt.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(time.Time)
	if result.UnixMilli() != ms {
		t.Errorf("Expected %d, got %d", ms, result.UnixMilli())
	}
}

func TestCustomTypeRegistry(t *testing.T) {
	registry := NewCustomTypeRegistry()

	// Built-in types should be registered
	if !registry.IsCustomType("bigint") {
		t.Error("BigInt should be registered")
	}

	if !registry.IsCustomType("timestamp") {
		t.Error("Timestamp should be registered")
	}

	// Case insensitive
	if !registry.IsCustomType("BigInt") {
		t.Error("BigInt should be case insensitive")
	}

	// Non-existent type
	if registry.IsCustomType("nonexistent") {
		t.Error("nonexistent should not be registered")
	}
}
