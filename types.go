package protobus

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CustomType defines the interface for custom type encoders/decoders.
type CustomType interface {
	Name() string
	Encode(value interface{}) (interface{}, error)
	Decode(value interface{}) (interface{}, error)
}

// CustomTypeRegistry manages registered custom types.
type CustomTypeRegistry struct {
	mu    sync.RWMutex
	types map[string]CustomType
}

// NewCustomTypeRegistry creates a new registry with built-in types.
func NewCustomTypeRegistry() *CustomTypeRegistry {
	r := &CustomTypeRegistry{
		types: make(map[string]CustomType),
	}
	// Register built-in types
	r.Register(&BigIntType{})
	r.Register(&TimestampType{})
	return r
}

// Register adds a custom type to the registry.
func (r *CustomTypeRegistry) Register(ct CustomType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.types[strings.ToLower(ct.Name())] = ct
}

// Get returns a custom type by name (case-insensitive).
func (r *CustomTypeRegistry) Get(name string) (CustomType, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ct, ok := r.types[strings.ToLower(name)]
	return ct, ok
}

// IsCustomType checks if a name is a registered custom type.
func (r *CustomTypeRegistry) IsCustomType(name string) bool {
	_, ok := r.Get(name)
	return ok
}

// Names returns all registered type names.
func (r *CustomTypeRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.types))
	for name := range r.types {
		names = append(names, name)
	}
	return names
}

// Global registry
var globalTypeRegistry = NewCustomTypeRegistry()

// GetTypeRegistry returns the global type registry.
func GetTypeRegistry() *CustomTypeRegistry {
	return globalTypeRegistry
}

// RegisterCustomType registers a custom type globally.
func RegisterCustomType(ct CustomType) {
	globalTypeRegistry.Register(ct)
}

// BigIntType handles encoding/decoding of big integers.
type BigIntType struct{}

func (t *BigIntType) Name() string {
	return "BigInt"
}

func (t *BigIntType) Encode(value interface{}) (interface{}, error) {
	var bi *big.Int

	switch v := value.(type) {
	case *big.Int:
		bi = v
	case int64:
		bi = big.NewInt(v)
	case int:
		bi = big.NewInt(int64(v))
	case string:
		bi = new(big.Int)
		if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "0X") {
			_, ok := bi.SetString(v[2:], 16)
			if !ok {
				return nil, fmt.Errorf("invalid hex string: %s", v)
			}
		} else {
			_, ok := bi.SetString(v, 10)
			if !ok {
				return nil, fmt.Errorf("invalid decimal string: %s", v)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported type for BigInt: %T", value)
	}

	// Encode as 32-byte big-endian (pad with zeros)
	bytes := bi.Bytes()
	result := make([]byte, 32)
	copy(result[32-len(bytes):], bytes)
	return result, nil
}

func (t *BigIntType) Decode(value interface{}) (interface{}, error) {
	var bytes []byte

	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		// Assume base64 or hex encoded
		bytes = []byte(v)
	default:
		return nil, fmt.Errorf("unsupported type for BigInt decode: %T", value)
	}

	bi := new(big.Int)
	bi.SetBytes(bytes)
	return bi, nil
}

// TimestampType handles encoding/decoding of timestamps.
type TimestampType struct{}

func (t *TimestampType) Name() string {
	return "Timestamp"
}

func (t *TimestampType) Encode(value interface{}) (interface{}, error) {
	var ms int64

	switch v := value.(type) {
	case time.Time:
		ms = v.UnixMilli()
	case int64:
		ms = v
	case int:
		ms = int64(v)
	case float64:
		ms = int64(v)
	default:
		return nil, fmt.Errorf("unsupported type for Timestamp: %T", value)
	}

	// Encode as 8-byte big-endian
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(ms))
	return bytes, nil
}

func (t *TimestampType) Decode(value interface{}) (interface{}, error) {
	var ms int64

	switch v := value.(type) {
	case []byte:
		if len(v) >= 8 {
			ms = int64(binary.BigEndian.Uint64(v))
		}
	case float64:
		ms = int64(v)
	case int64:
		ms = v
	case string:
		var err error
		ms, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp string: %s", v)
		}
	default:
		return nil, fmt.Errorf("unsupported type for Timestamp decode: %T", value)
	}

	return time.UnixMilli(ms), nil
}
