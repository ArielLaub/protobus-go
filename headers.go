package protobus

import (
	"math"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Header names of the retry and dead-letter protocol, shared with the other
// ports.
const (
	headerRetryCount     = "x-retry-count"
	headerOriginalKey    = "x-original-routing-key"
	headerOriginalQueue  = "x-original-queue"
	headerFirstFailure   = "x-first-failure-time"
	headerDeadLetterTime = "x-dlq-time"
	headerLastError      = "x-last-error"
)

const contentTypeOctetStream = "application/octet-stream"

// Peers encode header integers with whatever width suits the value (amqplib
// picks int8/16/32/64 by magnitude), and some send numbers as strings. Every
// reader here accepts all of them.

func headerInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case float32:
		return floatInt(float64(n))
	case float64:
		return floatInt(n)
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64)
		return i, err == nil
	case []byte:
		i, err := strconv.ParseInt(strings.TrimSpace(string(n)), 10, 64)
		return i, err == nil
	}
	return 0, false
}

func floatInt(f float64) (int64, bool) {
	if f != math.Trunc(f) || f < math.MinInt64 || f > math.MaxInt64 {
		return 0, false
	}
	return int64(f), true
}

// intHeader returns an integer encoded as the narrowest AMQP type that holds
// it, as amqplib does.
func intHeader(n int64) any {
	if n >= math.MinInt32 && n <= math.MaxInt32 {
		return int32(n)
	}
	return n
}

// streamFinal reads x-protobus-final tolerantly: a boolean, a number (non-zero
// is true) or the text "true"/"1". Absent means false.
func streamFinal(h amqp.Table) bool {
	switch v := h[headerFinal].(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true") || v == "1"
	case []byte:
		return strings.EqualFold(string(v), "true") || string(v) == "1"
	default:
		n, ok := headerInt(v)
		return ok && n != 0
	}
}

// streamSeq reads x-protobus-seq. ok is false when it is absent or not a
// non-negative integer, which disables sequence checking rather than
// manufacturing a violation: a peer predating the header is behaving
// correctly.
func streamSeq(h amqp.Table) (seq int64, ok bool) {
	v, present := h[headerSeq]
	if !present {
		return 0, false
	}
	n, ok := headerInt(v)
	if !ok || n < 0 {
		return 0, false
	}
	return n, true
}

// retryCount reads x-retry-count, treating anything unreadable as zero.
func retryCount(h amqp.Table) int {
	n, ok := headerInt(h[headerRetryCount])
	if !ok || n < 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(n)
}

// carriedProperties copies the delivery properties a retry or dead-letter
// republish preserves. deliveryMode is re-expressed as persistent; expiration
// would race the retry queue's TTL or delete DLQ evidence; userId would be
// rejected when the republisher is a different broker user.
func carriedProperties(d *amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		Priority:        d.Priority,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
	}
}

// copyHeaders gives handlers their own copy of the delivery headers, as a
// plain map so the AMQP client library stays out of the public API.
func copyHeaders(h amqp.Table) map[string]any {
	if len(h) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}
