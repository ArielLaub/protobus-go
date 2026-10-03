// Package wire encodes and decodes the protobus envelopes: the five small
// protobuf messages every request, reply and event travels inside.
//
// The envelopes have no .proto file and no package. In the TypeScript
// reference they are protobufjs decorator classes, which means presence works
// proto2-style: a field set to an empty string or empty bytes is still
// written. This package reproduces those bytes exactly rather than leaning on
// generated proto3 code, which would omit them. Both forms decode identically
// on every port, but byte parity keeps golden tests and captured traffic
// comparable across languages.
//
//	message RequestContainer  { string method = 1; string actor = 2; bytes data = 3; }
//	message ResponseResult    { string method = 1; bytes data = 2; }
//	message ResponseError     { string method = 1; string message = 2; string code = 3; }
//	message ResponseContainer { oneof value { ResponseResult result = 1; ResponseError error = 2; } }
//	message EventContainer    { string type = 1; string topic = 2; bytes data = 3; }
//
// Decoded byte slices alias the input buffer. AMQP deliveries own their body,
// so aliasing saves a copy per message; callers that keep a payload beyond the
// lifetime of the delivery must clone it.
package wire

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
)

// Request is a RequestContainer.
type Request struct {
	// Method is the contract method name, "<package>.<Service>.<method>".
	Method string
	// Actor is free-text caller identity, for tracing only. It is not
	// authenticated by anything.
	Actor string
	// Data is the encoded request message.
	Data []byte
}

// Result is a ResponseResult.
type Result struct {
	Method string
	Data   []byte
}

// Error is a ResponseError. Only these three fields cross the wire: an error's
// type, stack and any other detail stay in the process that raised it.
type Error struct {
	Method  string
	Message string
	Code    string
}

// Response is a ResponseContainer. Exactly one of Result and Error is set.
type Response struct {
	Result *Result
	Error  *Error
}

// Event is an EventContainer.
type Event struct {
	// Type is the fully-qualified protobuf message name of Data, without a
	// leading dot.
	Type string
	// Topic is the topic the event was published under.
	Topic string
	Data  []byte
}

// ErrMalformed reports bytes that are not a valid envelope.
var ErrMalformed = errors.New("protobus: malformed envelope")

// sizeField is the encoded size of a length-delimited field of n bytes.
func sizeField(field protowire.Number, n int) int {
	return protowire.SizeTag(field) + protowire.SizeBytes(n)
}

func appendString(b []byte, field protowire.Number, s string) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	return protowire.AppendString(b, s)
}

func appendBytes(b []byte, field protowire.Number, v []byte) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

// grow makes sure b has room for n more bytes, so an Append writes with a
// single allocation at most.
func grow(b []byte, n int) []byte {
	if cap(b)-len(b) >= n {
		return b
	}
	nb := make([]byte, len(b), len(b)+n)
	copy(nb, b)
	return nb
}

// SizeRequest is the encoded length of r.
func SizeRequest(r Request) int {
	n := sizeField(1, len(r.Method)) + sizeField(3, len(r.Data))
	if r.Actor != "" {
		n += sizeField(2, len(r.Actor))
	}
	return n
}

// AppendRequest appends the encoding of r to b. The method and data are always
// written; the actor only when there is one, as TS does when a caller passes
// none.
func AppendRequest(b []byte, r Request) []byte {
	b = grow(b, SizeRequest(r))
	b = appendString(b, 1, r.Method)
	if r.Actor != "" {
		b = appendString(b, 2, r.Actor)
	}
	return appendBytes(b, 3, r.Data)
}

func sizeResult(r Result) int {
	return sizeField(1, len(r.Method)) + sizeField(2, len(r.Data))
}

// AppendResult appends a bare ResponseResult (not wrapped in a container).
func AppendResult(b []byte, r Result) []byte {
	b = grow(b, sizeResult(r))
	b = appendString(b, 1, r.Method)
	return appendBytes(b, 2, r.Data)
}

func sizeError(e Error) int {
	return sizeField(1, len(e.Method)) + sizeField(2, len(e.Message)) + sizeField(3, len(e.Code))
}

func appendError(b []byte, e Error) []byte {
	b = grow(b, sizeError(e))
	b = appendString(b, 1, e.Method)
	b = appendString(b, 2, e.Message)
	return appendString(b, 3, e.Code)
}

// AppendResponse appends the encoding of r to b. It refuses a container that
// carries neither or both members, since no peer could interpret it.
func AppendResponse(b []byte, r Response) ([]byte, error) {
	switch {
	case r.Result != nil && r.Error != nil:
		return b, errors.New("protobus: response carries both a result and an error")
	case r.Result != nil:
		n := sizeResult(*r.Result)
		b = grow(b, protowire.SizeTag(1)+protowire.SizeBytes(n))
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(n))
		return AppendResult(b, *r.Result), nil
	case r.Error != nil:
		n := sizeError(*r.Error)
		b = grow(b, protowire.SizeTag(2)+protowire.SizeBytes(n))
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(n))
		return appendError(b, *r.Error), nil
	default:
		return b, errors.New("protobus: response carries neither a result nor an error")
	}
}

// AppendEvent appends the encoding of e to b. All three fields are always
// written.
func AppendEvent(b []byte, e Event) []byte {
	b = grow(b, sizeField(1, len(e.Type))+sizeField(2, len(e.Topic))+sizeField(3, len(e.Data)))
	b = appendString(b, 1, e.Type)
	b = appendString(b, 2, e.Topic)
	return appendBytes(b, 3, e.Data)
}

// fieldFunc receives one length-delimited field. Envelopes contain nothing
// else, so any other wire type on a known field is malformed.
type fieldFunc func(num protowire.Number, v []byte) error

// walk visits every field of a message, skipping unknown ones so that a newer
// peer adding a field cannot break an older reader.
func walk(b []byte, known func(protowire.Number) bool, fn fieldFunc) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return fmt.Errorf("%w: %v", ErrMalformed, protowire.ParseError(n))
		}
		b = b[n:]
		if !known(num) {
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return fmt.Errorf("%w: field %d: %v", ErrMalformed, num, protowire.ParseError(n))
			}
			b = b[n:]
			continue
		}
		if typ != protowire.BytesType {
			return fmt.Errorf("%w: field %d has wire type %d, want length-delimited", ErrMalformed, num, typ)
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return fmt.Errorf("%w: field %d: %v", ErrMalformed, num, protowire.ParseError(n))
		}
		b = b[n:]
		if err := fn(num, v); err != nil {
			return err
		}
	}
	return nil
}

func upTo(max protowire.Number) func(protowire.Number) bool {
	return func(n protowire.Number) bool { return n >= 1 && n <= max }
}

func str(num protowire.Number, v []byte) (string, error) {
	if !utf8.Valid(v) {
		return "", fmt.Errorf("%w: field %d is not valid UTF-8", ErrMalformed, num)
	}
	return string(v), nil
}

// DecodeRequest decodes a RequestContainer. Data aliases b.
func DecodeRequest(b []byte) (Request, error) {
	var r Request
	err := walk(b, upTo(3), func(num protowire.Number, v []byte) (err error) {
		switch num {
		case 1:
			r.Method, err = str(num, v)
		case 2:
			r.Actor, err = str(num, v)
		case 3:
			r.Data = v
		}
		return err
	})
	return r, err
}

func decodeResult(b []byte, r *Result) error {
	return walk(b, upTo(2), func(num protowire.Number, v []byte) (err error) {
		switch num {
		case 1:
			r.Method, err = str(num, v)
		case 2:
			r.Data = v
		}
		return err
	})
}

func decodeError(b []byte, e *Error) error {
	return walk(b, upTo(3), func(num protowire.Number, v []byte) (err error) {
		switch num {
		case 1:
			e.Method, err = str(num, v)
		case 2:
			e.Message, err = str(num, v)
		case 3:
			e.Code, err = str(num, v)
		}
		return err
	})
}

// DecodeResponse decodes a ResponseContainer. When both members are present
// the error wins, matching the TypeScript reader, so the same bytes cannot
// mean success on one port and failure on another. A container with neither
// is refused.
func DecodeResponse(b []byte) (Response, error) {
	var (
		res    Result
		er     Error
		hasRes bool
		hasErr bool
	)
	err := walk(b, upTo(2), func(num protowire.Number, v []byte) error {
		// Repeated occurrences of an embedded message merge, as protobuf
		// requires; decoding into the same value does exactly that.
		if num == 1 {
			hasRes = true
			return decodeResult(v, &res)
		}
		hasErr = true
		return decodeError(v, &er)
	})
	if err != nil {
		return Response{}, err
	}
	switch {
	case hasErr:
		return Response{Error: &er}, nil
	case hasRes:
		return Response{Result: &res}, nil
	default:
		return Response{}, fmt.Errorf("%w: response carries neither a result nor an error", ErrMalformed)
	}
}

// DecodeEvent decodes an EventContainer. Data aliases b.
func DecodeEvent(b []byte) (Event, error) {
	var e Event
	err := walk(b, upTo(3), func(num protowire.Number, v []byte) (err error) {
		switch num {
		case 1:
			e.Type, err = str(num, v)
		case 2:
			e.Topic, err = str(num, v)
		case 3:
			e.Data = v
		}
		return err
	})
	return e, err
}
