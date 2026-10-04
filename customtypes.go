package protobus

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

// The built-in custom types carry rules protobuf itself does not enforce: a
// bigint is at most 32 bytes. The other ports refuse a wider one while
// decoding, so protobus-go checks every decoded message the same way, before
// a handler sees it: a malformed value is answered as PROTOCOL_ERROR rather
// than surfacing later as a handler error that would be retried.

// bigintName is the full name of the built-in bigint type.
var bigintName = (&pbtypes.Bigint{}).ProtoReflect().Descriptor().FullName()

// hasBigint caches, per message type, whether a bigint can occur anywhere in
// it, so messages without one pay nothing beyond a map read.
var hasBigint sync.Map // protoreflect.FullName -> bool

func reachesBigint(md protoreflect.MessageDescriptor) bool {
	if v, ok := hasBigint.Load(md.FullName()); ok {
		return v.(bool)
	}
	found := reachesBigintWalk(md, map[protoreflect.FullName]bool{})
	hasBigint.Store(md.FullName(), found)
	return found
}

func reachesBigintWalk(md protoreflect.MessageDescriptor, visiting map[protoreflect.FullName]bool) bool {
	if md.FullName() == bigintName {
		return true
	}
	if visiting[md.FullName()] {
		return false // a cycle; the other paths decide
	}
	visiting[md.FullName()] = true
	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		if fd.IsMap() {
			fd = fd.MapValue()
		}
		if fd.Message() != nil && reachesBigintWalk(fd.Message(), visiting) {
			return true
		}
	}
	return false
}

// checkCustomTypes reports the first built-in custom type value in m that
// violates its wire format.
func checkCustomTypes(m proto.Message) error {
	r := m.ProtoReflect()
	if !reachesBigint(r.Descriptor()) {
		return nil
	}
	return checkMessage(r)
}

func checkMessage(r protoreflect.Message) error {
	if r.Descriptor().FullName() == bigintName {
		if v := r.Get(r.Descriptor().Fields().ByNumber(1)).Bytes(); len(v) > pbtypes.BigintBytes {
			return fmt.Errorf("%w: wire value is %d bytes, at most %d allowed", pbtypes.ErrBigintRange, len(v), pbtypes.BigintBytes)
		}
		return nil
	}
	var err error
	r.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			if fd.MapValue().Message() == nil || !reachesBigint(fd.MapValue().Message()) {
				return true
			}
			v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				err = checkMessage(mv.Message())
				return err == nil
			})
		case fd.Message() == nil || !reachesBigint(fd.Message()):
		case fd.IsList():
			l := v.List()
			for i := range l.Len() {
				if err = checkMessage(l.Get(i).Message()); err != nil {
					break
				}
			}
		default:
			err = checkMessage(v.Message())
		}
		return err == nil
	})
	return err
}

// unmarshal decodes b into m and checks its custom types.
func unmarshal(b []byte, m proto.Message) error {
	if err := proto.Unmarshal(b, m); err != nil {
		return err
	}
	return checkCustomTypes(m)
}

// marshal encodes m after checking its custom types, so protobus never sends
// a value every peer would refuse to decode.
func marshal(m proto.Message) ([]byte, error) {
	if err := checkCustomTypes(m); err != nil {
		return nil, err
	}
	return proto.Marshal(m)
}
