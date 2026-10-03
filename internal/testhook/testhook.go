// Package testhook lets protobustest reach protobus internals without making
// them public API.
package testhook

import "github.com/ArielLaub/protobus-go/v2/internal/transport"

// WithDialer returns a protobus.DialOption substituting the transport. The
// protobus package sets it.
var WithDialer func(transport.Dialer) any
