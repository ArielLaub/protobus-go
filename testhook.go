package protobus

import (
	"github.com/ArielLaub/protobus-go/v2/internal/testhook"
	"github.com/ArielLaub/protobus-go/v2/internal/transport"
)

func init() {
	testhook.WithDialer = func(d transport.Dialer) any { return withDialer(d) }
}
