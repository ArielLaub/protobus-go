// Package gopeer is the Go participant of the cross-language suite: the
// interop services, implemented exactly as the TypeScript and Python peers
// implement them (crosslang/peers), and the canonical values every peer
// produces and expects.
package gopeer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/crosslang/gen/interop"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

// Lang names this peer in events and Who replies.
const Lang = "go"

// Canonical is the Balance every peer returns for an ordinary account.
func Canonical() *interop.Balance {
	pow := func(base, exp int64) *big.Int { return new(big.Int).Exp(big.NewInt(base), big.NewInt(exp), nil) }
	return &interop.Balance{
		Amount:   pbtypes.MustBigint(pow(10, 30)),
		AsOf:     pbtypes.NewTimestamp(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
		Big:      9007199254740993,
		Tags:     []string{"a", "b"},
		Counts:   map[string]int32{"x": 1, "y": 2},
		Balances: map[string]*pbtypes.Bigint{"k": pbtypes.MustBigint(pow(2, 200))},
		Parts:    []*pbtypes.Bigint{pbtypes.BigintFromUint64(1), pbtypes.BigintFromUint64(2), pbtypes.BigintFromUint64(3)},
		Kind:     interop.Kind_KIND_FUTURE,
		Inner: &interop.Inner{Name: "root", Value: pbtypes.BigintFromUint64(7), Children: []*interop.Inner{
			{Name: "leaf", Value: pbtypes.BigintFromUint64(8)},
		}},
		Ubig:        18446744073709551615,
		Blob:        []byte{0, 1, 255},
		Ratio:       0.5,
		Flag:        true,
		Neg:         -5,
		BeforeEpoch: pbtypes.NewTimestamp(time.Date(1969, 7, 20, 20, 17, 40, 0, time.UTC)),
		Zero:        0,
	}
}

// Ping2to70 is the n every peer sends in the event round trip: beyond 64
// bits, so only the bigint encoding can carry it.
func Ping2to70() *big.Int { return new(big.Int).Lsh(big.NewInt(1), 70) }

type counter struct {
	interop.UnimplementedCounterServer
	mu       sync.Mutex
	produced interop.Produced
}

func (c *counter) Add(_ context.Context, in *interop.AddRequest) (*interop.AddResponse, error) {
	return &interop.AddResponse{Sum: in.A + in.B}, nil
}

func (c *counter) Tick(ctx context.Context, in *interop.TickRequest, s protobus.ServerStream[*interop.Tick]) error {
	if in.EmitNothing {
		return nil
	}
	c.mu.Lock()
	c.produced = interop.Produced{}
	c.mu.Unlock()
	record := func(f func(p *interop.Produced)) {
		c.mu.Lock()
		f(&c.produced)
		c.mu.Unlock()
	}
	for i := range in.Count {
		if in.FailAt > 0 && i >= in.FailAt {
			if in.Unhandled {
				return errors.New("stream broke")
			}
			return protobus.NewHandledError("TEST_FAIL", fmt.Sprintf("deliberate failure at chunk %d", i))
		}
		if ctx.Err() != nil {
			record(func(p *interop.Produced) { p.StoppedEarly = true })
			return nil
		}
		if err := s.Send(&interop.Tick{Seq: i, Payload: fmt.Sprintf("chunk-%d", i)}); err != nil {
			record(func(p *interop.Produced) { p.StoppedEarly = true })
			return err
		}
		record(func(p *interop.Produced) { p.Yielded++ })
		if in.DelayMs > 0 {
			select {
			case <-time.After(time.Duration(in.DelayMs) * time.Millisecond):
			case <-ctx.Done():
				record(func(p *interop.Produced) { p.StoppedEarly = true })
				return nil
			}
		}
	}
	record(func(p *interop.Produced) { p.Finished = true })
	return nil
}

func (c *counter) Produced(context.Context, *interop.Nothing) (*interop.Produced, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := interop.Produced{Yielded: c.produced.Yielded, StoppedEarly: c.produced.StoppedEarly, Finished: c.produced.Finished}
	return &p, nil
}

func (c *counter) Whoami(ctx context.Context, _ *interop.Nothing) (*interop.Who, error) {
	ci, _ := protobus.CallInfoFromContext(ctx)
	return &interop.Who{Actor: ci.Actor, MessageId: ci.MessageID, RoutingKey: ci.RoutingKey, Lang: Lang}, nil
}

type wallet struct {
	interop.UnimplementedWalletServer
}

func (wallet) Balance(_ context.Context, in *interop.Query) (*interop.Balance, error) {
	switch in.Account {
	case "boom":
		return nil, protobus.NewHandledError("NOT_FOUND", "no such account")
	case "crash":
		return nil, errors.New("kaboom")
	}
	return Canonical(), nil
}

func (wallet) Echo(_ context.Context, in *interop.Balance) (*interop.Balance, error) { return in, nil }

type flaky struct {
	interop.UnimplementedFlakyServer
	bus *protobus.Bus
}

func (f flaky) Fail(ctx context.Context, _ *interop.FailRequest) (*interop.Nothing, error) {
	ci, _ := protobus.CallInfoFromContext(ctx)
	if err := f.bus.PublishEvent(ctx, &interop.Attempted{Lang: Lang, MessageId: ci.MessageID}, protobus.WithTopic("EVENT.attempted")); err != nil {
		return nil, err
	}
	return nil, errors.New("flaky " + Lang)
}

// FlakyRetry is the retry policy every peer declares interop.Flaky with. The
// queues are shared, so the arguments must be equivalent in every language.
var FlakyRetry = protobus.RetryPolicy{MaxRetries: 3, Delay: 100 * time.Millisecond}

// Serve registers and starts every interop service on bus.
func Serve(ctx context.Context, bus *protobus.Bus) ([]*protobus.Service, error) {
	c := &counter{}
	var services []*protobus.Service
	add := func(svc *protobus.Service, err error) error {
		if err != nil {
			return err
		}
		services = append(services, svc)
		return nil
	}
	if err := add(interop.RegisterCounterServer(bus, c)); err != nil {
		return nil, err
	}
	if err := add(interop.RegisterCounterServer(bus, c, protobus.WithInstance("inst1"))); err != nil {
		return nil, err
	}
	if err := add(interop.RegisterWalletServer(bus, wallet{}, protobus.WithRetry(protobus.RetryPolicy{}))); err != nil {
		return nil, err
	}
	if err := add(interop.RegisterFlakyServer(bus, flaky{bus: bus}, protobus.WithRetry(FlakyRetry), protobus.WithMaxConcurrent(4))); err != nil {
		return nil, err
	}
	listener, err := interop.RegisterListenerServer(bus, struct {
		interop.UnimplementedListenerServer
	}{},
		protobus.WithInstance(Lang), protobus.WithRetry(protobus.RetryPolicy{}))
	if err := add(listener, err); err != nil {
		return nil, err
	}
	// Echo every ping addressed to this peer as a pong, one beyond n.
	err = protobus.Subscribe(ctx, listener.Events(), func(ctx context.Context, p *interop.Ping, _ protobus.EventInfo) error {
		n, err := p.N.BigInt()
		if err != nil {
			return err
		}
		next, err := pbtypes.NewBigint(n.Add(n, big.NewInt(1)))
		if err != nil {
			return err
		}
		return bus.PublishEvent(ctx, &interop.Ping{Id: "pong:" + p.Id, N: next, From: Lang}, protobus.WithTopic("EVENT.pong."+Lang))
	}, protobus.WithTopic("EVENT.ping."+Lang))
	if err != nil {
		return nil, err
	}
	for _, s := range services {
		if err := s.Start(ctx); err != nil {
			return nil, err
		}
	}
	return services, nil
}
