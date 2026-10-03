// Package game is a battle royale played over protobus: every player is its
// own service instance (Combat.Player.<id>), players shoot each other by RPC,
// and everything else (joins, hits, deaths, turns, the winner) travels as
// events every player subscribes to.
//
// It is a port of the TypeScript and Python combat samples, and runs against
// their players unchanged: the schema is the same file.
package game

import (
	"cmp"
	"context"
	"math/rand/v2"
	"slices"
	"sync"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/combat/gen/combat"
)

// StartingHealth is every player's health when the game begins.
const StartingHealth = 10

// Opponent is what a player knows about another.
type Opponent struct {
	ID     string
	Name   string
	Health int32
	Alive  bool
}

// Strategy picks whom to shoot. It runs under the player's lock and must not
// block.
type Strategy interface {
	Name() string
	Choose(p *View, alive []Opponent) (Opponent, bool)
}

// View is the slice of a player's state a strategy may read and keep.
type View struct {
	Health       int32
	LastAttacker string
	Focus        string // strategies that hold a grudge store it here
	Rand         *rand.Rand
}

// Player is one contestant: a Combat.Player service instance.
type Player struct {
	combat.UnimplementedPlayerServer

	ID       string
	strategy Strategy
	bus      *protobus.Bus
	log      func(format string, args ...any)

	// Handlers run on their own goroutines (requests and events are separate
	// consumers), so the state is guarded, unlike the single-threaded
	// TypeScript original.
	mu       sync.Mutex
	view     View
	others   map[string]*Opponent
	order    []string
	gameOver bool
}

// NewPlayer returns a player with a strategy.
func NewPlayer(bus *protobus.Bus, id string, s Strategy, seed uint64, log func(string, ...any)) *Player {
	return &Player{
		ID: id, strategy: s, bus: bus, log: log,
		view:   View{Health: StartingHealth, Rand: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))},
		others: map[string]*Opponent{},
	}
}

// Name is the strategy's name.
func (p *Player) Name() string { return p.strategy.Name() }

// Shoot is the RPC another player calls to fire at this one.
func (p *Player) Shoot(ctx context.Context, in *combat.ShootRequest) (*combat.ShootResponse, error) {
	p.mu.Lock()
	if p.view.Health <= 0 {
		p.mu.Unlock()
		return &combat.ShootResponse{Hit: false, RemainingHealth: 0}, nil
	}
	hit := p.view.Rand.IntN(2) == 0
	if hit {
		p.view.Health--
		p.view.LastAttacker = in.ShooterId
	}
	health := p.view.Health
	shooter := in.ShooterId
	if o, ok := p.others[in.ShooterId]; ok {
		shooter = o.Name
	}
	p.mu.Unlock()

	if hit {
		p.log("%s was hit by %s! Health: %d", p.Name(), shooter, health)
	} else {
		p.log("%s dodged an attack from %s!", p.Name(), shooter)
	}
	if err := p.bus.PublishEvent(ctx, &combat.PlayerShot{ShooterId: in.ShooterId, TargetId: p.ID, Hit: hit, TargetHealth: health}); err != nil {
		return nil, err
	}
	if hit && health <= 0 {
		p.log("%s has been eliminated!", p.Name())
		if err := p.bus.PublishEvent(ctx, &combat.PlayerDied{PlayerId: p.ID, KilledBy: in.ShooterId}); err != nil {
			return nil, err
		}
	}
	return &combat.ShootResponse{Hit: hit, RemainingHealth: health}, nil
}

// InitiateGame tells the player the turn order. The first player takes its
// turn straight away, on its own goroutine, so the RPC answers at once.
func (p *Player) InitiateGame(ctx context.Context, in *combat.InitiateGameRequest) (*combat.InitiateGameResponse, error) {
	p.mu.Lock()
	p.order = slices.Clone(in.PlayerOrder)
	p.mu.Unlock()
	if in.MyIndex == 0 {
		go p.takeTurn(context.WithoutCancel(ctx))
	}
	return &combat.InitiateGameResponse{Success: true}, nil
}

// GetStatus reports the player's health.
func (p *Player) GetStatus(context.Context, *combat.GetStatusRequest) (*combat.GetStatusResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &combat.GetStatusResponse{PlayerId: p.ID, PlayerName: p.Name(), Health: p.view.Health, Alive: p.view.Health > 0}, nil
}

// Meet records another player, as the PlayerJoined event would.
func (p *Player) Meet(id, name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id != p.ID {
		p.others[id] = &Opponent{ID: id, Name: name, Health: StartingHealth, Alive: true}
	}
}

// Subscribe wires the player's event handlers to its service.
func (p *Player) Subscribe(ctx context.Context, l *protobus.EventListener) error {
	subs := []error{
		protobus.Subscribe(ctx, l, func(_ context.Context, ev *combat.PlayerJoined, _ protobus.EventInfo) error {
			p.Meet(ev.PlayerId, ev.PlayerName)
			return nil
		}),
		protobus.Subscribe(ctx, l, func(_ context.Context, ev *combat.PlayerShot, _ protobus.EventInfo) error {
			p.mu.Lock()
			defer p.mu.Unlock()
			if o, ok := p.others[ev.TargetId]; ok {
				o.Health, o.Alive = ev.TargetHealth, ev.TargetHealth > 0
			}
			return nil
		}),
		protobus.Subscribe(ctx, l, func(_ context.Context, ev *combat.PlayerDied, _ protobus.EventInfo) error {
			p.mu.Lock()
			defer p.mu.Unlock()
			if o, ok := p.others[ev.PlayerId]; ok {
				o.Health, o.Alive = 0, false
			}
			if p.view.Focus == ev.PlayerId {
				p.view.Focus = ""
			}
			return nil
		}),
		protobus.Subscribe(ctx, l, func(ctx context.Context, ev *combat.TurnComplete, _ protobus.EventInfo) error {
			p.mu.Lock()
			mine := int32(slices.Index(p.order, p.ID)) == ev.NextPlayerIndex
			p.mu.Unlock()
			if mine {
				p.takeTurn(ctx)
			}
			return nil
		}),
		protobus.Subscribe(ctx, l, func(_ context.Context, ev *combat.GameOver, _ protobus.EventInfo) error {
			p.mu.Lock()
			p.gameOver = true
			p.mu.Unlock()
			return nil
		}),
	}
	for _, err := range subs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Player) aliveOthers() []Opponent {
	var alive []Opponent
	for _, o := range p.others {
		if o.Alive {
			alive = append(alive, *o)
		}
	}
	slices.SortFunc(alive, func(a, b Opponent) int { return cmp.Compare(a.ID, b.ID) })
	return alive
}

// takeTurn shoots at the chosen target, then passes the turn on.
func (p *Player) takeTurn(ctx context.Context) {
	p.mu.Lock()
	if p.gameOver {
		p.mu.Unlock()
		return
	}
	if p.view.Health <= 0 {
		// A turn handed to a player who died meanwhile is passed on, not
		// dropped: dropping it would stall the game.
		p.mu.Unlock()
		p.endTurn(ctx)
		return
	}
	alive := p.aliveOthers()
	if len(alive) == 0 {
		p.mu.Unlock()
		p.win(ctx)
		return
	}
	target, ok := p.strategy.Choose(&p.view, alive)
	p.mu.Unlock()
	if !ok {
		p.endTurn(ctx)
		return
	}

	p.log("%s shoots at %s!", p.Name(), target.Name)
	shooter := combat.NewPlayerClient(p.bus, protobus.ForInstance(target.ID))
	res, err := shooter.Shoot(ctx, &combat.ShootRequest{ShooterId: p.ID}, protobus.WithActor(p.ID))
	if err != nil {
		p.log("%s failed to shoot: %v", p.Name(), err)
	} else if res.RemainingHealth <= 0 {
		p.mu.Lock()
		if o, ok := p.others[target.ID]; ok {
			o.Health, o.Alive = 0, false
		}
		p.mu.Unlock()
	}

	p.mu.Lock()
	last := len(p.aliveOthers()) == 0
	p.mu.Unlock()
	if last {
		p.win(ctx)
		return
	}
	p.endTurn(ctx)
}

func (p *Player) win(ctx context.Context) {
	p.log("%s is the last one standing!", p.Name())
	if err := p.bus.PublishEvent(ctx, &combat.GameOver{WinnerId: p.ID, WinnerName: p.Name()}); err != nil {
		p.log("announcing the winner: %v", err)
	}
}

func (p *Player) endTurn(ctx context.Context) {
	p.mu.Lock()
	next := p.nextAlive()
	p.mu.Unlock()
	if err := p.bus.PublishEvent(ctx, &combat.TurnComplete{PlayerId: p.ID, NextPlayerIndex: next}); err != nil {
		p.log("passing the turn: %v", err)
	}
}

// nextAlive is the order index of the next player believed alive. Caller
// holds p.mu.
func (p *Player) nextAlive() int32 {
	me := slices.Index(p.order, p.ID)
	for i := 1; i <= len(p.order); i++ {
		idx := (me + i) % len(p.order)
		if o, ok := p.others[p.order[idx]]; ok && o.Alive {
			return int32(idx)
		}
	}
	return int32((me + 1) % len(p.order))
}
