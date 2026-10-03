package game

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/combat/gen/combat"
)

// Strategies are the contestants, in turn order.
var Strategies = []Strategy{Vindicator{}, BullyHunter{}, GiantSlayer{}, Equalizer{}, Wildcard{}, Terminator{}}

// Result is how a game ended.
type Result struct {
	Winner   string
	Statuses []*combat.GetStatusResponse
}

// Play runs one game to its end on bus and narrates it to out.
func Play(ctx context.Context, bus *protobus.Bus, out io.Writer, seed uint64) (Result, error) {
	say := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }
	rule := strings.Repeat("=", 60)
	say("%s\nCOMBAT GAME - Battle Royale!\n%s", rule, rule)

	// Hear the result like any other subscriber.
	results, err := bus.NewEventListener("")
	if err != nil {
		return Result{}, err
	}
	defer results.Close()
	over := make(chan *combat.GameOver, 1)
	err = protobus.Subscribe(ctx, results, func(_ context.Context, ev *combat.GameOver, _ protobus.EventInfo) error {
		select {
		case over <- ev:
		default:
		}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	if err := results.Start(ctx); err != nil {
		return Result{}, err
	}

	var players []*Player
	var order []string
	for i, s := range Strategies {
		id := fmt.Sprintf("player%d", i+1)
		p := NewPlayer(bus, id, s, seed+uint64(i), func(f string, a ...any) { say("  "+f, a...) })
		// Each player is an instance of the one Combat.Player contract.
		svc, err := combat.RegisterPlayerServer(bus, p, protobus.AsInstance(id))
		if err != nil {
			return Result{}, err
		}
		if err := p.Subscribe(ctx, svc.Events()); err != nil {
			return Result{}, err
		}
		if err := svc.Start(ctx); err != nil {
			return Result{}, err
		}
		defer svc.StopConsuming(context.Background())
		players = append(players, p)
		order = append(order, id)
		say("  joined: %s (%s)", p.Name(), id)
	}
	for _, p := range players {
		for _, o := range players {
			p.Meet(o.ID, o.Name())
		}
		if err := bus.PublishEvent(ctx, &combat.PlayerJoined{PlayerId: p.ID, PlayerName: p.Name(), Health: StartingHealth}); err != nil {
			return Result{}, err
		}
	}
	say("Turn order: %s\n%s\nLET THE BATTLE BEGIN!\n%s", strings.Join(order, " -> "), rule, rule)

	// Index 0 is initiated last: its first turn passes the turn on, and every
	// other player must know the order by then.
	for i := len(order) - 1; i >= 0; i-- {
		c := combat.NewPlayerClient(bus, protobus.ForInstance(order[i]))
		if _, err := c.InitiateGame(ctx, &combat.InitiateGameRequest{PlayerOrder: order, MyIndex: int32(i)}); err != nil {
			return Result{}, err
		}
	}

	var res Result
	select {
	case ev := <-over:
		res.Winner = ev.WinnerName
	case <-time.After(2 * time.Minute):
		return Result{}, errors.New("the game did not finish")
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}

	say("%s\nFINAL RESULTS\n%s", rule, rule)
	for _, id := range order {
		st, err := combat.NewPlayerClient(bus, protobus.ForInstance(id)).GetStatus(ctx, &combat.GetStatusRequest{})
		if err != nil {
			return Result{}, err
		}
		res.Statuses = append(res.Statuses, st)
		state := "eliminated"
		if st.Alive {
			state = "WINNER"
		}
		say("  %-18s %2d HP  (%s)", st.PlayerName, st.Health, state)
	}
	return res, nil
}
