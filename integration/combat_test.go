package integration_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ArielLaub/protobus-go/v2/examples/combat/game"
	"github.com/ArielLaub/protobus-go/v2/internal/brokertest"
)

// The combat example as a smoke test, as the other ports run theirs in CI:
// six instance-named services, RPC between them and a turn order carried
// entirely by events, played to the end.
func TestCombatGameHasExactlyOneWinner(t *testing.T) {
	broker := brokertest.Require(t)
	for seed := uint64(1); seed <= 3; seed++ {
		vh := broker.NewVHost(t)
		bus := dial(t, vh.URL())
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		res, err := game.Play(ctx, bus, io.Discard, seed)
		cancel()
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		alive := 0
		for _, s := range res.Statuses {
			if s.Alive {
				alive++
				if s.PlayerName != res.Winner {
					t.Errorf("seed %d: %s is alive but %s won", seed, s.PlayerName, res.Winner)
				}
			}
		}
		if alive != 1 {
			t.Fatalf("seed %d: %d players alive at the end", seed, alive)
		}
	}
}
