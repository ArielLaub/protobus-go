// Command combat runs the protobus battle royale: six players, each a
// Combat.Player service instance with its own strategy, shooting each other
// over RPC and coordinating through events.
//
//	docker compose up -d --wait
//	AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ go run ./examples/combat
package main

//go:generate go run ../../cmd/protobus generate -proto ./proto -out ./gen

import (
	"context"
	"log"
	"os"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/combat/game"
)

func main() {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@127.0.0.1:25672/"
	}
	ctx := context.Background()
	bus, err := protobus.Dial(ctx, url)
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()
	if _, err := game.Play(ctx, bus, os.Stdout, uint64(time.Now().UnixNano())); err != nil {
		log.Fatal(err)
	}
}
