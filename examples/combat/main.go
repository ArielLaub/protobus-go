// Combat Game - Battle Royale example demonstrating protobus-go
// RPC-based gameplay with multiple player strategies
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	protobus "github.com/ArielLaub/protobus-go"
)

// GameRunner coordinates the game between players
type GameRunner struct {
	players     []*CombatPlayer
	playerOrder []string
	gameOver    bool
	winner      *CombatPlayer
	mu          sync.RWMutex
}

// NewGameRunner creates a new game runner
func NewGameRunner(players []*CombatPlayer) *GameRunner {
	playerOrder := make([]string, len(players))
	for i, p := range players {
		playerOrder[i] = p.GetPlayerID()
	}

	gr := &GameRunner{
		players:     players,
		playerOrder: playerOrder,
	}

	// Set game runner reference on all players
	for _, p := range players {
		p.SetGameRunner(gr)
	}

	return gr
}

// NotifyShot broadcasts a shot event to all players
func (gr *GameRunner) NotifyShot(shooterID, targetID string, hit bool, targetHealth int) {
	// Update all players' game state
	for _, p := range gr.players {
		p.gameState.mu.Lock()
		if player, ok := p.gameState.Players[targetID]; ok {
			player.Health = targetHealth
			player.Alive = targetHealth > 0
		}
		p.gameState.mu.Unlock()
	}
}

// NotifyDeath broadcasts a death event to all players
func (gr *GameRunner) NotifyDeath(playerID, killedBy string) {
	for _, p := range gr.players {
		p.gameState.mu.Lock()
		if player, ok := p.gameState.Players[playerID]; ok {
			player.Alive = false
			player.Health = 0
		}
		if p.gameState.FocusTarget == playerID {
			p.gameState.FocusTarget = ""
		}
		p.gameState.mu.Unlock()
	}
}

// NotifyTurnComplete handles turn completion and triggers next player
func (gr *GameRunner) NotifyTurnComplete(playerID string, nextIndex int) {
	gr.mu.RLock()
	if gr.gameOver {
		gr.mu.RUnlock()
		return
	}
	gr.mu.RUnlock()

	// Find and trigger next player
	if nextIndex >= 0 && nextIndex < len(gr.players) {
		nextPlayer := gr.players[nextIndex]
		if nextPlayer.GetHealth() > 0 {
			go nextPlayer.takeTurn()
		}
	}
}

// NotifyGameOver marks the game as over
func (gr *GameRunner) NotifyGameOver(winnerID, winnerName string) {
	gr.mu.Lock()
	if gr.gameOver {
		gr.mu.Unlock()
		return
	}
	gr.gameOver = true

	// Find winner
	for _, p := range gr.players {
		if p.GetPlayerID() == winnerID {
			gr.winner = p
			break
		}
	}
	gr.mu.Unlock()

	// Notify all players
	for _, p := range gr.players {
		p.gameState.mu.Lock()
		p.gameState.GameOver = true
		p.gameState.mu.Unlock()
	}

	log.Printf("=== GAME OVER === Winner: %s", winnerName)
}

// IsGameOver returns true if the game has ended
func (gr *GameRunner) IsGameOver() bool {
	gr.mu.RLock()
	defer gr.mu.RUnlock()
	return gr.gameOver
}

// GetWinner returns the winner
func (gr *GameRunner) GetWinner() *CombatPlayer {
	gr.mu.RLock()
	defer gr.mu.RUnlock()
	return gr.winner
}

func main() {
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("COMBAT GAME - Battle Royale!")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println()

	// Create context
	ctx := protobus.NewContext(nil)
	if err := ctx.Init(rabbitURL); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer ctx.Close()

	// Parse schema for encoding
	ctx.Factory().Parse("", "Combat.Player")

	// Create all players with different strategies
	players := []*CombatPlayer{
		NewVindicator(ctx, "player1"),
		NewBullyHunter(ctx, "player2"),
		NewGiantSlayer(ctx, "player3"),
		NewEqualizer(ctx, "player4"),
		NewWildcard(ctx, "player5"),
		NewTerminator(ctx, "player6"),
	}

	fmt.Println("Players joining the arena:")
	fmt.Println(strings.Repeat("-", 40))

	// Initialize all players
	for _, player := range players {
		if err := player.Init(); err != nil {
			log.Fatalf("Failed to init player %s: %v", player.GetPlayerName(), err)
		}
		fmt.Printf("  [*] %s (%s)\n", player.GetPlayerName(), player.GetPlayerID())
	}

	fmt.Println(strings.Repeat("-", 40))
	fmt.Println()

	// Register all players with each other
	for _, player := range players {
		for _, other := range players {
			if player != other {
				player.RegisterPlayer(other.GetPlayerID(), other.GetPlayerName(), 10)
			}
		}
	}

	// Create game runner
	gameRunner := NewGameRunner(players)

	// Determine turn order
	playerOrder := make([]string, len(players))
	for i, p := range players {
		playerOrder[i] = p.GetPlayerID()
	}

	fmt.Printf("Turn order: %s\n", strings.Join(playerOrder, " -> "))
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("LET THE BATTLE BEGIN!")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println()

	// Start the game - tell each player the order and their position
	for i, player := range players {
		orderInterface := make([]interface{}, len(playerOrder))
		for j, id := range playerOrder {
			orderInterface[j] = id
		}
		player.InitiateGame(nil, map[string]interface{}{
			"playerOrder": orderInterface,
			"myIndex":     float64(i),
		}, "", "")
	}

	// Wait for game to complete
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for !gameRunner.IsGameOver() {
		select {
		case <-timeout:
			fmt.Println("Game timeout!")
			goto results
		case <-ticker.C:
			// Continue checking
		}
	}

results:
	// Small delay to let final events propagate
	time.Sleep(100 * time.Millisecond)

	// Print final results
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("FINAL RESULTS")
	fmt.Println(strings.Repeat("=", 60))

	for _, player := range players {
		health := player.GetHealth()
		status := "[X]"
		suffix := "(eliminated)"
		if health > 0 {
			status := "[*]"
			if player.IsWinner() {
				suffix = "(WINNER!)"
			} else {
				suffix = ""
			}
			fmt.Printf("  %s %s: %d HP %s\n", status, player.GetPlayerName(), health, suffix)
		} else {
			fmt.Printf("  %s %s: %d HP %s\n", status, player.GetPlayerName(), health, suffix)
		}
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println()
	fmt.Println("Game ended. Goodbye!")
}
