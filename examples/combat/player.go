// Combat game example demonstrating protobus-go RPC gameplay
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"

	protobus "github.com/ArielLaub/protobus-go"
)

// PlayerState tracks another player's status
type PlayerState struct {
	ID     string
	Name   string
	Health int
	Alive  bool
}

// GameState tracks the overall game state for a player
type GameState struct {
	Players      map[string]*PlayerState
	PlayerOrder  []string
	MyIndex      int
	LastAttacker string
	FocusTarget  string
	GameStarted  bool
	GameOver     bool
	mu           sync.RWMutex
}

// TargetChooser defines the strategy for choosing a target
type TargetChooser func(alivePlayers []*PlayerState, health int, gameState *GameState) *PlayerState

// CombatPlayer is the base implementation for combat game players
type CombatPlayer struct {
	*protobus.BaseService
	ctx          *protobus.Context
	playerID     string
	playerName   string
	health       int
	gameState    *GameState
	chooseTarget TargetChooser
	healthMu     sync.RWMutex
	gameRunner   *GameRunner // Reference to game runner for callbacks
	proxies      map[string]*protobus.ServiceProxy
	proxiesMu    sync.RWMutex
}

// NewCombatPlayer creates a new combat player
func NewCombatPlayer(ctx *protobus.Context, playerID, playerName string, chooser TargetChooser) *CombatPlayer {
	serviceName := fmt.Sprintf("Combat.Player.%s", playerID)
	p := &CombatPlayer{
		BaseService: protobus.NewBaseService(ctx, serviceName, "", nil),
		ctx:         ctx,
		playerID:    playerID,
		playerName:  playerName,
		health:      10,
		gameState: &GameState{
			Players:     make(map[string]*PlayerState),
			PlayerOrder: nil,
			MyIndex:     -1,
		},
		chooseTarget: chooser,
		proxies:      make(map[string]*protobus.ServiceProxy),
	}

	// Register RPC handlers using Handle
	p.Handle("shoot", p.Shoot)
	p.Handle("initiateGame", p.InitiateGame)
	p.Handle("getStatus", p.GetStatus)
	p.Handle("notifyShot", p.NotifyShot)
	p.Handle("notifyDeath", p.NotifyDeath)
	p.Handle("notifyGameOver", p.NotifyGameOver)

	return p
}

// SetGameRunner sets the game runner reference
func (p *CombatPlayer) SetGameRunner(gr *GameRunner) {
	p.gameRunner = gr
}

// GetPlayerID returns the player ID
func (p *CombatPlayer) GetPlayerID() string {
	return p.playerID
}

// GetPlayerName returns the player name
func (p *CombatPlayer) GetPlayerName() string {
	return p.playerName
}

// GetHealth returns current health (thread-safe)
func (p *CombatPlayer) GetHealth() int {
	p.healthMu.RLock()
	defer p.healthMu.RUnlock()
	return p.health
}

// Init initializes the player
func (p *CombatPlayer) Init() error {
	if err := p.BaseService.Init(); err != nil {
		return err
	}
	log.Printf("%s (%s) joined the game", p.playerName, p.playerID)
	return nil
}

// RegisterPlayer manually registers another player
func (p *CombatPlayer) RegisterPlayer(playerID, playerName string, health int) {
	if playerID != p.playerID {
		p.gameState.mu.Lock()
		p.gameState.Players[playerID] = &PlayerState{
			ID:     playerID,
			Name:   playerName,
			Health: health,
			Alive:  health > 0,
		}
		p.gameState.mu.Unlock()
	}
}

// Shoot handles being shot at by another player
func (p *CombatPlayer) Shoot(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	p.healthMu.Lock()
	currentHealth := p.health

	if currentHealth <= 0 {
		p.healthMu.Unlock()
		return map[string]interface{}{"hit": false, "remainingHealth": float64(0)}, nil
	}

	// 50/50 chance to hit
	hit := rand.Float64() < 0.5

	shooterID := data["shooterId"].(string)

	// Get shooter's name
	p.gameState.mu.RLock()
	shooter := p.gameState.Players[shooterID]
	shooterName := shooterID
	if shooter != nil {
		shooterName = shooter.Name
	}
	p.gameState.mu.RUnlock()

	if hit {
		p.health--
		currentHealth = p.health
		p.healthMu.Unlock()

		p.gameState.mu.Lock()
		p.gameState.LastAttacker = shooterID
		p.gameState.mu.Unlock()

		log.Printf("%s was hit by %s! Health: %d", p.playerName, shooterName, currentHealth)

		// Notify game runner of shot
		if p.gameRunner != nil {
			p.gameRunner.NotifyShot(shooterID, p.playerID, true, currentHealth)
		}

		if currentHealth <= 0 {
			log.Printf("%s has been eliminated!", p.playerName)
			if p.gameRunner != nil {
				p.gameRunner.NotifyDeath(p.playerID, shooterID)
			}
		}
	} else {
		p.healthMu.Unlock()
		log.Printf("%s dodged attack from %s!", p.playerName, shooterName)
		if p.gameRunner != nil {
			p.gameRunner.NotifyShot(shooterID, p.playerID, false, currentHealth)
		}
	}

	return map[string]interface{}{"hit": hit, "remainingHealth": float64(currentHealth)}, nil
}

// InitiateGame starts the game for this player
func (p *CombatPlayer) InitiateGame(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	playerOrderRaw := data["playerOrder"].([]interface{})
	playerOrder := make([]string, len(playerOrderRaw))
	for i, v := range playerOrderRaw {
		playerOrder[i] = v.(string)
	}
	myIndex := int(data["myIndex"].(float64))

	p.gameState.mu.Lock()
	p.gameState.PlayerOrder = playerOrder
	p.gameState.MyIndex = myIndex
	p.gameState.GameStarted = true
	p.gameState.mu.Unlock()

	log.Printf("%s received game initiation. My turn index: %d", p.playerName, myIndex)

	// If we're first, take our turn
	if myIndex == 0 {
		go p.takeTurn()
	}

	return map[string]interface{}{"success": true}, nil
}

// GetStatus returns current player status
func (p *CombatPlayer) GetStatus(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	health := p.GetHealth()
	return map[string]interface{}{
		"playerId":   p.playerID,
		"playerName": p.playerName,
		"health":     float64(health),
		"alive":      health > 0,
	}, nil
}

// NotifyShot notifies player of a shot (for updating game state)
func (p *CombatPlayer) NotifyShot(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	targetID := data["targetId"].(string)
	hit := data["hit"].(bool)
	targetHealth := int(data["targetHealth"].(float64))

	p.gameState.mu.Lock()
	if player, ok := p.gameState.Players[targetID]; ok {
		player.Health = targetHealth
		player.Alive = targetHealth > 0
	}
	if targetID == p.playerID && hit {
		p.gameState.LastAttacker = data["shooterId"].(string)
	}
	p.gameState.mu.Unlock()

	return map[string]interface{}{"ok": true}, nil
}

// NotifyDeath notifies player of a death
func (p *CombatPlayer) NotifyDeath(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	playerID := data["playerId"].(string)

	p.gameState.mu.Lock()
	if player, ok := p.gameState.Players[playerID]; ok {
		player.Alive = false
		player.Health = 0
	}
	if p.gameState.FocusTarget == playerID {
		p.gameState.FocusTarget = ""
	}
	p.gameState.mu.Unlock()

	return map[string]interface{}{"ok": true}, nil
}

// NotifyGameOver notifies player that game is over
func (p *CombatPlayer) NotifyGameOver(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {
	p.gameState.mu.Lock()
	p.gameState.GameOver = true
	p.gameState.mu.Unlock()

	winnerID := data["winnerId"].(string)
	if winnerID == p.playerID {
		log.Printf("[WINNER] %s WINS THE GAME!", p.playerName)
	}

	return map[string]interface{}{"ok": true}, nil
}

// IsWinner returns true if this player won
func (p *CombatPlayer) IsWinner() bool {
	p.gameState.mu.RLock()
	gameOver := p.gameState.GameOver
	p.gameState.mu.RUnlock()

	return p.GetHealth() > 0 && gameOver
}

// IsGameOver returns true if the game has ended
func (p *CombatPlayer) IsGameOver() bool {
	p.gameState.mu.RLock()
	defer p.gameState.mu.RUnlock()
	return p.gameState.GameOver
}

// getAlivePlayers returns list of alive players (excluding self)
func (p *CombatPlayer) getAlivePlayers() []*PlayerState {
	p.gameState.mu.RLock()
	defer p.gameState.mu.RUnlock()

	var alive []*PlayerState
	for _, player := range p.gameState.Players {
		if player.Alive && player.ID != p.playerID {
			alive = append(alive, player)
		}
	}
	return alive
}

// getNextAlivePlayerIndex finds the next alive player's index
func (p *CombatPlayer) getNextAlivePlayerIndex() int {
	p.gameState.mu.RLock()
	order := p.gameState.PlayerOrder
	players := p.gameState.Players
	p.gameState.mu.RUnlock()

	myIndex := -1
	for i, id := range order {
		if id == p.playerID {
			myIndex = i
			break
		}
	}

	for i := 1; i <= len(order); i++ {
		nextIndex := (myIndex + i) % len(order)
		nextPlayerID := order[nextIndex]

		if nextPlayerID == p.playerID {
			continue
		}

		p.gameState.mu.RLock()
		player := players[nextPlayerID]
		p.gameState.mu.RUnlock()

		if player != nil && player.Alive {
			return nextIndex
		}
	}

	return (myIndex + 1) % len(order)
}

// takeTurn executes this player's turn
func (p *CombatPlayer) takeTurn() {
	p.gameState.mu.RLock()
	gameOver := p.gameState.GameOver
	p.gameState.mu.RUnlock()

	if gameOver || p.GetHealth() <= 0 {
		return
	}

	alivePlayers := p.getAlivePlayers()

	p.gameState.mu.RLock()
	numKnownPlayers := len(p.gameState.Players)
	p.gameState.mu.RUnlock()

	if numKnownPlayers == 0 {
		log.Printf("%s doesn't know about other players yet, ending turn", p.playerName)
		p.endTurn()
		return
	}

	if len(alivePlayers) == 0 {
		// We win!
		log.Printf("%s is the last one standing!", p.playerName)
		if p.gameRunner != nil {
			p.gameRunner.NotifyGameOver(p.playerID, p.playerName)
		}
		return
	}

	// Choose target
	target := p.chooseTarget(alivePlayers, p.GetHealth(), p.gameState)
	if target == nil {
		log.Printf("%s couldn't find a target!", p.playerName)
		p.endTurn()
		return
	}

	log.Printf("%s shoots at %s!", p.playerName, target.Name)

	// Call shoot on target player
	result, err := p.callPlayerMethod(target.ID, "shoot", map[string]interface{}{
		"shooterId": p.playerID,
	})

	if err != nil {
		log.Printf("%s failed to shoot: %v", p.playerName, err)
	} else if result != nil {
		remainingHealth := int(result["remainingHealth"].(float64))
		if remainingHealth <= 0 {
			p.gameState.mu.Lock()
			if targetPlayer, ok := p.gameState.Players[target.ID]; ok {
				targetPlayer.Health = 0
				targetPlayer.Alive = false
			}
			p.gameState.mu.Unlock()
		}
	}

	// Check if game is over
	stillAlive := p.getAlivePlayers()
	if len(stillAlive) == 0 {
		log.Printf("%s is the last one standing!", p.playerName)
		if p.gameRunner != nil {
			p.gameRunner.NotifyGameOver(p.playerID, p.playerName)
		}
		return
	}

	p.endTurn()
}

func (p *CombatPlayer) endTurn() {
	if p.gameRunner != nil {
		nextIndex := p.getNextAlivePlayerIndex()
		p.gameRunner.NotifyTurnComplete(p.playerID, nextIndex)
	}
}

// getOrCreateProxy gets or creates a proxy for another player
func (p *CombatPlayer) getOrCreateProxy(targetPlayerID string) (*protobus.ServiceProxy, error) {
	serviceName := fmt.Sprintf("Combat.Player.%s", targetPlayerID)

	p.proxiesMu.RLock()
	proxy, exists := p.proxies[targetPlayerID]
	p.proxiesMu.RUnlock()

	if exists {
		return proxy, nil
	}

	p.proxiesMu.Lock()
	defer p.proxiesMu.Unlock()

	// Double-check after acquiring write lock
	if proxy, exists = p.proxies[targetPlayerID]; exists {
		return proxy, nil
	}

	proxy = protobus.NewServiceProxy(p.ctx, serviceName)
	if err := proxy.Init(); err != nil {
		return nil, fmt.Errorf("failed to init proxy: %w", err)
	}

	p.proxies[targetPlayerID] = proxy
	return proxy, nil
}

// callPlayerMethod calls an RPC method on another player
func (p *CombatPlayer) callPlayerMethod(targetPlayerID, method string, data map[string]interface{}) (map[string]interface{}, error) {
	proxy, err := p.getOrCreateProxy(targetPlayerID)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	err = proxy.Call(context.Background(), method, data, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
