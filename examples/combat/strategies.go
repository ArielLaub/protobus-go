package main

import (
	"math"
	"math/rand"

	protobus "github.com/ArielLaub/protobus-go"
)

// Strategy 1: The Vindicator
// Shoots back at whoever shot them last. If nobody has attacked them yet, picks randomly.
// "An eye for an eye!"
func VindicatorStrategy(alivePlayers []*PlayerState, health int, gameState *GameState) *PlayerState {
	if len(alivePlayers) == 0 {
		return nil
	}

	gameState.mu.RLock()
	lastAttacker := gameState.LastAttacker
	gameState.mu.RUnlock()

	// If someone attacked us, shoot them back
	if lastAttacker != "" {
		for _, p := range alivePlayers {
			if p.ID == lastAttacker {
				return p
			}
		}
	}

	// Otherwise, pick randomly
	return alivePlayers[rand.Intn(len(alivePlayers))]
}

// Strategy 2: The Bully Hunter
// Always targets the weakest player (lowest health).
// "Pick on someone your own size... wait, I mean pick on the smallest!"
func BullyHunterStrategy(alivePlayers []*PlayerState, health int, gameState *GameState) *PlayerState {
	if len(alivePlayers) == 0 {
		return nil
	}

	weakest := alivePlayers[0]
	for _, p := range alivePlayers[1:] {
		if p.Health < weakest.Health {
			weakest = p
		}
	}
	return weakest
}

// Strategy 3: The Giant Slayer
// Always targets the strongest player (highest health).
// "The bigger they are, the harder they fall!"
func GiantSlayerStrategy(alivePlayers []*PlayerState, health int, gameState *GameState) *PlayerState {
	if len(alivePlayers) == 0 {
		return nil
	}

	strongest := alivePlayers[0]
	for _, p := range alivePlayers[1:] {
		if p.Health > strongest.Health {
			strongest = p
		}
	}
	return strongest
}

// Strategy 4: The Equalizer
// Targets the player with health most similar to their own.
// "Let's keep things fair and square!"
func EqualizerStrategy(alivePlayers []*PlayerState, health int, gameState *GameState) *PlayerState {
	if len(alivePlayers) == 0 {
		return nil
	}

	var closest *PlayerState
	smallestDiff := math.MaxInt32

	for _, p := range alivePlayers {
		diff := int(math.Abs(float64(p.Health - health)))
		if diff < smallestDiff {
			smallestDiff = diff
			closest = p
		}
	}
	return closest
}

// Strategy 5: The Wildcard
// Picks a random target every time.
// "Chaos is a ladder... or something!"
func WildcardStrategy(alivePlayers []*PlayerState, health int, gameState *GameState) *PlayerState {
	if len(alivePlayers) == 0 {
		return nil
	}

	return alivePlayers[rand.Intn(len(alivePlayers))]
}

// Strategy 6: The Terminator
// Picks one target and relentlessly attacks until they're dead, then moves on.
// "I'll be back... for you specifically!"
func TerminatorStrategy(alivePlayers []*PlayerState, health int, gameState *GameState) *PlayerState {
	if len(alivePlayers) == 0 {
		return nil
	}

	gameState.mu.RLock()
	focusTarget := gameState.FocusTarget
	gameState.mu.RUnlock()

	// If we have a focus target and they're still alive, keep shooting
	if focusTarget != "" {
		for _, p := range alivePlayers {
			if p.ID == focusTarget {
				return p
			}
		}
		// Focus target is dead, clear it
		gameState.mu.Lock()
		gameState.FocusTarget = ""
		gameState.mu.Unlock()
	}

	// Pick a new focus target (randomly)
	newTarget := alivePlayers[rand.Intn(len(alivePlayers))]
	gameState.mu.Lock()
	gameState.FocusTarget = newTarget.ID
	gameState.mu.Unlock()

	return newTarget
}

// Player constructors for each strategy

func NewVindicator(ctx *protobus.Context, playerID string) *CombatPlayer {
	return NewCombatPlayer(ctx, playerID, "The Vindicator", VindicatorStrategy)
}

func NewBullyHunter(ctx *protobus.Context, playerID string) *CombatPlayer {
	return NewCombatPlayer(ctx, playerID, "The Bully Hunter", BullyHunterStrategy)
}

func NewGiantSlayer(ctx *protobus.Context, playerID string) *CombatPlayer {
	return NewCombatPlayer(ctx, playerID, "The Giant Slayer", GiantSlayerStrategy)
}

func NewEqualizer(ctx *protobus.Context, playerID string) *CombatPlayer {
	return NewCombatPlayer(ctx, playerID, "The Equalizer", EqualizerStrategy)
}

func NewWildcard(ctx *protobus.Context, playerID string) *CombatPlayer {
	return NewCombatPlayer(ctx, playerID, "The Wildcard", WildcardStrategy)
}

func NewTerminator(ctx *protobus.Context, playerID string) *CombatPlayer {
	return NewCombatPlayer(ctx, playerID, "The Terminator", TerminatorStrategy)
}
