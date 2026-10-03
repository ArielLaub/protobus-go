package game

// The six strategies of the original sample.

// Vindicator shoots back at whoever shot it last, else at random.
type Vindicator struct{}

func (Vindicator) Name() string { return "The Vindicator" }
func (Vindicator) Choose(v *View, alive []Opponent) (Opponent, bool) {
	for _, o := range alive {
		if o.ID == v.LastAttacker {
			return o, true
		}
	}
	return random(v, alive)
}

// BullyHunter targets the weakest.
type BullyHunter struct{}

func (BullyHunter) Name() string { return "The Bully Hunter" }
func (BullyHunter) Choose(_ *View, alive []Opponent) (Opponent, bool) {
	return pick(alive, func(a, b Opponent) bool { return a.Health < b.Health })
}

// GiantSlayer targets the strongest.
type GiantSlayer struct{}

func (GiantSlayer) Name() string { return "The Giant Slayer" }
func (GiantSlayer) Choose(_ *View, alive []Opponent) (Opponent, bool) {
	return pick(alive, func(a, b Opponent) bool { return a.Health > b.Health })
}

// Equalizer targets whoever's health is closest to its own.
type Equalizer struct{}

func (Equalizer) Name() string { return "The Equalizer" }
func (Equalizer) Choose(v *View, alive []Opponent) (Opponent, bool) {
	gap := func(o Opponent) int32 {
		if d := o.Health - v.Health; d >= 0 {
			return d
		}
		return v.Health - o.Health
	}
	return pick(alive, func(a, b Opponent) bool { return gap(a) < gap(b) })
}

// Wildcard picks at random every time.
type Wildcard struct{}

func (Wildcard) Name() string                                      { return "The Wildcard" }
func (Wildcard) Choose(v *View, alive []Opponent) (Opponent, bool) { return random(v, alive) }

// Terminator picks a target and keeps shooting it until it dies.
type Terminator struct{}

func (Terminator) Name() string { return "The Terminator" }
func (Terminator) Choose(v *View, alive []Opponent) (Opponent, bool) {
	for _, o := range alive {
		if o.ID == v.Focus {
			return o, true
		}
	}
	o, ok := random(v, alive)
	if ok {
		v.Focus = o.ID
	}
	return o, ok
}

func random(v *View, alive []Opponent) (Opponent, bool) {
	if len(alive) == 0 {
		return Opponent{}, false
	}
	return alive[v.Rand.IntN(len(alive))], true
}

// pick returns the first opponent no other beats under better.
func pick(alive []Opponent, better func(a, b Opponent) bool) (Opponent, bool) {
	if len(alive) == 0 {
		return Opponent{}, false
	}
	best := alive[0]
	for _, o := range alive[1:] {
		if better(o, best) {
			best = o
		}
	}
	return best, true
}
