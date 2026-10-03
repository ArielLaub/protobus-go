// Package topic matches AMQP topic routing keys against binding patterns.
//
// A broker already routes an event to a subscriber's queue when any of its
// bindings match; this trie decides, inside the subscriber, which of its
// handlers the delivery is for. Semantics are AMQP's: words are separated by
// '.', '*' matches exactly one word and '#' matches zero or more.
package topic

import (
	"cmp"
	"slices"
	"strings"
	"sync"
)

// Trie maps topic patterns to values. It is safe for concurrent use: patterns
// may be added while other goroutines match.
type Trie[T comparable] struct {
	mu   sync.RWMutex
	root node[T]
	seq  uint64
}

type node[T comparable] struct {
	children map[string]*node[T]
	star     *node[T]
	hash     *node[T]
	// values registered on a pattern that ends exactly here. A node that is
	// only a step towards a longer pattern holds none, which is what stops
	// partial matches from matching.
	values []entry[T]
}

type entry[T comparable] struct {
	v   T
	seq uint64
}

// New returns an empty trie.
func New[T comparable]() *Trie[T] { return &Trie[T]{} }

// Add registers v under pattern. Registering the same pattern several times
// keeps every value; a longer pattern never shadows a shorter one.
func (t *Trie[T]) Add(pattern string, v T) {
	t.mu.Lock()
	defer t.mu.Unlock()

	n := &t.root
	for word := range strings.SplitSeq(pattern, ".") {
		n = n.child(word)
	}
	t.seq++
	n.values = append(n.values, entry[T]{v: v, seq: t.seq})
}

func (n *node[T]) child(word string) *node[T] {
	switch word {
	case "*":
		if n.star == nil {
			n.star = &node[T]{}
		}
		return n.star
	case "#":
		if n.hash == nil {
			n.hash = &node[T]{}
		}
		return n.hash
	}
	if n.children == nil {
		n.children = make(map[string]*node[T])
	}
	c, ok := n.children[word]
	if !ok {
		c = &node[T]{}
		n.children[word] = c
	}
	return c
}

// Match appends to dst every value whose pattern matches topic, each value
// once, in the order the values were added, and returns the extended slice.
func (t *Trie[T]) Match(topic string, dst []T) []T {
	words := strings.Split(topic, ".")

	t.mu.RLock()
	var found []entry[T]
	t.root.collect(words, &found)
	t.mu.RUnlock()

	if len(found) == 0 {
		return dst
	}
	slices.SortFunc(found, func(a, b entry[T]) int { return cmp.Compare(a.seq, b.seq) })
	start := len(dst)
	for _, e := range found {
		// Linear dedup: a topic matches a handful of handlers, never enough
		// for a set to pay for itself.
		if !slices.Contains(dst[start:], e.v) {
			dst = append(dst, e.v)
		}
	}
	return dst
}

// collect walks the patterns that can match words from this node.
func (n *node[T]) collect(words []string, found *[]entry[T]) {
	if len(words) == 0 {
		*found = append(*found, n.values...)
		// A trailing '#' matches zero further words.
		if n.hash != nil {
			n.hash.collect(nil, found)
		}
		return
	}
	if c := n.children[words[0]]; c != nil {
		c.collect(words[1:], found)
	}
	if n.star != nil {
		n.star.collect(words[1:], found)
	}
	if n.hash != nil {
		// '#' absorbs any number of words, including none.
		for i := 0; i <= len(words); i++ {
			n.hash.collect(words[i:], found)
		}
	}
}
