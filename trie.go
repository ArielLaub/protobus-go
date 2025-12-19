package protobus

import (
	"strings"
	"sync"
)

// Trie implements a topic-matching trie for pub/sub routing.
// Supports single-level wildcards (*) and multi-level wildcards (#).
type Trie[T any] struct {
	mu       sync.RWMutex
	root     *trieNode[T]
	handlers map[string]T // pattern -> handler for direct lookup
}

type trieNode[T any] struct {
	children map[string]*trieNode[T]
	handlers []T
	pattern  string // original pattern for this node
}

// NewTrie creates a new Trie.
func NewTrie[T any]() *Trie[T] {
	return &Trie[T]{
		root: &trieNode[T]{
			children: make(map[string]*trieNode[T]),
		},
		handlers: make(map[string]T),
	}
}

// Add adds a handler for a topic pattern.
// Patterns can include:
//   - Exact segments: "foo.bar.baz"
//   - Single-level wildcard (*): "foo.*.baz" matches "foo.anything.baz"
//   - Multi-level wildcard (#): "foo.#" matches "foo.bar", "foo.bar.baz", etc.
func (t *Trie[T]) Add(pattern string, handler T) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.handlers[pattern] = handler

	parts := strings.Split(pattern, ".")
	node := t.root

	for _, part := range parts {
		if node.children[part] == nil {
			node.children[part] = &trieNode[T]{
				children: make(map[string]*trieNode[T]),
			}
		}
		node = node.children[part]
	}

	node.handlers = append(node.handlers, handler)
	node.pattern = pattern
}

// Remove removes a handler for a pattern.
func (t *Trie[T]) Remove(pattern string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.handlers, pattern)

	parts := strings.Split(pattern, ".")
	t.removeRecursive(t.root, parts, 0)
}

func (t *Trie[T]) removeRecursive(node *trieNode[T], parts []string, index int) bool {
	if index == len(parts) {
		node.handlers = nil
		node.pattern = ""
		return len(node.children) == 0
	}

	part := parts[index]
	child, exists := node.children[part]
	if !exists {
		return false
	}

	if t.removeRecursive(child, parts, index+1) {
		delete(node.children, part)
	}

	return len(node.children) == 0 && len(node.handlers) == 0
}

// Match returns all handlers that match the given topic.
func (t *Trie[T]) Match(topic string) []T {
	t.mu.RLock()
	defer t.mu.RUnlock()

	parts := strings.Split(topic, ".")
	var results []T

	t.matchRecursive(t.root, parts, 0, &results)

	return results
}

func (t *Trie[T]) matchRecursive(node *trieNode[T], parts []string, index int, results *[]T) {
	// If we've consumed all parts, collect handlers at this node
	if index == len(parts) {
		*results = append(*results, node.handlers...)
		// Also check for # wildcard at end (matches zero segments)
		if hashNode, exists := node.children["#"]; exists {
			*results = append(*results, hashNode.handlers...)
		}
		return
	}

	part := parts[index]

	// Exact match
	if child, exists := node.children[part]; exists {
		t.matchRecursive(child, parts, index+1, results)
	}

	// Single-level wildcard (*)
	if starNode, exists := node.children["*"]; exists {
		t.matchRecursive(starNode, parts, index+1, results)
	}

	// Multi-level wildcard (#) - matches remaining parts
	if hashNode, exists := node.children["#"]; exists {
		*results = append(*results, hashNode.handlers...)
	}
}

// Has checks if a pattern is registered.
func (t *Trie[T]) Has(pattern string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.handlers[pattern]
	return exists
}

// Patterns returns all registered patterns.
func (t *Trie[T]) Patterns() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	patterns := make([]string, 0, len(t.handlers))
	for pattern := range t.handlers {
		patterns = append(patterns, pattern)
	}
	return patterns
}
