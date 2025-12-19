package protobus

import (
	"testing"
)

func TestTrieExactMatch(t *testing.T) {
	trie := NewTrie[string]()
	trie.Add("foo.bar.baz", "handler1")

	matches := trie.Match("foo.bar.baz")
	if len(matches) != 1 || matches[0] != "handler1" {
		t.Errorf("Expected [handler1], got %v", matches)
	}
}

func TestTrieNoMatch(t *testing.T) {
	trie := NewTrie[string]()
	trie.Add("foo.bar.baz", "handler1")

	matches := trie.Match("foo.bar.qux")
	if len(matches) != 0 {
		t.Errorf("Expected no matches, got %v", matches)
	}
}

func TestTrieSingleWildcard(t *testing.T) {
	trie := NewTrie[string]()
	trie.Add("foo.*.baz", "handler1")

	matches := trie.Match("foo.bar.baz")
	if len(matches) != 1 || matches[0] != "handler1" {
		t.Errorf("Expected [handler1], got %v", matches)
	}

	matches = trie.Match("foo.anything.baz")
	if len(matches) != 1 || matches[0] != "handler1" {
		t.Errorf("Expected [handler1], got %v", matches)
	}

	matches = trie.Match("foo.bar.qux")
	if len(matches) != 0 {
		t.Errorf("Expected no matches, got %v", matches)
	}
}

func TestTrieMultiLevelWildcard(t *testing.T) {
	trie := NewTrie[string]()
	trie.Add("foo.#", "handler1")

	matches := trie.Match("foo.bar")
	if len(matches) != 1 || matches[0] != "handler1" {
		t.Errorf("Expected [handler1], got %v", matches)
	}

	matches = trie.Match("foo.bar.baz")
	if len(matches) != 1 || matches[0] != "handler1" {
		t.Errorf("Expected [handler1], got %v", matches)
	}

	matches = trie.Match("foo.bar.baz.qux")
	if len(matches) != 1 || matches[0] != "handler1" {
		t.Errorf("Expected [handler1], got %v", matches)
	}
}

func TestTrieMultiLevelWildcardAtEnd(t *testing.T) {
	trie := NewTrie[string]()
	trie.Add("foo.bar.#", "handler1")

	// # should match zero or more segments
	matches := trie.Match("foo.bar")
	if len(matches) != 1 || matches[0] != "handler1" {
		t.Errorf("Expected [handler1], got %v", matches)
	}
}

func TestTrieMultipleHandlers(t *testing.T) {
	trie := NewTrie[string]()
	trie.Add("foo.bar.baz", "handler1")
	trie.Add("foo.*.baz", "handler2")
	trie.Add("foo.#", "handler3")

	matches := trie.Match("foo.bar.baz")
	if len(matches) != 3 {
		t.Errorf("Expected 3 matches, got %d: %v", len(matches), matches)
	}
}

func TestTrieRemove(t *testing.T) {
	trie := NewTrie[string]()
	trie.Add("foo.bar.baz", "handler1")

	matches := trie.Match("foo.bar.baz")
	if len(matches) != 1 {
		t.Errorf("Expected 1 match before remove, got %d", len(matches))
	}

	trie.Remove("foo.bar.baz")

	matches = trie.Match("foo.bar.baz")
	if len(matches) != 0 {
		t.Errorf("Expected 0 matches after remove, got %d", len(matches))
	}
}

func TestTrieHas(t *testing.T) {
	trie := NewTrie[string]()
	trie.Add("foo.bar", "handler1")

	if !trie.Has("foo.bar") {
		t.Error("Expected Has(foo.bar) to be true")
	}

	if trie.Has("foo.baz") {
		t.Error("Expected Has(foo.baz) to be false")
	}
}
