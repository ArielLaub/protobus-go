package topic

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
)

func match[T comparable](tr *Trie[T], topic string) []T { return tr.Match(topic, nil) }

func sorted(v []string) []string { s := slices.Clone(v); slices.Sort(s); return s }

func expect(t *testing.T, tr *Trie[string], topic string, want ...string) {
	t.Helper()
	got := sorted(match(tr, topic))
	if want == nil {
		want = []string{}
	}
	if got == nil {
		got = []string{}
	}
	if !slices.Equal(got, sorted(want)) {
		t.Fatalf("Match(%q) = %v, want %v", topic, got, want)
	}
}

// Ported from the TypeScript reference (test/unit/trie.test.ts).

func TestExactSimpleMatch(t *testing.T) {
	tr := New[string]()
	tr.Add("a.b.c", "abc")
	tr.Add("b.c.d", "2")
	expect(t, tr, "a.b.c", "abc")
	expect(t, tr, "b.c.d", "2")
	expect(t, tr, "c.d.e")
}

func TestNodeSplit(t *testing.T) {
	tr := New[string]()
	tr.Add("a.b.c.2", "2")
	tr.Add("a.b.c.1", "1")
	expect(t, tr, "a.b.c.1", "1")
	expect(t, tr, "a.b.c.2", "2")
}

func TestNoMatchIfNotAPatternEnd(t *testing.T) {
	tr := New[string]()
	tr.Add("a.b.c.d", "something")
	expect(t, tr, "a")
	expect(t, tr, "a.b")
	expect(t, tr, "a.b.c")
	expect(t, tr, "a.b.c.d", "something")
}

func TestStarInAllPositions(t *testing.T) {
	tr := New[string]()
	tr.Add("*.b.c", "first")
	tr.Add("a.*.c", "second")
	tr.Add("a.b.*", "third")
	expect(t, tr, "a.b.c", "first", "second", "third")
	expect(t, tr, "z.b.c", "first")
	expect(t, tr, "a.z.c", "second")
	expect(t, tr, "a.b.z", "third")
}

func TestHashReplacesZeroOrMoreWords(t *testing.T) {
	tr := New[string]()
	tr.Add("#.b.c", "first")
	tr.Add("a.#.c", "second")
	tr.Add("a.b.#", "third")

	for _, tp := range []string{"z.b.c", "x.z.b.c", "x.y.z.b.c", "b.c"} {
		expect(t, tr, tp, "first")
	}
	expect(t, tr, "b.b.b")
	expect(t, tr, "c.c.c")
	for _, tp := range []string{"a.z.c", "a.x.z.c", "a.x.y.z.c", "a.c"} {
		expect(t, tr, tp, "second")
	}
	expect(t, tr, "a.a.a")
	for _, tp := range []string{"a.b.z", "a.b.x.z", "a.b.x.y.z", "a.b"} {
		expect(t, tr, tp, "third")
	}
}

func TestRabbitBlogPost(t *testing.T) {
	tr := New[string]()
	tr.Add("a.b.c", "first")
	tr.Add("a.*.b.c", "second")
	tr.Add("a.#.c", "third")
	tr.Add("b.b.c", "forth")
	expect(t, tr, "a.d.d.d.c", "third")
}

func TestRabbitMQTopicsTutorial(t *testing.T) {
	tr := New[string]()
	tr.Add("*.orange.*", "Q1")
	tr.Add("*.*.rabbit", "Q2")
	tr.Add("lazy.#", "Q2")
	expect(t, tr, "quick.orange.rabbit", "Q1", "Q2")
	expect(t, tr, "lazy.orange.elephant", "Q1", "Q2")
	expect(t, tr, "quick.orange.fox", "Q1")
	expect(t, tr, "lazy.brown.fox", "Q2")
	expect(t, tr, "lazy.pink.rabbit", "Q2") // once, though two patterns match
	expect(t, tr, "orange")
	expect(t, tr, "quick.brown.fox")
	expect(t, tr, "lazy.orange.male.rabbit", "Q2")
}

// Ported from test/unit/trie_multi.test.ts.

func TestKeepsBothValuesOnOneTopic(t *testing.T) {
	tr := New[string]()
	tr.Add("EVENT.Order", "a")
	tr.Add("EVENT.Order", "b")
	expect(t, tr, "EVENT.Order", "a", "b")
}

func TestPatternIsNotShadowedByALongerOne(t *testing.T) {
	for _, order := range [][2]string{{"EVENT.Order", "EVENT.Order.Shipped"}, {"EVENT.Order.Shipped", "EVENT.Order"}} {
		tr := New[string]()
		for _, p := range order {
			tr.Add(p, p)
		}
		expect(t, tr, "EVENT.Order", "EVENT.Order")
		expect(t, tr, "EVENT.Order.Shipped", "EVENT.Order.Shipped")
	}
}

func TestIntermediateNodesStayOutOfUnrelatedMatches(t *testing.T) {
	tr := New[string]()
	tr.Add("EVENT.Order", "short")
	tr.Add("EVENT.Order.Shipped", "long")
	tr.Add("EVENT.Invoice.Paid", "other")
	expect(t, tr, "EVENT.Invoice")
	expect(t, tr, "EVENT.Invoice.Paid", "other")
}

func TestSeveralValuesUnderAWildcard(t *testing.T) {
	tr := New[string]()
	tr.Add("EVENT.*", "x")
	tr.Add("EVENT.*", "y")
	tr.Add("EVENT.Order", "exact")
	expect(t, tr, "EVENT.Order", "exact", "x", "y")
}

// Ported from test/unit/trie_documented_examples.test.ts.

func TestDocumentedWildcardExample(t *testing.T) {
	build := func() *Trie[string] {
		tr := New[string]()
		tr.Add("ORDERS.*.CREATED", "A")
		tr.Add("ORDERS.#", "B")
		tr.Add("ORDERS.US.*.SHIPPED", "C")
		return tr
	}
	expect(t, build(), "ORDERS.US.CREATED", "A", "B")
	expect(t, build(), "ORDERS.US.123.CREATED", "B")
	expect(t, build(), "ORDERS.US.123.SHIPPED", "B", "C")
	expect(t, build(), "ORDERS.EU.456.SHIPPED", "B")
	expect(t, build(), "ORDERS", "B") // # matches zero words where * cannot
}

// Go-specific guarantees.

func TestMatchOrderIsRegistrationOrder(t *testing.T) {
	// Handlers run in a deterministic order: the order they were subscribed.
	tr := New[string]()
	tr.Add("#", "1")
	tr.Add("a.b", "2")
	tr.Add("a.*", "3")
	tr.Add("*.b", "4")
	got := match(tr, "a.b")
	if !slices.Equal(got, []string{"1", "2", "3", "4"}) {
		t.Fatalf("got %v", got)
	}
}

func TestMatchAppendsToTheGivenSlice(t *testing.T) {
	tr := New[string]()
	tr.Add("a", "x")
	buf := make([]string, 0, 4)
	got := tr.Match("a", buf)
	if len(got) != 1 || &got[:1][0] != &buf[:1][0] {
		t.Fatal("Match must reuse the provided buffer")
	}
}

func TestEmptyTopicAndEmptyWords(t *testing.T) {
	tr := New[string]()
	tr.Add("#", "all")
	tr.Add("a..b", "gap")
	tr.Add("", "empty")
	expect(t, tr, "", "all", "empty")
	expect(t, tr, "a..b", "all", "gap")
	expect(t, tr, "a.b", "all")
}

// amqpMatch is an independent reference implementation of AMQP topic
// semantics, written as the textbook recursion, used to cross-check the trie
// on random inputs.
func amqpMatch(pattern, topic string) bool {
	var rec func(p, t []string) bool
	rec = func(p, t []string) bool {
		switch {
		case len(p) == 0:
			return len(t) == 0
		case p[0] == "#":
			return rec(p[1:], t) || len(t) > 0 && rec(p, t[1:])
		case len(t) == 0:
			return false
		case p[0] == "*" || p[0] == t[0]:
			return rec(p[1:], t[1:])
		default:
			return false
		}
	}
	return rec(strings.Split(pattern, "."), strings.Split(topic, "."))
}

func TestAgainstReferenceMatcher(t *testing.T) {
	alphabet := []string{"a", "b", "c", "*", "#"}
	topicWords := []string{"a", "b", "c", "d"}
	rng := rand.New(rand.NewPCG(1, 2))
	word := func(from []string) string { return from[rng.IntN(len(from))] }
	join := func(from []string, n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = word(from)
		}
		return strings.Join(parts, ".")
	}
	for range 3000 {
		pattern := join(alphabet, 1+rng.IntN(4))
		tp := join(topicWords, 1+rng.IntN(5))
		tr := New[string]()
		tr.Add(pattern, "hit")
		got := len(match(tr, tp)) == 1
		if want := amqpMatch(pattern, tp); got != want {
			t.Fatalf("pattern %q topic %q: trie=%v reference=%v", pattern, tp, got, want)
		}
	}
}

func TestConcurrentAddAndMatch(t *testing.T) {
	// Subscriptions may be added while deliveries are matched on many
	// goroutines at once; run under -race.
	tr := New[string]()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := range 200 {
				tr.Add(fmt.Sprintf("EVENT.%d.%d", i, j), "v")
			}
		}()
		go func() {
			defer wg.Done()
			var buf []string
			for range 200 {
				buf = tr.Match("EVENT.1.1", buf[:0])
			}
		}()
	}
	wg.Wait()
	expect(t, tr, "EVENT.3.7", "v")
}

func BenchmarkMatch(b *testing.B) {
	tr := New[int]()
	for i := range 500 {
		tr.Add(fmt.Sprintf("EVENT.svc%d.*", i), i)
	}
	tr.Add("EVENT.#", -1)
	buf := make([]int, 0, 8)
	b.ReportAllocs()
	for b.Loop() {
		buf = tr.Match("EVENT.svc250.Created", buf[:0])
	}
}
