package uuid

import (
	"regexp"
	"testing"
)

var v4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewIsRFC4122Version4(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id := New()
		if !v4.MatchString(id) {
			t.Fatalf("%q is not a v4 UUID", id)
		}
		if seen[id] {
			t.Fatalf("duplicate %q", id)
		}
		seen[id] = true
	}
}

func BenchmarkNew(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = New()
	}
}
