package protobus

import (
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
)

// No secret is ever committed, and the ignore file keeps it that way.
func TestNoSecretsAreTracked(t *testing.T) {
	out, err := exec.Command("git", "ls-files").Output()
	if err != nil {
		t.Skip("not a git checkout:", err)
	}
	for _, f := range strings.Fields(string(out)) {
		base := path.Base(f)
		if base == ".env" || strings.HasPrefix(base, ".env.") || path.Ext(base) == ".pem" || path.Ext(base) == ".key" {
			t.Errorf("%s looks like a secret and must not be tracked", f)
		}
	}
	ignore, err := os.ReadFile(".gitignore")
	if err != nil {
		t.Fatal(err)
	}
	for _, pattern := range []string{".env", ".env.*", "*.pem", "*.key"} {
		if !strings.Contains("\n"+string(ignore)+"\n", "\n"+pattern+"\n") {
			t.Errorf(".gitignore lacks %q", pattern)
		}
	}
}
