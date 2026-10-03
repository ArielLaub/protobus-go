package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const calculatorProto = `syntax = "proto3";
package Calculator;

// Adds numbers.
service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc ticks(AddRequest) returns (stream AddResponse);
  rpc total(bigint) returns (bigint);
}

message AddRequest {
  int32 a = 1;
  int32 b = 2;
  timestamp at = 3;
  map<string, bigint> balances = 4;
}

message AddResponse { int32 result = 1; }
`

const ordersProto = `syntax = "proto3";
package shop.orders;

import "Calculator.proto";

message OrderPlaced {
  string id = 1;
  Calculator.AddRequest detail = 2;
}
`

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, _ := os.Getwd()
	return filepath.Dir(filepath.Dir(wd))
}

// newModule makes a scratch Go module that resolves protobus-go to this
// checkout.
func newModule(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	root := repoRoot(t)
	gomod := "module example.com/app\n\ngo 1.25\n\nrequire github.com/ArielLaub/protobus-go/v2 v2.0.0\n\n" +
		"replace github.com/ArielLaub/protobus-go/v2 => " + root + "\n"
	must(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644))
	sum, err := os.ReadFile(filepath.Join(root, "go.sum"))
	must(t, err)
	must(t, os.WriteFile(filepath.Join(dir, "go.sum"), sum, 0o644))
	must(t, os.MkdirAll(filepath.Join(dir, "proto", "shop"), 0o755))
	must(t, os.WriteFile(filepath.Join(dir, "proto", "Calculator.proto"), []byte(calculatorProto), 0o644))
	must(t, os.WriteFile(filepath.Join(dir, "proto", "shop", "orders.proto"), []byte(ordersProto), 0o644))
	return dir
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func inDir(t *testing.T, dir string) {
	t.Helper()
	old, _ := os.Getwd()
	must(t, os.Chdir(dir))
	t.Cleanup(func() { _ = os.Chdir(old) })
}

func cli(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(context.Background(), args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// goBuild compiles the scratch module offline, from the module cache.
func goBuild(t *testing.T, dir string) {
	t.Helper()
	if testing.Short() {
		t.Skip("compiles generated code")
	}
	cmd := exec.Command("go", "build", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOPROXY=off", "GOWORK=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated code does not build: %v\n%s", err, out)
	}
}

func TestVersionHelpInit(t *testing.T) {
	if code, out, _ := cli(t, "version"); code != 0 || !strings.Contains(out, "protobus-go 2.") {
		t.Fatalf("version: %d %q", code, out)
	}
	if code, out, _ := cli(t, "help"); code != 0 || !strings.Contains(out, "generate:service") {
		t.Fatalf("help: %d", code)
	}
	if code, out, _ := cli(t, "init"); code != 0 || !strings.Contains(out, "protobus generate") {
		t.Fatalf("init: %d", code)
	}
	if code, _, errOut := cli(t, "frobnicate"); code != 1 || !strings.Contains(errOut, "unknown command") {
		t.Fatalf("unknown: %d %q", code, errOut)
	}
	if code, _, _ := cli(t); code != 1 {
		t.Fatal("no arguments is a usage error")
	}
}

func TestGenerateCompiles(t *testing.T) {
	dir := newModule(t)
	inDir(t, dir)
	code, out, errOut := cli(t, "generate")
	if code != 0 {
		t.Fatalf("generate failed: %s", errOut)
	}
	for _, want := range []string{
		"gen/calculator/Calculator.pb.go", "gen/calculator/Calculator_protobus.pb.go", "gen/shop/orders/orders.pb.go",
	} {
		if !strings.Contains(out, filepath.FromSlash(want)) {
			t.Errorf("missing %s in\n%s", want, out)
		}
	}
	if strings.Contains(out, "orders_protobus") {
		t.Error("a file without services gets no bindings")
	}
	src, err := os.ReadFile(filepath.Join(dir, "gen", "calculator", "Calculator_protobus.pb.go"))
	must(t, err)
	for _, want := range []string{"package calculator", `const Service_ServiceName = "Calculator.Service"`,
		"*pbtypes.Bigint", "iter.Seq2[*AddResponse, error]"} {
		if !strings.Contains(string(src), want) {
			t.Errorf("generated bindings lack %q", want)
		}
	}
	goBuild(t, dir)
}

func TestGenerateDryRunWritesNothing(t *testing.T) {
	dir := newModule(t)
	inDir(t, dir)
	code, out, errOut := cli(t, "generate", "-dry-run")
	if code != 0 || !strings.Contains(out, "Calculator.pb.go") {
		t.Fatalf("%d %s %s", code, out, errOut)
	}
	if _, err := os.Stat(filepath.Join(dir, "gen")); !os.IsNotExist(err) {
		t.Fatal("dry run wrote files")
	}
}

func TestGenerateNeedsAModule(t *testing.T) {
	dir := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(dir, "proto"), 0o755))
	must(t, os.WriteFile(filepath.Join(dir, "proto", "a.proto"), []byte(calculatorProto), 0o644))
	inDir(t, dir)
	if code, _, errOut := cli(t, "generate"); code != 1 || !strings.Contains(errOut, "go.mod") {
		t.Fatalf("%d %q", code, errOut)
	}
}

func TestGenerateReportsSchemaErrors(t *testing.T) {
	dir := newModule(t)
	must(t, os.WriteFile(filepath.Join(dir, "proto", "bad.proto"), []byte("syntax = \"proto3\";\nmessage X { nope y = 1; }\n"), 0o644))
	inDir(t, dir)
	if code, _, errOut := cli(t, "generate"); code != 1 || !strings.Contains(errOut, "bad.proto:2") {
		t.Fatalf("%d %q", code, errOut)
	}
}

func TestGenerateServiceSkeletonCompiles(t *testing.T) {
	dir := newModule(t)
	inDir(t, dir)
	if code, _, errOut := cli(t, "generate"); code != 0 {
		t.Fatal(errOut)
	}
	code, out, errOut := cli(t, "generate:service", "Calculator")
	if code != 0 {
		t.Fatalf("generate:service: %s", errOut)
	}
	if !strings.Contains(out, filepath.Join("services", "calculator", "main.go")) {
		t.Fatalf("output %q", out)
	}
	goBuild(t, dir)

	if code, _, errOut := cli(t, "generate:service", "Calculator"); code != 1 || !strings.Contains(errOut, "not overwriting") {
		t.Fatalf("a second run must refuse to overwrite: %d %q", code, errOut)
	}
}

func TestGenerateServiceRefusesUnsafeNames(t *testing.T) {
	dir := newModule(t)
	inDir(t, dir)
	for _, name := range []string{"../escape", "../../etc/passwd", "foo/bar", `foo\bar`, "/absolute", ".", "..", "with space", "nul\x00byte", strings.Repeat("a", 101)} {
		if code, _, errOut := cli(t, "generate:service", name); code != 1 || !strings.Contains(errOut, "invalid service name") {
			t.Errorf("%q: %d %q", name, code, errOut)
		}
	}
	for _, name := range []string{"Calculator", "Order-Service", "billing_v2", "Svc123"} {
		if err := checkServiceName(name); err != nil {
			t.Errorf("%q must be accepted: %v", name, err)
		}
	}
	if code, _, _ := cli(t, "generate:service"); code != 1 {
		t.Error("a missing name is an error")
	}
	if code, _, errOut := cli(t, "generate:service", "Nope"); code != 1 || !strings.Contains(errOut, "no Nope.proto") {
		t.Errorf("unknown proto: %q", errOut)
	}
}

func TestGoName(t *testing.T) {
	for in, want := range map[string]string{"add": "Add", "get_user": "GetUser", "Service": "Service", "v2_api": "V2Api", "x9y": "X9Y"} {
		if got := goName(in); got != want {
			t.Errorf("goName(%q) = %q, want %q", in, got, want)
		}
	}
}
