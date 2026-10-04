package protoload

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

// A schema as the TypeScript and Python ports accept it: custom types used
// with no import, no go_package.
const calcProto = `syntax = "proto3";
package Calculator;

service Service {
  rpc add(AddRequest) returns (AddResponse);
  rpc watch(AddRequest) returns (stream AddResponse);
}

message AddRequest {
  int32 a = 1;
  int32 b = 2;
  bigint amount = 3;
  timestamp at = 4;
  map<string, bigint> balances = 5;
}

message AddResponse { int32 sum = 1; }
`

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadResolvesCustomTypesWithoutAnImport(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "Calculator.proto", calcProto)
	res, err := Load(context.Background(), []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	d, err := res.Files.FindDescriptorByName("Calculator.AddRequest")
	if err != nil {
		t.Fatal(err)
	}
	fields := d.(protoreflect.MessageDescriptor).Fields()
	amount := fields.ByName("amount")
	if amount.Kind() != protoreflect.MessageKind || amount.Message().FullName() != "bigint" {
		t.Fatalf("amount is %v %v", amount.Kind(), amount.Message())
	}
	// The built-in types are pbtypes' own descriptors, so values decode into
	// the same Go types generated code uses.
	if amount.Message() != (&pbtypes.Bigint{}).ProtoReflect().Descriptor() {
		t.Fatal("bigint must resolve to pbtypes.Bigint's descriptor")
	}
	svc, err := res.Files.FindDescriptorByName("Calculator.Service")
	if err != nil || !svc.(protoreflect.ServiceDescriptor).Methods().ByName("watch").IsStreamingServer() {
		t.Fatalf("service %v %v", svc, err)
	}
}

func TestLoadKeepsLineNumbersInErrors(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "bad.proto", "syntax = \"proto3\";\npackage X;\n\nmessage M {\n  nosuchtype f = 1;\n}\n")
	_, err := Load(context.Background(), []string{dir})
	if err == nil || !strings.Contains(err.Error(), "bad.proto:5") {
		t.Fatalf("the error must point at line 5 of the user's file: %v", err)
	}
}

func TestLoadFindsFilesRecursivelyAndOnlyProtoFiles(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "a/one.proto", "syntax = \"proto3\";\npackage A;\nmessage One {}\n")
	write(t, dir, "a/b/two.proto", "syntax = \"proto3\";\npackage B;\nimport \"a/one.proto\";\nmessage Two { A.One one = 1; }\n")
	write(t, dir, "notes.protocol.txt", "not a schema")
	write(t, dir, "schema.proto.bak", "syntax = garbage")
	res, err := Load(context.Background(), []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := res.Files.FindDescriptorByName("B.Two"); err != nil {
		t.Fatal(err)
	}
	if got := len(res.Compiled); got != 2 {
		t.Fatalf("compiled %d files, want exactly the two .proto files", got)
	}
}

func TestLoadAcceptsAnExplicitImport(t *testing.T) {
	src := strings.Replace(calcProto, "package Calculator;", "package Calculator;\nimport \"protobus/types.proto\";", 1)
	res, err := Parse(context.Background(), map[string]string{"calc.proto": src})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := res.Files.FindDescriptorByName("Calculator.AddRequest"); err != nil {
		t.Fatal(err)
	}
}

func TestParseWithoutSyntaxLine(t *testing.T) {
	res, err := Parse(context.Background(), map[string]string{"p.proto": "package P;\nmessage M { optional int32 x = 1; }\n"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := res.Files.FindDescriptorByName("P.M"); err != nil {
		t.Fatal(err)
	}
}

func TestUserCustomTypes(t *testing.T) {
	src := "syntax = \"proto3\";\npackage U;\nmessage User { uuid id = 1; money balance = 2; }\n"
	res, err := Parse(context.Background(), map[string]string{"u.proto": src},
		WithCustomType("uuid", protoreflect.BytesKind), WithCustomType("money", protoreflect.Int64Kind))
	if err != nil {
		t.Fatal(err)
	}
	d, _ := res.Files.FindDescriptorByName("User")
	if d != nil {
		t.Fatal("custom types are root-level, not the user's message")
	}
	md, err := res.Files.FindDescriptorByName("uuid")
	if err != nil {
		t.Fatal(err)
	}
	v := md.(protoreflect.MessageDescriptor).Fields().ByNumber(1)
	if v.Kind() != protoreflect.BytesKind || !v.HasPresence() {
		t.Fatalf("uuid value field %v presence %v", v.Kind(), v.HasPresence())
	}
}

func TestCustomTypeValidation(t *testing.T) {
	cases := map[string]Option{
		"shadows a built-in": WithCustomType("bigint", protoreflect.BytesKind),
		"not an identifier":  WithCustomType("my-type", protoreflect.BytesKind),
		"unsupported kind":   WithCustomType("thing", protoreflect.MessageKind),
	}
	for name, opt := range cases {
		if _, err := Parse(context.Background(), map[string]string{"x.proto": "syntax = \"proto3\";"}, opt); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestDynamicMessagesRoundTrip(t *testing.T) {
	res, err := Parse(context.Background(), map[string]string{"calc.proto": calcProto})
	if err != nil {
		t.Fatal(err)
	}
	mt, err := res.Types.FindMessageByName("Calculator.AddRequest")
	if err != nil {
		t.Fatal(err)
	}
	m := mt.New()
	m.Set(m.Descriptor().Fields().ByName("a"), protoreflect.ValueOfInt32(7))
	b, err := proto.Marshal(m.Interface())
	if err != nil {
		t.Fatal(err)
	}
	out := dynamicpb.NewMessage(m.Descriptor())
	if err := proto.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	if out.Get(out.Descriptor().Fields().ByName("a")).Int() != 7 {
		t.Fatal("round trip")
	}
}

func TestLoadOfAMissingDirectory(t *testing.T) {
	if _, err := Load(context.Background(), []string{filepath.Join(t.TempDir(), "nope")}); err == nil {
		t.Fatal("expected an error")
	}
}
