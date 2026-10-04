# Code generation

protobus schemas are ordinary `.proto` files, shared verbatim with the
TypeScript and Python ports. Go needs them turned into code: message types (as
`protoc-gen-go` makes them) plus protobus service bindings. Two tools do it:

| Tool | Needs protoc | Use it when |
|---|---|---|
| `protobus` CLI | no | The usual case. Reads shared schemas as they are. |
| `protoc-gen-go-protobus` | yes | You already drive `protoc` or `buf` and want protobus bindings alongside. |

Both emit the same bindings. A third route, [`protoload`](#loading-schemas-at-runtime),
compiles schemas at runtime for programs that have no generated code at all.

## The `protobus` CLI

```
go install github.com/ArielLaub/protobus-go/v2/cmd/protobus@latest
```

### `protobus generate`

```
protobus generate [-proto DIR] [-out DIR] [-custom-type name=kind ...] [-dry-run]
```

Compiles every file ending in `.proto` under `-proto` (default `./proto`,
searched recursively, each file importing others by its path relative to that
directory) and writes Go code under `-out` (default `./gen`). It prints the
files it wrote; `-dry-run` prints them without writing. `-custom-type`
declares an application custom type; see [Application custom
types](#application-custom-types).

The generated code imports `google.golang.org/protobuf`. If your module does
not require it yet, run `go mod tidy` after the first `protobus generate`, or
the build fails with `no required module provides package
google.golang.org/protobuf/...`.

Shared schemas need neither a `go_package` option nor an import for the
built-in `bigint` and `timestamp` types: the CLI adds the import itself and
chooses the Go package. Each proto package becomes one Go package, at the
package name lower-cased with dots as directories, named after its last
element:

| Proto | Go import path | Go package |
|---|---|---|
| `package Calculator;` | `<module>/gen/calculator` | `calculator` |
| `package Acme.Billing;` | `<module>/gen/acme/billing` | `billing` |
| no package, file `notes.proto` | `<module>/gen/notes` | `notes` |

A schema that does declare `go_package` keeps it. `<module>` is the module path
in the nearest `go.mod` above `-out`, which must therefore be inside a Go module
(run `go mod init` first); the CLI refuses to write outside it.

For each `.proto` it writes two files into that package's directory:

- `<File>.pb.go`, the message and enum types, exactly as `protoc-gen-go` would
  generate them;
- `<File>_protobus.pb.go`, the service bindings (only for files declaring a
  service).

The built-in types are not regenerated: fields of type `bigint` and
`timestamp` refer to `github.com/ArielLaub/protobus-go/v2/pbtypes`.

Generation fails, naming the file and method, for a client-streaming or
bidirectional method (protobus has unary and server-streaming calls only), and
for two methods whose names map to the same Go name.

To regenerate with `go generate ./...`, put a directive in any Go file of the
module, as `examples/calculator/main.go` does:

```go
package main

//go:generate protobus generate -proto ./proto -out ./gen

func main() {}
```

The repository's CI regenerates every example and the cross-language schema
and fails if the result differs from what is committed.

### `protobus generate:service`

```
protobus generate:service NAME [-proto DIR] [-out DIR] [-services DIR] [-custom-type name=kind ...]
```

Writes a runnable skeleton for the first service declared in `NAME.proto`
(looked up under `-proto`, default `./proto`) to
`<-services>/<name lower-cased>/main.go` (default `./services`). `-out` must be
where `protobus generate` put the generated code (default `./gen`), so the
skeleton imports the right package. Run `protobus generate` first, and pass the
same `-custom-type` flags if the schema uses application custom types.

The skeleton contains a `server` type embedding the generated
`Unimplemented…Server`, one method per rpc returning a `NOT_IMPLEMENTED`
`HandledError`, and a `main` that dials `AMQP_URL` (default
`amqp://guest:guest@localhost:5672/`), registers the server and calls
`protobus.Run`. It is a starting point to edit, not generated code to keep in
sync.

It never overwrites an existing file. `NAME` must be letters, digits, `-` and
`_` only, at most 100 characters, so it cannot point outside the output
directories. The skeleton qualifies message types of the service's own file
and the built-in types; a service whose methods take messages imported from
another file needs those references fixed by hand.

### `protobus init`, `version`, `help`

`init` prints step-by-step project setup (module, directories, `go get`, a
first schema, generating and running). `version` prints the release
(`protobus-go 2.0.0`). `help` prints the usage.

## `protoc-gen-go-protobus`

A standard protoc plugin that generates only the `_protobus.pb.go` bindings;
run it next to `protoc-gen-go`, which generates the messages:

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install github.com/ArielLaub/protobus-go/v2/cmd/protoc-gen-go-protobus@latest

protoc --go_out=. --go_opt=paths=source_relative \
       --go-protobus_out=. --go-protobus_opt=paths=source_relative \
       -I . -I $(go list -m -f '{{.Dir}}' github.com/ArielLaub/protobus-go/v2)/pbtypes/proto \
       calc.proto
```

Under protoc, you supply what the CLI would have added:

- a schema using `bigint` or `timestamp` must `import "protobus/types.proto";`,
  found through the `-I` path above (its `go_package` already points at
  `pbtypes`);
- `protoc-gen-go` needs a Go package for every file, through a `go_package`
  option or `--go_opt=M<file>=<import path>` (and the same `M` option for
  `--go-protobus_opt`).

The plugin accepts protogen's standard parameters (`paths`, `module`, `M…`)
and no others. `protoc-gen-go-protobus --version` prints its release.

## What gets generated

For a service `X` (the Go name of the proto service), the bindings file
declares:

| Identifier | What it is |
|---|---|
| `X_ServiceName` | The service's fully-qualified proto name, e.g. `"Calculator.Service"` |
| `XServer` | The interface your implementation satisfies: one method per rpc |
| `UnimplementedXServer` | A struct whose methods all return `protobus.ErrUnimplemented` |
| `X_ServiceDesc` | The `protobus.ServiceDesc` binding each rpc name to a handler |
| `RegisterXServer(bus, srv, opts...)` | `bus.Register(&X_ServiceDesc, srv, opts...)`: returns a `*protobus.Service` to `Start` or pass to `protobus.Run` |
| `XClient` | The client interface: one method per rpc |
| `NewXClient(bus, opts...)` | Returns an `XClient`; pass `protobus.WithInstance` to address a named instance |

Method names are converted to Go style (`add` becomes `Add`); on the wire the
name stays exactly as the `.proto` declares it. Signatures:

| rpc | Server method | Client method |
|---|---|---|
| `rpc add(AddRequest) returns (AddResponse)` | `Add(context.Context, *AddRequest) (*AddResponse, error)` | `Add(ctx, in, ...protobus.CallOption) (*AddResponse, error)` |
| `rpc generate(Req) returns (stream Token)` | `Generate(context.Context, *Req, protobus.ServerStream[*Token]) error` | `Generate(ctx, in, ...protobus.StreamOption) iter.Seq2[*Token, error]` |

Implementations should embed `UnimplementedXServer`. In Go, embedding a struct
in another (writing its type as an unnamed field) promotes its methods, so
your type satisfies the interface even for rpcs you have not written yet, and
adding an rpc to the schema does not break your build. An unimplemented method
answers its caller with `PROTOCOL_ERROR`.

```go
package main

import (
	"context"
	"log"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/calculator/gen/calculator"
)

type server struct {
	calculator.UnimplementedServiceServer // divide answers PROTOCOL_ERROR until written
}

func (server) Add(_ context.Context, in *calculator.AddRequest) (*calculator.AddResponse, error) {
	return &calculator.AddResponse{Result: in.A + in.B}, nil
}

func main() {
	ctx := context.Background()
	bus, err := protobus.Dial(ctx, "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	svc, err := calculator.RegisterServiceServer(bus, server{}, protobus.WithMaxConcurrent(8))
	if err != nil {
		log.Fatal(err)
	}
	if err := protobus.Run(ctx, bus, svc); err != nil {
		log.Fatal(err)
	}
}
```

Each generated file also references `protobus.SupportPackageIsVersion1`. That
constant names the generated-code contract; a future library that drops the
contract drops the constant, so stale generated code fails to compile with an
error naming the cause instead of misbehaving. The fix is to regenerate.

Comments on services and rpcs in the `.proto` are copied onto the generated
identifiers, and `option deprecated = true` marks them deprecated.

See [Services](services.md) and [Clients](clients.md) for using the bindings,
[Streaming](streaming.md) for server-streaming methods.

## Custom types: `bigint` and `timestamp`

protobus predeclares two message types at the root of the type namespace, so a
shared schema can use them like scalars:

```proto
message Transfer {
  bigint amount = 1;
  timestamp at = 2;
}
```

Their wire format and their meaning in each language are in
[Compatibility](compatibility.md#the-contract-is-the-proto). In Go such fields
have the types `*pbtypes.Bigint` and `*pbtypes.Timestamp`, from
`github.com/ArielLaub/protobus-go/v2/pbtypes`:

| Function | Does |
|---|---|
| `pbtypes.NewBigint(v *big.Int) (*Bigint, error)` | Encodes `v`; refuses a negative value or one above 2^256-1 (`ErrBigintRange`) rather than wrapping or truncating it |
| `pbtypes.MustBigint(v)` | `NewBigint` for values known to be in range; panics otherwise |
| `pbtypes.BigintFromUint64(u)` | Encodes a `uint64`, always in range |
| `pbtypes.ParseBigint(s)` | Encodes a non-negative decimal or `0x`-prefixed hexadecimal string |
| `(*Bigint).BigInt() (*big.Int, error)` | Decodes into a new `*big.Int`; a nil message or empty value is 0; a value wider than 32 bytes is refused |
| `pbtypes.NewTimestamp(t time.Time)` | Encodes `t` as milliseconds since the epoch; finer precision is truncated |
| `(*Timestamp).AsTime() time.Time` | Decodes as UTC; a nil message or unset value is the Unix epoch |

`pbtypes.BigintBytes` (32) is the wire width. `timestamp` is not
`google.protobuf.Timestamp`: it is milliseconds, with no nanos field.

```go
package main

import (
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

func main() {
	wei, _ := new(big.Int).SetString("1000000000000000000", 10)
	amount, err := pbtypes.NewBigint(wei)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := pbtypes.NewBigint(big.NewInt(-1)); err != nil {
		fmt.Println(err) // protobus: bigint out of range ...
	}

	back, err := amount.BigInt()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(back) // 1000000000000000000

	at := pbtypes.NewTimestamp(time.Now())
	fmt.Println(at.AsTime().Format(time.RFC3339))
}
```

The range is also enforced by the library itself, on every message it encodes
or decodes, nested fields, lists and map values included: a request carrying a
`bigint` wider than 32 bytes is answered with `PROTOCOL_ERROR` before a handler
sees it, and one you try to send fails with `ErrInvalidRequest` before it
leaves. See [Security](security.md#bigint-range-checks).

## Application custom types

Besides the built-ins, an application can declare its own custom types, as
`registerType` does in the TypeScript port. Each is a root-level message
`name { optional <kind> value = 1; }` that schemas use like a scalar; what the
value means (a decimal as a string, say) is the application's business, and
protobus only carries it. Every port must declare the same name with the same
kind.

With the CLI, declare each with a repeatable `-custom-type name=kind` flag,
where kind is one of `bytes`, `string`, `int64`, `uint64`, `int32`, `uint32` or
`double`:

```
protobus generate -custom-type decimal=string -custom-type money=int64
```

The declared types are generated into one extra package, `<out>/custom_types`
(Go package `custom_types`); a field `decimal amount = 1;` then has the type
`*custom_types.Decimal`, whose `Value` is a `*string`. A schema that uses a
custom type without declaring it fails to generate, and the error names the
type (`field Shop.Price.amount: unknown type decimal`). An unsupported kind, a
name that is not an identifier, or `bigint`/`timestamp` is refused as a usage
error.

At runtime the same declaration is `protoload.WithCustomType(name, kind)`;
see below.

## Loading schemas at runtime

The `protoload` package compiles `.proto` files at runtime, without protoc,
the way the TypeScript and Python ports load them. It adds the built-in type
import exactly as the CLI does (on the line of the `syntax` statement, so error
positions still match your file), and resolves the built-ins to the same
descriptors `pbtypes` is generated from, so dynamic and generated code agree.

| Function | Does |
|---|---|
| `protoload.Load(ctx, dirs, opts...)` | Compiles every `.proto` under `dirs`, recursively; each directory is an import root |
| `protoload.Parse(ctx, sources, opts...)` | Compiles in-memory sources, keyed by path |
| `protoload.WithCustomType(name, kind)` | Declares an application custom type, as `registerType` does in TypeScript: a root-level message `name { optional <kind> value = 1; }` usable like a scalar. Kinds: bytes, string, int64, uint64, int32, uint32, double |

The `Result` holds `Files` and `Types` registries (built-ins as the generated
`pbtypes` types, everything else as `dynamicpb` messages, or the generated type
when one with the same descriptor is linked in) and `Compiled`, the files
loaded. Hand the registries to `Dial` with `WithRegistry`, then serve with
`bus.RegisterDynamic` and call with `NewClient(...).Call`, `CallStream` and
`NewRequest`, or subscribe with `protobus.SubscribeType`:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/protoload"
)

func main() {
	ctx := context.Background()
	schema, err := protoload.Load(ctx, []string{"./proto"})
	if err != nil {
		log.Fatal(err)
	}
	bus, err := protobus.Dial(ctx, "amqp://guest:guest@localhost:5672/",
		protobus.WithRegistry(schema.Files, schema.Types))
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()

	// Serve Calculator.Service.add with no generated code.
	svc, err := bus.RegisterDynamic("Calculator.Service", protobus.DynamicHandlers{
		Unary: map[string]protobus.DynamicHandler{
			"add": func(ctx context.Context, req proto.Message) (proto.Message, error) {
				in := req.ProtoReflect()
				a := in.Get(in.Descriptor().Fields().ByName("a")).Int()
				b := in.Get(in.Descriptor().Fields().ByName("b")).Int()
				// The response type comes from the same registry.
				mt, err := schema.Types.FindMessageByName("Calculator.AddResponse")
				if err != nil {
					return nil, err
				}
				m := mt.New()
				m.Set(m.Descriptor().Fields().ByName("result"), protoreflect.ValueOfInt32(int32(a+b)))
				return m.Interface(), nil
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := svc.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Call it, also with no generated code.
	client := protobus.NewClient(bus, "Calculator.Service")
	req, err := client.NewRequest("add")
	if err != nil {
		log.Fatal(err)
	}
	r := req.ProtoReflect()
	r.Set(r.Descriptor().Fields().ByName("a"), protoreflect.ValueOfInt32(2))
	r.Set(r.Descriptor().Fields().ByName("b"), protoreflect.ValueOfInt32(3))
	resp, err := client.Call(ctx, "add", req)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp) // result:5
}
```

A declared method with no handler answers `PROTOCOL_ERROR`, as an
unimplemented generated method does.
