// Command protoc-gen-go-protobus is a protoc plugin that generates protobus
// service bindings: for every service, a server interface with its
// registration function and a typed client.
//
// Use it next to protoc-gen-go, which generates the message types:
//
//	protoc --go_out=. --go_opt=paths=source_relative \
//	       --go-protobus_out=. --go-protobus_opt=paths=source_relative \
//	       -I . -I $(go list -m -f '{{.Dir}}' github.com/ArielLaub/protobus-go/v2)/pbtypes/proto \
//	       calc.proto
//
// Schemas using the bigint or timestamp custom types need
// `import "protobus/types.proto";` under protoc. The protobus CLI
// (`protobus generate`) needs no protoc and adds the import itself.
package main

import (
	"flag"
	"fmt"
	"os"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/ArielLaub/protobus-go/v2/internal/gen"
)

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "--version" || os.Args[1] == "-v") {
		fmt.Println("protoc-gen-go-protobus", gen.Version)
		return
	}
	var flags flag.FlagSet
	protogen.Options{ParamFunc: flags.Set}.Run(func(p *protogen.Plugin) error {
		p.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		for _, f := range p.Files {
			if !f.Generate {
				continue
			}
			if _, err := gen.GenerateFile(p, f); err != nil {
				return err
			}
		}
		return nil
	})
}
