// Command protobus generates Go code for protobus schemas, without protoc.
//
//	protobus generate               Go message types and service bindings for every .proto
//	protobus generate:service NAME  a runnable service skeleton for NAME.proto
//	protobus init                   setup instructions
//	protobus version
//
// Schemas are shared with the TypeScript and Python ports as they are: no
// go_package option and no import for the bigint and timestamp types is
// needed. Each proto package becomes a Go package under the output directory
// (package Calculator → <out>/calculator).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/ArielLaub/protobus-go/v2/internal/gen"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

const usage = `protobus-go CLI v%s

Usage:
  protobus generate [-proto DIR] [-out DIR] [-dry-run]
      Generate Go message types and protobus bindings for every .proto under
      -proto (default ./proto) into -out (default ./gen), inside the current
      Go module.

  protobus generate:service NAME [-proto DIR] [-out DIR] [-services DIR]
      Write a runnable skeleton for the first service in NAME.proto into
      -services/<name>/main.go (default ./services). Never overwrites.

  protobus init       Show project setup instructions
  protobus version    Show the version
  protobus help       Show this help
`

const initText = `Protobus-go project setup
=========================

1. Create a module and the directories:

     go mod init example.com/app
     mkdir -p proto gen services

2. Add the library and the CLI:

     go get github.com/ArielLaub/protobus-go/v2
     go install github.com/ArielLaub/protobus-go/v2/cmd/protobus@latest

3. Write proto/Calculator.proto (shared verbatim with TypeScript and Python
   services; no go_package and no imports needed):

     syntax = "proto3";
     package Calculator;

     service Service {
       rpc add(AddRequest) returns (AddResponse);
     }
     message AddRequest { int32 a = 1; int32 b = 2; }
     message AddResponse { int32 result = 1; }

4. Generate code, and a service to start from:

     protobus generate
     protobus generate:service Calculator

5. Run RabbitMQ and the service:

     docker run -d -p 5672:5672 rabbitmq:3-management
     go run ./services/calculator

Add "//go:generate protobus generate" to a Go file to regenerate with
"go generate ./...".
`

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintf(stderr, usage, gen.Version)
		return 1
	}
	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "help", "-h", "--help":
		fmt.Fprintf(stdout, usage, gen.Version)
		return 0
	case "version", "-v", "--version":
		fmt.Fprintln(stdout, "protobus-go", gen.Version)
		return 0
	case "init":
		fmt.Fprint(stdout, initText)
		return 0
	case "generate":
		err = runGenerate(ctx, rest, stdout, stderr)
	case "generate:service":
		err = runGenerateService(ctx, rest, stdout, stderr)
	default:
		err = fmt.Errorf("unknown command %q (see protobus help)", cmd)
	}
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(stderr, "Error:", err)
		}
		return 1
	}
	return 0
}

func runGenerate(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	o := generateOptions{}
	fs.StringVar(&o.protoDir, "proto", "./proto", "directory of .proto files (searched recursively)")
	fs.StringVar(&o.outDir, "out", "./gen", "output directory, inside the current Go module")
	fs.BoolVar(&o.dryRun, "dry-run", false, "list the files that would be written")
	fs.Var(&o.custom, "custom-type", "declare a custom type, name=kind (repeatable; kind is bytes, string, int64, uint64, int32, uint32 or double)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %v", fs.Args())
	}
	files, err := generate(ctx, o)
	if err != nil {
		return err
	}
	for _, f := range files {
		fmt.Fprintln(stdout, f)
	}
	return nil
}

func runGenerateService(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || len(args[0]) > 0 && args[0][0] == '-' {
		return errors.New("generate:service needs a service name, e.g. protobus generate:service Calculator")
	}
	o := serviceOptions{name: args[0]}
	fs := flag.NewFlagSet("generate:service", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.protoDir, "proto", "./proto", "directory of .proto files")
	fs.StringVar(&o.outDir, "out", "./gen", "where protobus generate writes the generated code")
	fs.StringVar(&o.servicesDir, "services", "./services", "where to write the service")
	fs.Var(&o.custom, "custom-type", "declare a custom type, name=kind (repeatable), as for generate")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	path, err := generateService(ctx, o)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, path)
	return nil
}
