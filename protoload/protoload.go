// Package protoload compiles .proto schemas at runtime, the way the
// TypeScript and Python ports load them, without protoc.
//
// Shared protobus schemas use the built-in custom types (bigint, timestamp)
// without importing them, because the other ports predeclare them. protoload
// adds the import itself, on the line of the syntax statement so that error
// positions still point at the user's own lines, and resolves it to the
// descriptors pbtypes is generated from, so dynamic and generated code agree
// on the types.
//
// Use the result with protobus.WithRegistry to serve or call schemas that
// have no generated Go code, or let the protobus CLI turn it into Go.
package protoload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

// customTypesPath is the virtual file declaring user custom types.
const customTypesPath = "protobus/custom_types.proto"

// Result is a compiled schema set.
type Result struct {
	// Files holds every compiled file and its dependencies.
	Files *protoregistry.Files
	// Types holds a message, enum and extension type for every declaration
	// in Files: the generated pbtypes types for the built-ins, dynamicpb
	// types for everything else.
	Types *protoregistry.Types
	// Compiled lists the files that were loaded, by path.
	Compiled []protoreflect.FileDescriptor
}

// Option configures loading.
type Option func(*options) error

type options struct {
	custom []customType
}

type customType struct {
	name string
	kind protoreflect.Kind
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// WithCustomType declares a user custom type, as registerType does in the
// TypeScript port: a root-level message `name { optional <kind> value = 1; }`
// that schemas can use like a scalar. Its encoding is the application's
// business; protobus only carries it.
func WithCustomType(name string, kind protoreflect.Kind) Option {
	return func(o *options) error {
		if !identifier.MatchString(name) {
			return fmt.Errorf("protoload: custom type name %q is not an identifier", name)
		}
		if name == "bigint" || name == "timestamp" {
			return fmt.Errorf("protoload: %q is a built-in type", name)
		}
		switch kind {
		case protoreflect.BytesKind, protoreflect.StringKind, protoreflect.Int64Kind, protoreflect.Uint64Kind,
			protoreflect.Int32Kind, protoreflect.Uint32Kind, protoreflect.DoubleKind:
		default:
			return fmt.Errorf("protoload: custom type %q: unsupported wire kind %v", name, kind)
		}
		o.custom = append(o.custom, customType{name, kind})
		return nil
	}
}

// Load compiles every file ending in ".proto" under dirs, recursively. Each
// directory is an import root: a file imports another by its path relative
// to the root that contains it.
func Load(ctx context.Context, dirs []string, opts ...Option) (*Result, error) {
	var names []string
	seen := map[string]bool{}
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			// HasSuffix, not Contains: notes.protocol.txt and schema.proto.bak
			// are not schemas.
			if d.IsDir() || !strings.HasSuffix(d.Name(), ".proto") {
				return nil
			}
			rel, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if !seen[rel] {
				seen[rel] = true
				names = append(names, rel)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("protoload: %w", err)
		}
	}
	sort.Strings(names)
	read := func(path string) (io.ReadCloser, error) {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(b)), nil
	}
	return compile(ctx, dirs, read, names, opts)
}

// Parse compiles in-memory sources, keyed by path.
func Parse(ctx context.Context, sources map[string]string, opts ...Option) (*Result, error) {
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	read := func(path string) (io.ReadCloser, error) {
		src, ok := sources[path]
		if !ok {
			return nil, fs.ErrNotExist
		}
		return io.NopCloser(strings.NewReader(src)), nil
	}
	return compile(ctx, nil, read, names, opts)
}

func compile(ctx context.Context, roots []string, read func(string) (io.ReadCloser, error), names []string, opts []Option) (*Result, error) {
	var o options
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	customSrc := customTypesSource(o.custom)

	src := &protocompile.SourceResolver{
		ImportPaths: roots,
		Accessor: func(path string) (io.ReadCloser, error) {
			rc, err := read(path)
			if err != nil {
				return nil, err
			}
			defer rc.Close()
			b, err := io.ReadAll(rc)
			if err != nil {
				return nil, err
			}
			return io.NopCloser(bytes.NewReader(injectImports(b, customSrc != ""))), nil
		},
	}
	resolver := protocompile.ResolverFunc(func(path string) (protocompile.SearchResult, error) {
		switch path {
		case pbtypes.TypesProtoPath:
			return protocompile.SearchResult{Desc: pbtypes.File_protobus_types_proto}, nil
		case customTypesPath:
			if customSrc != "" {
				return protocompile.SearchResult{Source: strings.NewReader(customSrc)}, nil
			}
		}
		return src.FindFileByPath(path)
	})
	compiler := protocompile.Compiler{
		Resolver:       protocompile.WithStandardImports(resolver),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	files, err := compiler.Compile(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("protoload: %w", err)
	}

	res := &Result{Files: new(protoregistry.Files), Types: new(protoregistry.Types)}
	registered := map[string]bool{}
	var register func(fd protoreflect.FileDescriptor) error
	register = func(fd protoreflect.FileDescriptor) error {
		if registered[fd.Path()] {
			return nil
		}
		registered[fd.Path()] = true
		for i := range fd.Imports().Len() {
			if err := register(fd.Imports().Get(i).FileDescriptor); err != nil {
				return err
			}
		}
		if err := res.Files.RegisterFile(fd); err != nil {
			return err
		}
		return registerTypes(res.Types, fd)
	}
	for _, f := range files {
		if err := register(f); err != nil {
			return nil, fmt.Errorf("protoload: %w", err)
		}
		res.Compiled = append(res.Compiled, f)
	}
	return res, nil
}

var (
	syntaxLine     = regexp.MustCompile(`(?m)^[ \t]*(syntax|edition)[ \t]*=[ \t]*"[^"]*"[ \t]*;`)
	importsTypes   = regexp.MustCompile(`import\s+(?:public\s+|weak\s+)?"` + regexp.QuoteMeta(pbtypes.TypesProtoPath) + `"`)
	importsCustoms = regexp.MustCompile(`import\s+(?:public\s+|weak\s+)?"` + regexp.QuoteMeta(customTypesPath) + `"`)
)

// injectImports adds the built-in (and user) custom type imports a schema
// does not already have. They go on the line of the syntax statement, or the
// first line when there is none, so every original line keeps its number.
// An unused import is harmless.
func injectImports(src []byte, custom bool) []byte {
	var add []string
	if !importsTypes.Match(src) {
		add = append(add, `import "`+pbtypes.TypesProtoPath+`";`)
	}
	if custom && !importsCustoms.Match(src) {
		add = append(add, `import "`+customTypesPath+`";`)
	}
	if len(add) == 0 {
		return src
	}
	extra := " " + strings.Join(add, " ")
	if loc := syntaxLine.FindIndex(src); loc != nil {
		return slices.Concat(src[:loc[1]], []byte(extra), src[loc[1]:])
	}
	return slices.Concat([]byte(strings.TrimSpace(extra)+" "), src)
}

func customTypesSource(types []customType) string {
	if len(types) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("syntax = \"proto3\";\n")
	for _, t := range types {
		fmt.Fprintf(&b, "message %s { optional %s value = 1; }\n", t.name, t.kind)
	}
	return b.String()
}

// registerTypes adds a type for every declaration in fd: the generated Go
// type when one is linked into the binary with this very descriptor, a
// dynamic type otherwise.
func registerTypes(types *protoregistry.Types, fd protoreflect.FileDescriptor) error {
	var messages func(ms protoreflect.MessageDescriptors) error
	enums := func(es protoreflect.EnumDescriptors) error {
		for i := range es.Len() {
			ed := es.Get(i)
			if gt, err := protoregistry.GlobalTypes.FindEnumByName(ed.FullName()); err == nil && gt.Descriptor() == ed {
				if err := types.RegisterEnum(gt); err != nil {
					return err
				}
				continue
			}
			if err := types.RegisterEnum(dynamicpb.NewEnumType(ed)); err != nil {
				return err
			}
		}
		return nil
	}
	messages = func(ms protoreflect.MessageDescriptors) error {
		for i := range ms.Len() {
			md := ms.Get(i)
			if md.IsMapEntry() {
				continue
			}
			var err error
			if gt, gerr := protoregistry.GlobalTypes.FindMessageByName(md.FullName()); gerr == nil && gt.Descriptor() == md {
				err = types.RegisterMessage(gt)
			} else {
				err = types.RegisterMessage(dynamicpb.NewMessageType(md))
			}
			if err != nil && !errors.Is(err, protoregistry.NotFound) {
				return err
			}
			if err := enums(md.Enums()); err != nil {
				return err
			}
			if err := messages(md.Messages()); err != nil {
				return err
			}
		}
		return nil
	}
	if err := enums(fd.Enums()); err != nil {
		return err
	}
	return messages(fd.Messages())
}
