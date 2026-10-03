package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"unicode"

	gengo "google.golang.org/protobuf/cmd/protoc-gen-go/internal_gengo"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/ArielLaub/protobus-go/v2/internal/gen"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
	"github.com/ArielLaub/protobus-go/v2/protoload"
)

// module describes the Go module code is generated into.
type module struct {
	path string // module path, from go.mod
	root string // directory holding go.mod
}

var moduleLine = regexp.MustCompile(`^module\s+(\S+)`)

// findModule walks up from dir to the nearest go.mod.
func findModule(dir string) (module, error) {
	abs, err := absPath(dir)
	if err != nil {
		return module{}, err
	}
	for d := abs; ; d = filepath.Dir(d) {
		f, err := os.Open(filepath.Join(d, "go.mod"))
		if err == nil {
			defer f.Close()
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				if m := moduleLine.FindStringSubmatch(strings.TrimSpace(sc.Text())); m != nil {
					return module{path: strings.Trim(m[1], `"`), root: d}, nil
				}
			}
			return module{}, fmt.Errorf("%s/go.mod declares no module", d)
		}
		if filepath.Dir(d) == d {
			return module{}, errors.New("no go.mod found; run inside a Go module (go mod init)")
		}
	}
}

// goPackageFor maps a proto package to the Go package generated for it:
// directory <out>/<package, lower-cased, dots as slashes>, named after its
// last element. A schema with its own go_package keeps it.
// absPath resolves dir to an absolute path with symlinks evaluated as far as
// the path exists, so paths reached different ways compare equal.
func absPath(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	// Evaluate the longest existing prefix; the rest may not exist yet.
	rest := ""
	for p := abs; ; p = filepath.Dir(p) {
		if real, err := filepath.EvalSymlinks(p); err == nil {
			return filepath.Join(real, rest), nil
		}
		if filepath.Dir(p) == p {
			return abs, nil
		}
		rest = filepath.Join(filepath.Base(p), rest)
	}
}

func goPackageFor(mod module, outDir string, fd protoreflect.FileDescriptor) (importPath, name string, err error) {
	absOut, err := absPath(outDir)
	if err != nil {
		return "", "", err
	}
	rel, err := filepath.Rel(mod.root, absOut)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", "", fmt.Errorf("output directory %s is outside module %s (%s)", outDir, mod.path, mod.root)
	}
	pkg := string(fd.Package())
	if pkg == "" {
		pkg = strings.TrimSuffix(path.Base(fd.Path()), ".proto")
	}
	segments := strings.Split(strings.ToLower(pkg), ".")
	for i, s := range segments {
		segments[i] = sanitizeIdent(s)
	}
	importPath = path.Join(mod.path, filepath.ToSlash(rel), path.Join(segments...))
	return importPath, segments[len(segments)-1], nil
}

func sanitizeIdent(s string) string {
	var b strings.Builder
	for i, r := range s {
		switch {
		case r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) && i > 0:
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

type generateOptions struct {
	protoDir string
	outDir   string
	dryRun   bool
	custom   customTypes
}

// generate compiles every schema under protoDir and writes Go message types
// and protobus bindings into the module.
func generate(ctx context.Context, o generateOptions) ([]string, error) {
	mod, err := findModule(o.outDir)
	if err != nil {
		return nil, err
	}
	res, err := protoload.Load(ctx, []string{o.protoDir}, o.custom...)
	if err != nil {
		return nil, err
	}
	if len(res.Compiled) == 0 {
		return nil, fmt.Errorf("no .proto files under %s", o.protoDir)
	}

	// Every file in dependency order, as protoc hands them to plugins.
	var files []*descriptorpb.FileDescriptorProto
	seen := map[string]bool{}
	var params []string
	var add func(fd protoreflect.FileDescriptor) error
	add = func(fd protoreflect.FileDescriptor) error {
		if seen[fd.Path()] {
			return nil
		}
		seen[fd.Path()] = true
		for i := range fd.Imports().Len() {
			if err := add(fd.Imports().Get(i).FileDescriptor); err != nil {
				return err
			}
		}
		fdp := protodesc.ToFileDescriptorProto(fd)
		if fdp.GetOptions().GetGoPackage() == "" && fd.Path() != pbtypes.TypesProtoPath {
			imp, name, err := goPackageFor(mod, o.outDir, fd)
			if err != nil {
				return err
			}
			params = append(params, fmt.Sprintf("M%s=%s;%s", fd.Path(), imp, name))
		}
		files = append(files, fdp)
		return nil
	}
	var targets []string
	for _, fd := range res.Compiled {
		if err := add(fd); err != nil {
			return nil, err
		}
		targets = append(targets, fd.Path())
	}
	// User custom types live in a virtual file that needs Go code too.
	if fd, err := res.Files.FindFileByPath("protobus/custom_types.proto"); err == nil {
		targets = append(targets, fd.Path())
	}
	params = append(params, "module="+mod.path)

	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: targets,
		Parameter:      proto.String(strings.Join(params, ",")),
		ProtoFile:      files,
	}
	plugin, err := protogen.Options{}.New(req)
	if err != nil {
		return nil, err
	}
	plugin.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
	for _, f := range plugin.Files {
		if !f.Generate {
			continue
		}
		gengo.GenerateFile(plugin, f)
		if _, err := gen.GenerateFile(plugin, f); err != nil {
			return nil, err
		}
	}
	resp := plugin.Response()
	if resp.Error != nil {
		return nil, errors.New(resp.GetError())
	}

	var written []string
	for _, f := range resp.File {
		target := filepath.Join(mod.root, filepath.FromSlash(f.GetName()))
		if !strings.HasPrefix(target, mod.root+string(filepath.Separator)) {
			return nil, fmt.Errorf("refusing to write %s outside the module", f.GetName())
		}
		written = append(written, target)
		if o.dryRun {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(target, []byte(f.GetContent()), 0o644); err != nil {
			return nil, err
		}
	}
	slices.Sort(written)
	return written, nil
}
