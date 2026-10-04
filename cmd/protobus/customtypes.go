package main

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ArielLaub/protobus-go/v2/protoload"
)

// customKinds are the wire kinds a user custom type can wrap, by the names
// the -custom-type flag accepts.
var customKinds = map[string]protoreflect.Kind{
	"bytes": protoreflect.BytesKind, "string": protoreflect.StringKind,
	"int64": protoreflect.Int64Kind, "uint64": protoreflect.Uint64Kind,
	"int32": protoreflect.Int32Kind, "uint32": protoreflect.Uint32Kind,
	"double": protoreflect.DoubleKind,
}

// customTypes collects repeated -custom-type name=kind flags: the CLI
// counterpart of protoload.WithCustomType (registerType in the TypeScript
// port).
type customTypes []protoload.Option

func (c *customTypes) String() string { return "" }

func (c *customTypes) Set(v string) error {
	name, kind, ok := strings.Cut(v, "=")
	if !ok {
		return fmt.Errorf("want name=kind, got %q", v)
	}
	k, ok := customKinds[kind]
	if !ok {
		return fmt.Errorf("custom type %q: unsupported kind %q (bytes, string, int64, uint64, int32, uint32 or double)", name, kind)
	}
	opt := protoload.WithCustomType(name, k)
	// Validate the name now, so a bad flag is a usage error.
	if _, err := protoload.Parse(context.Background(), map[string]string{}, opt); err != nil {
		return err
	}
	*c = append(*c, opt)
	return nil
}
