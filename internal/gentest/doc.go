// Package gentest holds generated protobus bindings used by tests.
package gentest

//go:generate protoc -I ../../pbtypes/proto -I . --go_out=. --go_opt=paths=source_relative --go-protobus_out=. --go-protobus_opt=paths=source_relative gentest.proto
