package protobus

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
)

func TestHandledErrorDefaultsItsCode(t *testing.T) {
	err := NewHandledError("", "name is required")
	if err.Code != CodeHandled || err.Message != "name is required" {
		t.Fatalf("got %+v", err)
	}
	if err.Error() != "name is required" {
		t.Fatalf("Error() = %q", err.Error())
	}
}

func TestIsHandledSeesThroughWrapping(t *testing.T) {
	inner := NewHandledError("VALIDATION", "bad input")
	wrapped := fmt.Errorf("creating order: %w", inner)
	h, ok := AsHandled(wrapped)
	if !ok || h != inner {
		t.Fatal("a wrapped HandledError must still be recognised as handled")
	}
	if _, ok := AsHandled(errors.New("boom")); ok {
		t.Fatal("a plain error is not handled")
	}
	if _, ok := AsHandled(nil); ok {
		t.Fatal("nil is not handled")
	}
}

func TestProtocolErrorIsHandled(t *testing.T) {
	err := newProtocolError("request envelope did not decode")
	h, ok := AsHandled(err)
	if !ok || h.Code != CodeProtocol {
		t.Fatalf("protocol errors are handled with code %s, got %+v", CodeProtocol, h)
	}
}

func TestPublishErrorClassifiesAmbiguity(t *testing.T) {
	cases := []struct {
		sentinel  error
		code      string
		ambiguous bool
	}{
		{ErrPublishNacked, CodePublishNacked, false},
		{ErrUnroutable, CodeUnroutable, false},
		{ErrPublishConfirmTimeout, CodePublishConfirmTimeout, true},
		{ErrChannelClosed, CodeChannelClosed, true},
	}
	for _, c := range cases {
		err := error(&PublishError{Err: c.sentinel, MessageID: "m-1", Exchange: "proto.bus", RoutingKey: "REQUEST.a.B.c"})
		if !errors.Is(err, c.sentinel) {
			t.Errorf("%v: errors.Is must match the sentinel", c.sentinel)
		}
		var pe *PublishError
		if !errors.As(err, &pe) || pe.MessageID != "m-1" {
			t.Errorf("%v: the messageId must be reachable for deduplication", c.sentinel)
		}
		if pe.Ambiguous() != c.ambiguous {
			t.Errorf("%v: Ambiguous() = %v", c.sentinel, pe.Ambiguous())
		}
		if got := ErrorCode(err); got != c.code {
			t.Errorf("%v: code %q, want %q", c.sentinel, got, c.code)
		}
		if !strings.Contains(err.Error(), "REQUEST.a.B.c") {
			t.Errorf("message should name the destination: %q", err.Error())
		}
	}
}

func TestRemoteErrorCarriesCode(t *testing.T) {
	err := error(&RemoteError{Method: "a.B.c", Code: "NOT_FOUND", Message: "no such account"})
	if ErrorCode(err) != "NOT_FOUND" {
		t.Fatalf("code %q", ErrorCode(err))
	}
	if err.Error() != "no such account" {
		t.Fatalf("Error() = %q; a remote error reads as the message the service sent", err.Error())
	}
	if !IsCode(fmt.Errorf("calling: %w", err), "NOT_FOUND") {
		t.Fatal("IsCode must see through wrapping")
	}
}

func TestErrorCodeOfLocalFailures(t *testing.T) {
	cases := map[error]string{
		ErrRPCTimeout:                      CodeRPCTimeout,
		ErrNotReady:                        CodeNotReady,
		NewHandledError("X", "y"):          "X",
		errors.New("plain"):                "",
		nil:                                "",
		fmt.Errorf("w: %w", ErrRPCTimeout): CodeRPCTimeout,
	}
	for err, want := range cases {
		if got := ErrorCode(err); got != want {
			t.Errorf("ErrorCode(%v) = %q, want %q", err, got, want)
		}
	}
}

func TestRPCTimeoutIsAlsoADeadline(t *testing.T) {
	err := rpcTimeoutError("REQUEST.a.B.c", "cid", context.DeadlineExceeded)
	if !errors.Is(err, ErrRPCTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("an RPC timeout must satisfy both ErrRPCTimeout and context.DeadlineExceeded: %v", err)
	}
	if !strings.Contains(err.Error(), "cid") {
		t.Fatalf("the correlationId belongs in the message: %q", err.Error())
	}
}

type codedErr struct{}

func (codedErr) Error() string { return "secret connection string amqp://u:p@h" }
func (codedErr) Code() string  { return "ECONNRESET" }

func TestSafeErrorSummaryNeverDisclosesUnhandledText(t *testing.T) {
	// x-last-error persists in queues read by dashboards; an unhandled
	// error's message routinely quotes the data that caused it.
	cases := []struct {
		err  error
		want string
	}{
		{nil, "UnknownError"},
		{errors.New("password=hunter2"), "Error"},
		{codedErr{}, "codedErr[ECONNRESET]"},
		{&fs.PathError{Op: "open", Path: "/etc/secret", Err: fs.ErrNotExist}, "PathError"},
		{NewHandledError("VALIDATION", "name is required"), "HandledError[VALIDATION]: name is required"},
		{fmt.Errorf("wrapped: %w", NewHandledError("V", "m")), "HandledError[V]: m"},
		{newProcessingTimeoutError("cid", 0), "TimeoutError[PROCESSING_TIMEOUT]"},
	}
	for _, c := range cases {
		got := safeErrorSummary(c.err)
		if got != c.want {
			t.Errorf("safeErrorSummary(%v) = %q, want %q", c.err, got, c.want)
		}
		if c.err != nil && strings.Contains(got, "hunter2") || strings.Contains(got, "/etc/secret") {
			t.Errorf("summary leaked text: %q", got)
		}
	}
}

func TestSanitizeForCaller(t *testing.T) {
	handled := NewHandledError("V", "visible")
	unhandled := errors.New("internal detail")

	if got := sanitizeForCaller(handled, false); got.Message != "visible" || got.Code != "V" {
		t.Fatalf("a HandledError always crosses: %+v", got)
	}
	if got := sanitizeForCaller(unhandled, true); got.Message != "internal detail" || got.Code != "" {
		t.Fatalf("exposed unhandled error: %+v", got)
	}
	if got := sanitizeForCaller(unhandled, false); got.Message != "internal service error" || got.Code != CodeInternal {
		t.Fatalf("hidden unhandled error: %+v", got)
	}
	coded := sanitizeForCaller(codedErr{}, true)
	if coded.Code != "ECONNRESET" {
		t.Fatalf("an exposed error keeps its own code: %+v", coded)
	}
	timeout := sanitizeForCaller(newProcessingTimeoutError("cid", 0), false)
	if timeout.Code != CodeProcessingTimeout {
		t.Fatalf("a processing timeout tells the caller what happened even when hidden: %+v", timeout)
	}
}
