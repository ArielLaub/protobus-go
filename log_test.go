package protobus

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestRedactURL(t *testing.T) {
	cases := map[string]string{
		"amqp://user:s3cret@broker:5672/vhost":   "amqp://user:***@broker:5672/vhost",
		"amqps://user:s3cret@broker/%2f?x=1":     "amqps://user:***@broker/%2f?x=1",
		"amqp://guest@localhost":                 "amqp://guest@localhost",
		"amqp://localhost:5672/":                 "amqp://localhost:5672/",
		"not a url with password p@ss:word\x7f": "<redacted>",
		"":                                       "",
	}
	for in, want := range cases {
		got := RedactURL(in)
		if got != want {
			t.Errorf("RedactURL(%q) = %q, want %q", in, got, want)
		}
		if strings.Contains(got, "s3cret") {
			t.Errorf("password leaked: %q", got)
		}
	}
}

func TestLevelFromEnv(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug, "INFO": slog.LevelInfo, "warn": slog.LevelWarn,
		"warning": slog.LevelWarn, "error": slog.LevelError, "": slog.LevelInfo, "bogus": slog.LevelInfo,
	}
	for raw, want := range cases {
		t.Setenv("LOG_LEVEL", raw)
		got, silent := levelFromEnv()
		if silent || got != want {
			t.Errorf("LOG_LEVEL=%q -> %v (silent %v), want %v", raw, got, silent, want)
		}
	}
	for _, raw := range []string{"silent", "off", "none"} {
		t.Setenv("LOG_LEVEL", raw)
		if _, silent := levelFromEnv(); !silent {
			t.Errorf("LOG_LEVEL=%q must silence logging", raw)
		}
	}
}

func TestDefaultLoggerSuppressesDebugAndTagsComponent(t *testing.T) {
	t.Setenv("LOG_LEVEL", "")
	var buf bytes.Buffer
	l := defaultLogger(&buf)
	l.Debug("hidden")
	l.Info("shown")
	out := buf.String()
	if strings.Contains(out, "hidden") {
		t.Fatal("debug must be off by default: payload-level detail is opt-in")
	}
	if !strings.Contains(out, "shown") || !strings.Contains(out, "component=protobus") {
		t.Fatalf("unexpected output %q", out)
	}
}

func TestSilentLoggerDropsEverything(t *testing.T) {
	t.Setenv("LOG_LEVEL", "silent")
	var buf bytes.Buffer
	defaultLogger(&buf).Error("nothing")
	if buf.Len() != 0 {
		t.Fatalf("silent level must emit nothing, got %q", buf.String())
	}
}

func TestLogAttrsUseTheSharedFieldNames(t *testing.T) {
	// Field names match the TypeScript LogRecord so logs from a mixed
	// deployment aggregate under one schema.
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, nil))
	l.LogAttrs(context.Background(), slog.LevelInfo, "x",
		attrOperation("publish"), attrCorrelationID("c1"), attrMessageID("m1"), attrQueue("q"),
		attrExchange("e"), attrRoutingKey("rk"), attrMethod("a.B.c"), attrSize(12), attrErrorName("TimeoutError"),
		attrOutcome(outcomeConfirmed), attrAttempt(2), attrService("a.B"))
	for _, want := range []string{"operation=publish", "correlationId=c1", "messageId=m1", "queue=q", "exchange=e",
		"routingKey=rk", "method=a.B.c", "sizeBytes=12", "errorName=TimeoutError", "outcome=confirmed", "attempt=2", "service=a.B"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("missing %q in %q", want, buf.String())
		}
	}
}

func TestErrAttrNeverQuotesUnhandledText(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, nil))
	l.Info("x", attrSafeError(errorString("password=hunter2")))
	if strings.Contains(buf.String(), "hunter2") {
		t.Fatalf("safe error attr leaked text: %q", buf.String())
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }
