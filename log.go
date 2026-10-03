package protobus

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"
)

// Logging.
//
// protobus logs through log/slog. Pass your own *slog.Logger with WithLogger;
// otherwise slog.Default is used, filtered by LOG_LEVEL when it is set
// (debug, info, warn, error, or silent/off/none).
//
// What the library logs is framework metadata only: operation, queue,
// exchange, routing key, correlation and message ids, sizes, durations and
// error class names. Message payloads, headers and broker credentials are
// never logged, and neither is the text of an unhandled error that crosses a
// process boundary. Attribute names match the TypeScript port's LogRecord so
// logs from a mixed deployment aggregate under one schema.

// levelFromEnv reads LOG_LEVEL. set is false when it is unset or not a level
// name, in which case the application's handler decides.
func levelFromEnv() (level slog.Level, silent, set bool) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		return slog.LevelDebug, false, true
	case "info":
		return slog.LevelInfo, false, true
	case "warn", "warning":
		return slog.LevelWarn, false, true
	case "error":
		return slog.LevelError, false, true
	case "silent", "off", "none":
		return 0, true, true
	default:
		return slog.LevelInfo, false, false
	}
}

// defaultLogger is the logger used without WithLogger: the application's
// handler (slog.Default's, unless given), tagged component=protobus, and
// filtered by LOG_LEVEL when that is set, as the other ports are.
func defaultLogger(h slog.Handler) *slog.Logger {
	if h == nil {
		h = slog.Default().Handler()
	}
	level, silent, set := levelFromEnv()
	switch {
	case silent:
		return slog.New(slog.DiscardHandler)
	case set:
		h = minLevel{Handler: h, min: level}
	}
	return slog.New(h).With(slog.String("component", "protobus"))
}

// minLevel drops records below min, whatever the wrapped handler would keep.
type minLevel struct {
	slog.Handler
	min slog.Level
}

func (m minLevel) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= m.min && m.Handler.Enabled(ctx, l)
}

func (m minLevel) WithAttrs(as []slog.Attr) slog.Handler {
	return minLevel{Handler: m.Handler.WithAttrs(as), min: m.min}
}

func (m minLevel) WithGroup(name string) slog.Handler {
	return minLevel{Handler: m.Handler.WithGroup(name), min: m.min}
}

// redactURL returns a broker URL that is safe to log: the password is
// replaced with *** and everything else that helps diagnose a connection
// (scheme, user, host, port, vhost, parameters) is kept. Anything that does
// not parse as an absolute URL is reported as <redacted>, since it may still
// be a credential.
func redactURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "<redacted>"
	}
	if _, has := u.User.Password(); has {
		const placeholder = "PROTOBUSREDACTED"
		u.User = url.UserPassword(u.User.Username(), placeholder)
		return strings.Replace(u.String(), placeholder, "***", 1)
	}
	return u.String()
}

// Outcome vocabulary, kept small so the field groups well in an aggregator.
const (
	outcomeOK         = "ok"
	outcomeConfirmed  = "confirmed"
	outcomeFailed     = "failed"
	outcomeTimeout    = "timeout"
	outcomeRetried    = "retried"
	outcomeRejected   = "rejected"
	outcomeDropped    = "dropped"
	outcomeUnroutable = "unroutable"
	outcomeDeadLetter = "dead-lettered"
	outcomeCancelled  = "cancelled"
)

func attrOperation(v string) slog.Attr     { return slog.String("operation", v) }
func attrCorrelationID(v string) slog.Attr { return slog.String("correlationId", clip(v)) }
func attrMessageID(v string) slog.Attr     { return slog.String("messageId", clip(v)) }
func attrQueue(v string) slog.Attr         { return slog.String("queue", v) }
func attrExchange(v string) slog.Attr      { return slog.String("exchange", v) }
func attrRoutingKey(v string) slog.Attr    { return slog.String("routingKey", clip(v)) }
func attrMethod(v string) slog.Attr        { return slog.String("method", clip(v)) }
func attrService(v string) slog.Attr       { return slog.String("service", v) }
func attrMessageType(v string) slog.Attr   { return slog.String("messageType", clip(v)) }
func attrSize(n int) slog.Attr             { return slog.Int("sizeBytes", n) }
func attrErrorName(v string) slog.Attr     { return slog.String("errorName", v) }
func attrOutcome(v string) slog.Attr       { return slog.String("outcome", v) }
func attrAttempt(n int) slog.Attr          { return slog.Int("attempt", n) }
func attrDuration(d time.Duration) slog.Attr {
	return slog.Int64("durationMs", d.Milliseconds())
}

// attrSafeError describes an error the way x-last-error does: class and code,
// never an unhandled error's text. Use it for anything that may be forwarded
// or retained; a service's own failure log uses attrError.
// Alongside it go errorName and errorCode, the TypeScript LogRecord fields,
// as an inlined group so they stay top-level.
func attrSafeError(err error) slog.Attr {
	attrs := []any{slog.String("error", safeErrorSummary(err))}
	name, code := "UnknownError", ""
	if h, ok := AsHandled(err); ok {
		name, code = "HandledError", h.Code
	} else if err != nil {
		name, code = errorName(innermostNamed(err)), ErrorCode(err)
	}
	attrs = append(attrs, attrErrorName(name))
	if code != "" {
		attrs = append(attrs, slog.String("errorCode", code))
	}
	return slog.Group("", attrs...)
}

// attrError is the full error, for a service's log of its own failures.
func attrError(err error) slog.Attr { return slog.Any("error", err) }

// maxLogField bounds a logged value that a publisher controls (a method name,
// a routing key, an id), so one message cannot flood the log. slog's handlers
// already escape control characters, so a value cannot forge a line.
const maxLogField = 256

func clip(v string) string {
	if len(v) <= maxLogField {
		return v
	}
	return v[:maxLogField] + "..."
}

// panicSummary describes a recovered panic value for the log. A runtime
// error's text comes from the Go runtime (a nil dereference, an index out of
// range) and is kept; any other value may carry request data, as a
// panic(fmt.Sprintf(...)) does, so only its type is logged. The stack is
// logged beside it.
func panicSummary(v any) string {
	if re, ok := v.(runtime.Error); ok {
		return clip(re.Error())
	}
	return fmt.Sprintf("%T", v)
}
