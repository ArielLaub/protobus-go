package protobus

import (
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"
)

// Logging.
//
// protobus logs through log/slog. Pass your own *slog.Logger with WithLogger;
// otherwise a text logger on stderr is used, at the level named by LOG_LEVEL
// (debug, info, warn, error, or silent/off/none; default info).
//
// What the library logs is framework metadata only: operation, queue,
// exchange, routing key, correlation and message ids, sizes, durations and
// error class names. Message payloads, headers and broker credentials are
// never logged, and neither is the text of an unhandled error that crosses a
// process boundary. Attribute names match the TypeScript port's LogRecord so
// logs from a mixed deployment aggregate under one schema.

func levelFromEnv() (level slog.Level, silent bool) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		return slog.LevelDebug, false
	case "warn", "warning":
		return slog.LevelWarn, false
	case "error":
		return slog.LevelError, false
	case "silent", "off", "none":
		return 0, true
	default:
		return slog.LevelInfo, false
	}
}

func defaultLogger(w io.Writer) *slog.Logger {
	level, silent := levelFromEnv()
	if silent {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})).With(slog.String("component", "protobus"))
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
func attrCorrelationID(v string) slog.Attr { return slog.String("correlationId", v) }
func attrMessageID(v string) slog.Attr     { return slog.String("messageId", v) }
func attrQueue(v string) slog.Attr         { return slog.String("queue", v) }
func attrExchange(v string) slog.Attr      { return slog.String("exchange", v) }
func attrRoutingKey(v string) slog.Attr    { return slog.String("routingKey", v) }
func attrMethod(v string) slog.Attr        { return slog.String("method", v) }
func attrService(v string) slog.Attr       { return slog.String("service", v) }
func attrMessageType(v string) slog.Attr   { return slog.String("messageType", v) }
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
func attrSafeError(err error) slog.Attr { return slog.String("error", safeErrorSummary(err)) }

// attrError is the full error, for a service's log of its own failures.
func attrError(err error) slog.Attr { return slog.Any("error", err) }
