package protobus

import (
	"fmt"
	"log"
	"os"
)

// LogLevel represents the severity of a log message.
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// Logger is the interface for logging.
type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

// DefaultLogger is a simple logger implementation.
type DefaultLogger struct {
	level  LogLevel
	logger *log.Logger
}

// NewDefaultLogger creates a new default logger.
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		level:  level,
		logger: log.New(os.Stdout, "", log.LstdFlags),
	}
}

func (l *DefaultLogger) Debug(msg string, args ...interface{}) {
	if l.level <= LogLevelDebug {
		l.logger.Printf("[DEBUG] "+msg, args...)
	}
}

func (l *DefaultLogger) Info(msg string, args ...interface{}) {
	if l.level <= LogLevelInfo {
		l.logger.Printf("[INFO] "+msg, args...)
	}
}

func (l *DefaultLogger) Warn(msg string, args ...interface{}) {
	if l.level <= LogLevelWarn {
		l.logger.Printf("[WARN] "+msg, args...)
	}
}

func (l *DefaultLogger) Error(msg string, args ...interface{}) {
	if l.level <= LogLevelError {
		l.logger.Printf("[ERROR] "+msg, args...)
	}
}

// globalLogger is the global logger instance
var globalLogger Logger = NewDefaultLogger(LogLevelInfo)

// SetLogger sets the global logger.
func SetLogger(l Logger) {
	globalLogger = l
}

// GetLogger returns the global logger.
func GetLogger() Logger {
	return globalLogger
}

// Package-level logging functions
func logDebug(msg string, args ...interface{}) {
	globalLogger.Debug(msg, args...)
}

func logInfo(msg string, args ...interface{}) {
	globalLogger.Info(msg, args...)
}

func logWarn(msg string, args ...interface{}) {
	globalLogger.Warn(msg, args...)
}

func logError(msg string, args ...interface{}) {
	globalLogger.Error(msg, args...)
}

func logErrorf(format string, args ...interface{}) {
	globalLogger.Error(fmt.Sprintf(format, args...))
}
