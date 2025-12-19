package protobus

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// RunnableService wraps a BaseService with lifecycle management.
type RunnableService struct {
	*BaseService
	cleanupFn  func() error
	shutdownCh chan struct{}
}

// NewRunnableService creates a RunnableService.
// It derives the proto filename from the service name by convention:
// "Calculator.Service" -> "Calculator.proto"
func NewRunnableService(ctx *Context, serviceName string, options *ServiceOptions) *RunnableService {
	// Derive proto filename from service name
	parts := strings.Split(serviceName, ".")
	protoFileName := parts[0] + ".proto"

	return &RunnableService{
		BaseService: NewBaseService(ctx, serviceName, protoFileName, options),
		shutdownCh:  make(chan struct{}),
	}
}

// NewRunnableServiceWithProto creates a RunnableService with explicit proto filename.
func NewRunnableServiceWithProto(ctx *Context, serviceName, protoFileName string, options *ServiceOptions) *RunnableService {
	return &RunnableService{
		BaseService: NewBaseService(ctx, serviceName, protoFileName, options),
		shutdownCh:  make(chan struct{}),
	}
}

// SetCleanup sets the cleanup function called on shutdown.
func (s *RunnableService) SetCleanup(fn func() error) {
	s.cleanupFn = fn
}

// Run runs the service until a shutdown signal is received.
func (s *RunnableService) Run(ctx context.Context) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	logInfo("Service %s running, press Ctrl+C to stop", s.serviceName)

	select {
	case sig := <-sigCh:
		logInfo("Received %v, shutting down gracefully...", sig)
	case <-ctx.Done():
		logInfo("Context cancelled, shutting down...")
	case <-s.shutdownCh:
		logInfo("Shutdown requested...")
	}

	// Run cleanup
	if s.cleanupFn != nil {
		logInfo("Running cleanup for %s...", s.serviceName)
		if err := s.cleanupFn(); err != nil {
			logError("Cleanup error: %v", err)
		}
	}

	// Stop the base service
	if err := s.Stop(); err != nil {
		logError("Stop error: %v", err)
	}

	logInfo("Service %s stopped", s.serviceName)
	return nil
}

// Shutdown triggers a graceful shutdown.
func (s *RunnableService) Shutdown() {
	close(s.shutdownCh)
}

// Start is a convenience function to bootstrap and run a service.
// The serviceSetup function should configure the service (register handlers, etc.)
func Start(ctx *Context, serviceName string, serviceSetup func(*RunnableService) error, options *ServiceOptions) error {
	service := NewRunnableService(ctx, serviceName, options)

	// Let the setup function configure the service
	if serviceSetup != nil {
		if err := serviceSetup(service); err != nil {
			return err
		}
	}

	// Parse proto (empty source is OK)
	ctx.Factory().Parse("", serviceName)

	// Initialize
	if err := service.Init(); err != nil {
		return err
	}

	logInfo("Service %s started", serviceName)

	// Run (blocks until shutdown)
	return service.Run(context.Background())
}

// StartWithHandlers is a convenience to start a service with handlers registered via reflection.
func StartWithHandlers(ctx *Context, serviceName string, handlers interface{}, options *ServiceOptions) error {
	return Start(ctx, serviceName, func(s *RunnableService) error {
		s.RegisterHandlers(handlers)
		return nil
	}, options)
}
