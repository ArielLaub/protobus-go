package protobus

import (
	"context"
	"fmt"
	"sync"
)

// ServiceCluster manages multiple services.
type ServiceCluster struct {
	mu       sync.RWMutex
	ctx      *Context
	services []*RunnableService
}

// NewServiceCluster creates a new ServiceCluster.
func NewServiceCluster(ctx *Context) *ServiceCluster {
	return &ServiceCluster{
		ctx:      ctx,
		services: make([]*RunnableService, 0),
	}
}

// Add adds a service to the cluster.
func (c *ServiceCluster) Add(service *RunnableService) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.services = append(c.services, service)
}

// AddNew creates and adds a new service to the cluster.
func (c *ServiceCluster) AddNew(serviceName string, setup func(*RunnableService) error, options *ServiceOptions) error {
	service := NewRunnableService(c.ctx, serviceName, options)

	if setup != nil {
		if err := setup(service); err != nil {
			return fmt.Errorf("failed to setup service %s: %w", serviceName, err)
		}
	}

	c.Add(service)
	return nil
}

// Init initializes all services in the cluster.
func (c *ServiceCluster) Init() error {
	c.mu.RLock()
	services := make([]*RunnableService, len(c.services))
	copy(services, c.services)
	c.mu.RUnlock()

	for _, service := range services {
		c.ctx.Factory().Parse("", service.ServiceName())

		if err := service.Init(); err != nil {
			return fmt.Errorf("failed to init service %s: %w", service.ServiceName(), err)
		}
	}

	logInfo("ServiceCluster initialized with %d services", len(services))
	return nil
}

// Run runs all services in the cluster until ctx is cancelled.
func (c *ServiceCluster) Run(ctx context.Context) error {
	c.mu.RLock()
	services := make([]*RunnableService, len(c.services))
	copy(services, c.services)
	c.mu.RUnlock()

	if len(services) == 0 {
		return nil
	}

	// Wait for context cancellation
	<-ctx.Done()

	// Stop all services
	c.Stop()

	return ctx.Err()
}

// Stop stops all services in the cluster.
func (c *ServiceCluster) Stop() {
	c.mu.RLock()
	services := make([]*RunnableService, len(c.services))
	copy(services, c.services)
	c.mu.RUnlock()

	for _, service := range services {
		if err := service.Stop(); err != nil {
			logWarn("Failed to stop service %s: %v", service.ServiceName(), err)
		}
	}

	logInfo("ServiceCluster stopped")
}

// Services returns the list of services.
func (c *ServiceCluster) Services() []*RunnableService {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*RunnableService, len(c.services))
	copy(result, c.services)
	return result
}

// Get returns a service by name.
func (c *ServiceCluster) Get(serviceName string) *RunnableService {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, service := range c.services {
		if service.ServiceName() == serviceName {
			return service
		}
	}
	return nil
}
