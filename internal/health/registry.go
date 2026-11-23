// Package health реализует реестр readiness-проверок Parker.
package health

import (
	"context"
	"fmt"
	"sync"
)

type Check interface{ Ping(context.Context) error }
type CheckFunc func(context.Context) error

func (f CheckFunc) Ping(ctx context.Context) error { return f(ctx) }

type Registry struct {
	mu      sync.RWMutex
	checks  map[string]Check
	enabled bool
}

func NewRegistry() *Registry { return &Registry{checks: make(map[string]Check)} }
func (r *Registry) Add(name string, check Check) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.checks == nil {
		r.checks = make(map[string]Check)
	}
	r.checks[name] = check
	r.enabled = true
}
func (r *Registry) Enabled() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.enabled
}
func (r *Registry) Ready(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for name, check := range r.checks {
		if err := check.Ping(ctx); err != nil {
			return fmt.Errorf("%s недоступен: %w", name, err)
		}
	}

	return nil
}
