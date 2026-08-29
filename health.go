package parker

import (
	"context"
	"fmt"
	"sync"
)

// HealthCheck — проверка готовности внешней зависимости (реализуется сервисом,
// либо pgxpool-пулом и т.п. через HealthCheckFunc).
type HealthCheck interface {
	Ping(ctx context.Context) error
}

// HealthCheckFunc адаптирует функцию к интерфейсу HealthCheck.
type HealthCheckFunc func(ctx context.Context) error

// Ping вызывает функцию.
func (f HealthCheckFunc) Ping(ctx context.Context) error { return f(ctx) }

// HealthRegistry хранит проверки готовности и агрегирует их в один Ready().
type HealthRegistry struct {
	mu      sync.RWMutex
	checks  map[string]HealthCheck
	enabled bool
}

// NewHealthRegistry создаёт пустой реестр.
func NewHealthRegistry() *HealthRegistry {
	return &HealthRegistry{checks: make(map[string]HealthCheck)}
}

// Add регистрирует проверку по имени.
func (h *HealthRegistry) Add(name string, c HealthCheck) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = c
	h.enabled = true
}

// Enabled сообщает, зарегистрирована ли хотя бы одна проверка.
func (h *HealthRegistry) Enabled() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.enabled
}

// Ready проверяет все зависимости; возвращает nil, если все готовы,
// либо ошибку с именем первой упавшей проверки.
func (h *HealthRegistry) Ready(ctx context.Context) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for name, check := range h.checks {
		if err := check.Ping(ctx); err != nil {
			return fmt.Errorf("%s недоступен: %w", name, err)
		}
	}

	return nil
}
