package parker

import (
	"context"

	internalhealth "github.com/overmindv/parker/internal/health"
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
	registry internalhealth.Registry
}

// NewHealthRegistry создаёт пустой реестр.
func NewHealthRegistry() *HealthRegistry {
	return &HealthRegistry{}
}

// Add регистрирует проверку по имени.
func (h *HealthRegistry) Add(name string, check HealthCheck) {
	h.registry.Add(name, check)
}

// Enabled сообщает, зарегистрирована ли хотя бы одна проверка.
func (h *HealthRegistry) Enabled() bool { return h.registry.Enabled() }

// Ready проверяет все зарегистрированные зависимости.
func (h *HealthRegistry) Ready(ctx context.Context) error { return h.registry.Ready(ctx) }
