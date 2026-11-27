package parker

import (
	"net/http"

	"github.com/overmindv/parker/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics — агрегатор HTTP-метрик сервиса (RPS, latency, error-rate),
// публикуемых на GET /metrics в формате Prometheus.
type Metrics observability.Metrics

// NewMetrics регистрирует метрики запросов в отдельном prometheus-регистре.
func NewMetrics(enabled bool) *Metrics {
	return (*Metrics)(observability.NewMetrics(enabled))
}

// SetReady обновляет gauge готовности для сервиса.
func (m *Metrics) SetReady(service string, ready bool) {
	(*observability.Metrics)(m).SetReady(service, ready)
}

// Register добавляет метрику сервиса в его реестр /metrics.
func (m *Metrics) Register(collector prometheus.Collector) error {
	return (*observability.Metrics)(m).Register(collector)
}

// Handler возвращает HTTP-обработчик для GET /metrics.
func (m *Metrics) Handler() http.Handler {
	return (*observability.Metrics)(m).Handler()
}
