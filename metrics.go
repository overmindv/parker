package parker

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics — агрегатор HTTP-метрик сервиса (RPS, latency, error-rate),
// публикуемых на GET /metrics в формате Prometheus.
type Metrics struct {
	Enabled         bool
	registry        *prometheus.Registry
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	readyGauge      *prometheus.GaugeVec
}

// NewMetrics регистрирует метрики запросов в отдельном prometheus-регистре.
func NewMetrics(enabled bool) *Metrics {
	registry := prometheus.NewRegistry()

	requestsTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "parker_http_requests_total",
		Help: "Всего HTTP-запросов по service/method/route/status.",
	}, []string{"service", "method", "route", "status"})

	requestDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "parker_http_request_duration_seconds",
		Help:    "Длительность HTTP-запросов (гистограмма) по service/method/route.",
		Buckets: prometheus.DefBuckets, // 12 buckets по умолчанию
	}, []string{"service", "method", "route"})

	// Gauge-статус /ready (1 — готов, 0 — нет) для алерта ServiceDown.
	readyGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "parker_ready",
		Help: "Готовность сервиса (1 — /ready ok, 0 — нет) по service.",
	}, []string{"service"})

	registry.MustRegister(requestsTotal, requestDuration, readyGauge)

	return &Metrics{
		Enabled:         enabled,
		registry:        registry,
		requestsTotal:   requestsTotal,
		requestDuration: requestDuration,
		readyGauge:      readyGauge,
	}
}

// SetReady обновляет gauge готовности для сервиса.
func (m *Metrics) SetReady(service string, ready bool) {
	v := 0.0
	if ready {
		v = 1
	}
	m.readyGauge.WithLabelValues(service).Set(v)
}

// observe записывает метрику одного запроса.
func (m *Metrics) observe(service, method, route string, status int, dur time.Duration) {
	m.requestsTotal.WithLabelValues(service, method, route, strconv.Itoa(status)).Inc()
	m.requestDuration.WithLabelValues(service, method, route).Observe(dur.Seconds())
}

// Handler возвращает HTTP-обработчик для GET /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// middleware замеряет метод/путь/статус/длительность каждого запроса.
func (m *Metrics) middleware(service string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			m.observe(service, r.Method, normalizeRoute(r.URL.Path), rec.status, time.Since(start))
		})
	}
}

// normalizeRoute приводит путь к низкокардинальному виду: сегменты-идентификаторы
// (целые числа и UUID) заменяются на {id}, чтобы не плодить серии по конкретным id.
func normalizeRoute(path string) string {
	parts := strings.Split(path, "/")
	changed := false
	for i, p := range parts {
		if looksLikeID(p) {
			parts[i] = "{id}"
			changed = true
		}
	}
	if !changed {
		return path
	}
	return strings.Join(parts, "/")
}

func looksLikeID(p string) bool {
	if p == "" {
		return false
	}
	if _, err := strconv.ParseInt(p, 10, 64); err == nil {
		return true
	}
	// UUID-подобный сегмент: 8-4-4-4-12 hex.
	if len(p) != 36 {
		return false
	}
	for i, r := range p {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return false
			}
			continue
		}
		hexDigit := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
		if !hexDigit {
			return false
		}
	}
	return true
}
