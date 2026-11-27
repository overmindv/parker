package config

import (
	"time"

	"github.com/overmindv/parker/internal/kafka"
)

// Options — конфигурация parker: стандартные (инфраструктурные) настройки сервиса.
// Загружается из env через Load() (см. config.go) и модифицируется опциями Main().
type Options struct {
	// ServiceName — имя сервиса (SERVICE_NAME), уходит в логи и в название приложения.
	ServiceName string
	// HTTPAddress — адрес HTTP-сервера (HTTP_ADDR).
	HTTPAddress string
	// Environment — среда: local|development|test|staging|production (ENV).
	Environment string
	// LogLevel — уровень логгера: debug|info|warn|error (LOG_LEVEL).
	LogLevel string

	ReadTimeout     time.Duration // READ_TIMEOUT
	WriteTimeout    time.Duration // WRITE_TIMEOUT
	ShutdownTimeout time.Duration // SHUTDOWN_TIMEOUT

	// Postgres.
	DatabaseURL   string // DATABASE_URL
	MigrationsDir string // MIGRATIONS_DIR

	// Kafka.
	KafkaSecurity kafka.Security
	KafkaBrokers  []string // KAFKA_BOOTSTRAP_SERVERS

	// Observability.
	MetricsEnabled bool // METRICS_ENABLED
	PprofEnabled   bool // PPROF_ENABLED
	Tracing        TraceOptions
}

// TraceOptions настраивает экспорт трасс. Пустой Endpoint отключает экспорт.
type TraceOptions struct {
	Endpoint    string  // PARKER_OTLP_TRACE_ENDPOINT
	SampleRatio float64 // PARKER_TRACE_SAMPLE_RATIO
}

// DefaultOptions возвращает Options с дефолтными значениями стандартных полей.
func DefaultOptions() Options {
	return Options{
		ServiceName:     "parker",
		HTTPAddress:     ":8080",
		Environment:     "local",
		LogLevel:        "info",
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    20 * time.Second,
		ShutdownTimeout: 10 * time.Second,
		MigrationsDir:   "migrations",
		MetricsEnabled:  true,
		PprofEnabled:    false,
		Tracing:         TraceOptions{SampleRatio: 0.1},
	}
}

// Apply применяет список опций к Options.
func Apply[T any, F ~func(*T)](o T, opts []F) T {
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	return o
}

// SetAppName записывает имя сервиса в переданное поле конфигурации.
func SetAppName(field *string, name string) { *field = name }
