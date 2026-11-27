package parker

import (
	"time"

	"github.com/overmindv/parker/internal/config"
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
	KafkaSecurity KafkaSecurity
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

// Option — функция-модификатор Options для Main(run, opts...).
type Option func(*Options)

// WithAppName задаёт имя сервиса (иначе env SERVICE_NAME или заглушка).
func WithAppName(name string) Option {
	return func(o *Options) { config.SetAppName(&o.ServiceName, name) }
}

// DefaultOptions возвращает Options с дефолтными значениями стандартных полей.
func DefaultOptions() Options { return publicOptions(config.DefaultOptions()) }

// Apply применяет список опций к Options.
func (o Options) Apply(opts []Option) Options { return config.Apply(o, opts) }

func (o Options) settings() config.Options {
	return config.Options{
		ServiceName:     o.ServiceName,
		HTTPAddress:     o.HTTPAddress,
		Environment:     o.Environment,
		LogLevel:        o.LogLevel,
		ReadTimeout:     o.ReadTimeout,
		WriteTimeout:    o.WriteTimeout,
		ShutdownTimeout: o.ShutdownTimeout,
		DatabaseURL:     o.DatabaseURL,
		MigrationsDir:   o.MigrationsDir,
		KafkaSecurity:   kafka.Security(o.KafkaSecurity),
		KafkaBrokers:    o.KafkaBrokers,
		MetricsEnabled:  o.MetricsEnabled,
		PprofEnabled:    o.PprofEnabled,
		Tracing:         config.TraceOptions(o.Tracing),
	}
}

func publicOptions(o config.Options) Options {
	return Options{
		ServiceName:     o.ServiceName,
		HTTPAddress:     o.HTTPAddress,
		Environment:     o.Environment,
		LogLevel:        o.LogLevel,
		ReadTimeout:     o.ReadTimeout,
		WriteTimeout:    o.WriteTimeout,
		ShutdownTimeout: o.ShutdownTimeout,
		DatabaseURL:     o.DatabaseURL,
		MigrationsDir:   o.MigrationsDir,
		KafkaSecurity:   KafkaSecurity(o.KafkaSecurity),
		KafkaBrokers:    o.KafkaBrokers,
		MetricsEnabled:  o.MetricsEnabled,
		PprofEnabled:    o.PprofEnabled,
		Tracing:         TraceOptions(o.Tracing),
	}
}
