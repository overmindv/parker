// Package config загружает и проверяет инфраструктурные настройки сервиса.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/overmindv/parker/internal/env"
	"github.com/overmindv/parker/internal/kafka"
	"github.com/overmindv/parker/internal/observability"
)

const (
	environmentLocal       = "local"
	environmentDevelopment = "development"
	environmentTest        = "test"
	environmentStaging     = "staging"
	environmentProduction  = "production"
	logLevelDebug          = "debug"
	logLevelInfo           = "info"
	logLevelWarn           = "warn"
	logLevelError          = "error"
)

// Load читает стандартные env-переменные parker в Options поверх DefaultOptions()
// и валидирует результат.
func Load() (Options, error) {
	readTimeout, err := env.Duration("READ_TIMEOUT", time.Second*10)
	if err != nil {
		return Options{}, err
	}
	writeTimeout, err := env.Duration("WRITE_TIMEOUT", time.Second*20)
	if err != nil {
		return Options{}, err
	}
	shutdownTimeout, err := env.Duration("SHUTDOWN_TIMEOUT", time.Second*10)
	if err != nil {
		return Options{}, err
	}

	opts := DefaultOptions()
	opts.ServiceName = env.String("SERVICE_NAME", opts.ServiceName)
	opts.HTTPAddress = env.String("HTTP_ADDR", opts.HTTPAddress)
	opts.Environment = env.String("ENV", opts.Environment)
	opts.LogLevel = env.String("LOG_LEVEL", opts.LogLevel)
	opts.ReadTimeout = readTimeout
	opts.WriteTimeout = writeTimeout
	opts.ShutdownTimeout = shutdownTimeout
	opts.DatabaseURL = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	opts.MigrationsDir = env.String("MIGRATIONS_DIR", opts.MigrationsDir)
	opts.KafkaBrokers = env.List("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
	opts.KafkaSecurity, err = kafka.LoadSecurity(opts.Environment)
	if err != nil {
		return Options{}, err
	}
	opts.MetricsEnabled = env.Bool("METRICS_ENABLED", opts.MetricsEnabled)
	opts.PprofEnabled = env.Bool("PPROF_ENABLED", opts.PprofEnabled)
	opts.Tracing.Endpoint = strings.TrimSpace(os.Getenv("PARKER_OTLP_TRACE_ENDPOINT"))
	if raw := strings.TrimSpace(os.Getenv("PARKER_TRACE_SAMPLE_RATIO")); raw != "" {
		opts.Tracing.SampleRatio, err = strconv.ParseFloat(raw, 64)
		if err != nil {
			return Options{}, errors.New("PARKER_TRACE_SAMPLE_RATIO must be between 0 and 1")
		}
	}

	if err := opts.Validate(); err != nil {
		return Options{}, fmt.Errorf("validate options: %w", err)
	}

	return opts, nil
}

// Validate проверяет обязательные и ограниченные настройки.
func (o Options) Validate() error {
	if o.ServiceName == "" {
		return errors.New("SERVICE_NAME не задан")
	}
	if o.HTTPAddress == "" {
		return errors.New("HTTP_ADDR не задан")
	}

	switch o.LogLevel {
	case logLevelDebug, logLevelInfo, logLevelWarn, logLevelError:
	default:
		return fmt.Errorf("недопустимый LOG_LEVEL %q: ожидается debug, info, warn или error", o.LogLevel)
	}

	switch o.Environment {
	case environmentLocal, environmentDevelopment, environmentTest, environmentStaging, environmentProduction:
	default:
		return fmt.Errorf("недопустимый ENV %q", o.Environment)
	}

	if o.ReadTimeout <= 0 {
		return errors.New("READ_TIMEOUT должен быть больше нуля")
	}
	if o.WriteTimeout <= 0 {
		return errors.New("WRITE_TIMEOUT должен быть больше нуля")
	}
	if o.ShutdownTimeout <= 0 {
		return errors.New("SHUTDOWN_TIMEOUT должен быть больше нуля")
	}

	if o.DatabaseURL != "" {
		if err := validateDatabaseURL(o.DatabaseURL); err != nil {
			return fmt.Errorf("DATABASE_URL некорректен: %w", err)
		}
	}

	security := o.KafkaSecurity
	security.Environment = o.Environment
	if _, err := observability.ResolveEndpoint(o.Tracing.Endpoint, o.Tracing.SampleRatio, o.Environment, o.ServiceName); err != nil {
		return err
	}

	return kafka.ValidateConfiguration(security)
}

// validateDatabaseURL проверяет схему, host и имя PostgreSQL database.
func validateDatabaseURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return err
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return fmt.Errorf("неподдерживаемая схема %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return errors.New("не указан host")
	}
	if strings.TrimPrefix(parsed.Path, "/") == "" {
		return errors.New("не указано имя базы данных")
	}

	return nil
}
