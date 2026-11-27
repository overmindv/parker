// Package observability реализует метрики, трейсинг и OTLP-экспорт parker.
package observability

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	traceExportTimeout = 2 * time.Second
	httpsScheme        = "https"
	TraceExportPath    = "/v1/traces"

	environmentStaging    = "staging"
	environmentProduction = "production"
)

var traceServiceName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ResolveEndpoint проверяет настройки экспорта и возвращает адрес коллектора.
func ResolveEndpoint(endpoint string, sampleRatio float64, environment, service string) (*url.URL, error) {
	if math.IsNaN(sampleRatio) || sampleRatio < 0 || sampleRatio > 1 {
		return nil, errors.New("PARKER_TRACE_SAMPLE_RATIO must be between 0 and 1")
	}
	if endpoint == "" {
		return nil, nil
	}
	if !traceServiceName.MatchString(service) {
		return nil, errors.New("tracing requires a static SERVICE_NAME of up to 64 characters")
	}
	if len(endpoint) > 1024 {
		return nil, errors.New("invalid PARKER_OTLP_TRACE_ENDPOINT")
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != httpsScheme) || u.Hostname() == "" ||
		u.User != nil || u.Opaque != "" || u.ForceQuery || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid PARKER_OTLP_TRACE_ENDPOINT")
	}
	if port := u.Port(); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil || p < 1 || p > 65535 {
			return nil, errors.New("invalid PARKER_OTLP_TRACE_ENDPOINT port")
		}
	}
	if u.Scheme != httpsScheme && (environment == environmentStaging || environment == environmentProduction) {
		return nil, errors.New("OTLP trace export requires HTTPS in staging and production")
	}
	if u.Path == "" {
		u.Path = TraceExportPath
	}

	return u, nil
}

// Runtime — провайдер трейсинга parker: создаёт спаны и выгружает их в OTLP.
type Runtime struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	log      *slog.Logger
}

// NewRuntime собирает провайдер трейсинга; пустой endpoint отключает экспорт.
func NewRuntime(endpoint string, sampleRatio float64, service, environment string, log *slog.Logger) (*Runtime, error) {
	resolved, err := ResolveEndpoint(endpoint, sampleRatio, environment, service)
	if err != nil || resolved == nil {
		return nil, err
	}
	exporter, err := otlptrace.New(context.Background(), NewOTLPClient(resolved))
	if err != nil {
		return nil, errors.New("cannot initialize OTLP trace exporter")
	}

	return newTraceProvider(service, sampleRatio, exporter, log), nil
}

func newTraceProvider(service string, ratio float64, exporter sdktrace.SpanExporter, log *slog.Logger) *Runtime {
	sampler := sdktrace.NeverSample()
	if ratio > 0 {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sampler),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
		sdktrace.WithRawSpanLimits(sdktrace.SpanLimits{AttributeCountLimit: 8, AttributeValueLengthLimit: 128}),
		sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(1024), sdktrace.WithMaxExportBatchSize(128),
			sdktrace.WithBatchTimeout(time.Second), sdktrace.WithExportTimeout(traceExportTimeout)),
	)

	return &Runtime{provider: provider, tracer: provider.Tracer("github.com/overmindv/parker"), log: log}
}

// Tracer возвращает tracer провайдера (нужен интеграционным тестам).
func (t *Runtime) Tracer() trace.Tracer {
	if t == nil {
		return nil
	}

	return t.tracer
}

// Shutdown выгружает оставшиеся спаны с ограниченным дедлайном.
func (t *Runtime) Shutdown(ctx context.Context) {
	if t == nil {
		return
	}
	deadline, cancel := context.WithTimeout(ctx, traceExportTimeout)
	defer cancel()
	if err := t.provider.Shutdown(deadline); err != nil && t.log != nil {
		t.log.Warn("trace export did not finish before shutdown")
	}
}
