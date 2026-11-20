package parker

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
	traceExportPath    = "/v1/traces"
)

var traceServiceName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func (o TraceOptions) collectorEndpoint(environment, service string) (*url.URL, error) {
	if math.IsNaN(o.SampleRatio) || o.SampleRatio < 0 || o.SampleRatio > 1 {
		return nil, errors.New("PARKER_TRACE_SAMPLE_RATIO must be between 0 and 1")
	}
	if o.Endpoint == "" {
		return nil, nil
	}
	if !traceServiceName.MatchString(service) {
		return nil, errors.New("tracing requires a static SERVICE_NAME of up to 64 characters")
	}
	if len(o.Endpoint) > 1024 {
		return nil, errors.New("invalid PARKER_OTLP_TRACE_ENDPOINT")
	}
	u, err := url.Parse(o.Endpoint)
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
		u.Path = traceExportPath
	}

	return u, nil
}

type traceRuntime struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	log      *slog.Logger
}

func newTraceRuntime(options Options, log *slog.Logger) (*traceRuntime, error) {
	endpoint, err := options.Tracing.collectorEndpoint(options.Environment, options.ServiceName)
	if err != nil || endpoint == nil {
		return nil, err
	}
	exporter, err := otlptrace.New(context.Background(), newOTLPClient(endpoint))
	if err != nil {
		return nil, errors.New("cannot initialize OTLP trace exporter")
	}

	return newTraceProvider(options.ServiceName, options.Tracing.SampleRatio, exporter, log), nil
}

func newTraceProvider(service string, ratio float64, exporter sdktrace.SpanExporter, log *slog.Logger) *traceRuntime {
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

	return &traceRuntime{provider: provider, tracer: provider.Tracer("github.com/overmindv/parker"), log: log}
}

func (t *traceRuntime) shutdown(ctx context.Context) {
	deadline, cancel := context.WithTimeout(ctx, traceExportTimeout)
	defer cancel()
	if err := t.provider.Shutdown(deadline); err != nil && t.log != nil {
		t.log.Warn("trace export did not finish before shutdown")
	}
}
