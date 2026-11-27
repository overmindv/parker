package parker

import (
	"context"
	"log/slog"
	"net/url"

	"github.com/overmindv/parker/internal/observability"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TraceparentHeader — W3C заголовок трассировки (traceparent).
const TraceparentHeader = observability.TraceparentHeader

// TraceparentFrom возвращает W3C traceparent без tracestate и baggage.
func TraceparentFrom(ctx context.Context) string {
	return observability.TraceparentFrom(ctx)
}

// ExtractKafkaTrace читает ровно один traceparent. Другие заголовки не влияют на контекст.
func ExtractKafkaTrace(ctx context.Context, record *kgo.Record) context.Context {
	return observability.ExtractKafkaTrace(ctx, record)
}

// InjectKafkaTrace заменяет traceparent записи, если в контексте есть W3C trace.
func InjectKafkaTrace(ctx context.Context, record *kgo.Record) {
	observability.InjectKafkaTrace(ctx, record)
}

// TraceIDFrom возвращает trace_id из контекста (пусто, если не задан).
func TraceIDFrom(ctx context.Context) string {
	return observability.TraceIDFrom(ctx)
}

// SpanIDFrom возвращает span_id из контекста (пусто, если не задан).
func SpanIDFrom(ctx context.Context) string {
	return observability.SpanIDFrom(ctx)
}

func (o TraceOptions) collectorEndpoint(environment, service string) (*url.URL, error) {
	return observability.ResolveEndpoint(o.Endpoint, o.SampleRatio, environment, service)
}

func newTraceRuntime(options Options, log *slog.Logger) (*observability.Runtime, error) {
	return observability.NewRuntime(
		options.Tracing.Endpoint,
		options.Tracing.SampleRatio,
		options.ServiceName,
		options.Environment,
		log,
	)
}
