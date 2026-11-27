package observability

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// TraceIDFrom возвращает trace_id из контекста (пусто, если не задан).
func TraceIDFrom(ctx context.Context) string {
	if id := trace.SpanContextFromContext(ctx).TraceID(); id.IsValid() {
		return id.String()
	}

	return ""
}

// SpanIDFrom возвращает span_id из контекста (пусто, если не задан).
func SpanIDFrom(ctx context.Context) string {
	if id := trace.SpanContextFromContext(ctx).SpanID(); id.IsValid() {
		return id.String()
	}

	return ""
}
