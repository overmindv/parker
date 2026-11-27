package observability

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// KafkaSpan создаёт спан операции с Kafka.
func (t *Runtime) KafkaSpan(ctx context.Context, operation string, kind trace.SpanKind) (context.Context, trace.Span) {
	ctx, span := t.tracer.Start(ctx, "kafka "+operation, trace.WithSpanKind(kind))
	if span.IsRecording() {
		span.SetAttributes(attribute.String("messaging.system.name", "kafka"),
			attribute.String("messaging.operation.type", operation))
	}

	return ctx, span
}

// MarkKafkaError помечает спан ошибкой обработки.
func MarkKafkaError(span trace.Span, reason string) {
	if span != nil && span.IsRecording() {
		span.SetStatus(codes.Error, reason)
	}
}
