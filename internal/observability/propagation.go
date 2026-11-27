package observability

import (
	"context"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
)

// TraceparentHeader — W3C заголовок трассировки (traceparent).
const TraceparentHeader = "traceparent"

type traceHeader struct{ value string }

func (h *traceHeader) Get(key string) string {
	if key == TraceparentHeader {
		return h.value
	}

	return ""
}

func (h *traceHeader) Set(key, value string) {
	if key == TraceparentHeader {
		h.value = value
	}
}

func (*traceHeader) Keys() []string { return []string{TraceparentHeader} }

func ExtractTraceparent(ctx context.Context, parent string) context.Context {
	if len(parent) != 55 || !strings.HasPrefix(parent, "00-") {
		return ctx
	}

	return propagation.TraceContext{}.Extract(ctx, &traceHeader{value: parent})
}

// TraceparentFrom возвращает W3C traceparent без tracestate и baggage.
func TraceparentFrom(ctx context.Context) string {
	var header traceHeader
	propagation.TraceContext{}.Inject(ctx, &header)

	return header.value
}

// ExtractKafkaTrace читает ровно один traceparent. Другие заголовки не влияют на контекст.
func ExtractKafkaTrace(ctx context.Context, record *kgo.Record) context.Context {
	if record == nil {
		return ctx
	}
	var parent []byte
	for _, header := range record.Headers {
		if strings.EqualFold(header.Key, TraceparentHeader) {
			if parent != nil || len(header.Value) != 55 {
				return ctx
			}
			parent = header.Value
		}
	}
	if parent == nil {
		return ctx
	}

	return ExtractTraceparent(ctx, string(parent))
}

// InjectKafkaTrace заменяет traceparent записи, если в контексте есть W3C trace.
func InjectKafkaTrace(ctx context.Context, record *kgo.Record) {
	if record == nil {
		return
	}
	if parent := TraceparentFrom(ctx); parent != "" {
		record.Headers = slices.DeleteFunc(record.Headers, func(h kgo.RecordHeader) bool {
			return strings.EqualFold(h.Key, TraceparentHeader)
		})
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: TraceparentHeader, Value: []byte(parent)})
	}
}
