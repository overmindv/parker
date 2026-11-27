package kafka

import (
	"context"
	"fmt"

	"github.com/overmindv/parker/internal/observability"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

// Producer — тонкая обёртка над franz-go клиентом для публикации записей.
// Реализует HealthCheck (Ping) для /ready.
type Producer struct {
	client  *kgo.Client
	tracing *observability.Runtime
}

// NewProducer создаёт producer к заданным брокерам.
func NewProducer(brokers []string, security ...Security) (*Producer, error) {
	return NewProducerWithRuntime(nil, brokers, security...)
}

// NewProducerWithRuntime создаёт producer с провайдером трейсинга.
func NewProducerWithRuntime(runtime *observability.Runtime, brokers []string, security ...Security) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("не заданы kafka-брокеры")
	}
	opts, err := SecurityOptions(security...)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.SeedBrokers(brokers...))
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("создать kafka producer: %w", err)
	}

	return &Producer{client: client, tracing: runtime}, nil
}

// Publish публикует запись в топик с ключом (обеспечивает порядок в partition).
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	var span trace.Span
	if p.tracing != nil {
		ctx, span = p.tracing.KafkaSpan(ctx, "send", trace.SpanKindProducer)
		defer span.End()
	}
	observability.InjectKafkaTrace(ctx, rec)
	result := p.client.ProduceSync(ctx, rec)
	if err := result.FirstErr(); err != nil {
		observability.MarkKafkaError(span, "publish_failed")

		return fmt.Errorf("kafka publish %s: %w", topic, err)
	}

	return nil
}

// Ping проверяет доступность кластера.
func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Close закрывает клиент.
func (p *Producer) Close() { p.client.Close() }
