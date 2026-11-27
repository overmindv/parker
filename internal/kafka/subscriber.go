package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/overmindv/parker/internal/observability"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

// Handler — обработчик записи из подписки. Ключ задаёт семантику (тип события).
type Handler func(ctx context.Context, key string, value []byte) error

// RecordPosition содержит координаты записи без ключа, тела и заголовков.
type RecordPosition struct {
	Topic     string
	Partition int32
	Offset    int64
}

type positionKey struct{}

// PositionFromContext возвращает координаты записи внутри вызова Handler.
func PositionFromContext(ctx context.Context) (RecordPosition, bool) {
	position, ok := ctx.Value(positionKey{}).(RecordPosition)

	return position, ok
}

// Subscriber — consumer-group подписчик: читает топики, вызывает Handler.
// Коммит offset'а выполняется после успешной обработки (at-least-once).
type subscriberClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	Ping(context.Context) error
	Close()
}

// Subscriber — consumer-group подписчик.
type Subscriber struct {
	client  subscriberClient
	log     *slog.Logger
	handler Handler
	tracing *observability.Runtime
}

// NewSubscriber создаёт consumer-group подписчика.
func NewSubscriber(brokers []string, group string, topics []string, handler Handler, log *slog.Logger, security ...Security) (*Subscriber, error) {
	return NewSubscriberWithRuntime(nil, brokers, group, topics, handler, log, security...)
}

// NewSubscriberWithRuntime создаёт подписчика с провайдером трейсинга.
func NewSubscriberWithRuntime(runtime *observability.Runtime, brokers []string, group string, topics []string, handler Handler, log *slog.Logger, security ...Security) (*Subscriber, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("не заданы kafka-брокеры")
	}
	if handler == nil {
		return nil, fmt.Errorf("не задан handler подписчика")
	}
	opts, err := SecurityOptions(security...)
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("создать kafka consumer: %w", err)
	}

	return &Subscriber{client: client, log: log, handler: handler, tracing: runtime}, nil
}

// ClientOptions возвращает базовые kgo-опции (TLS/SASL) для собственных Kafka-клиентов.
func ClientOptions(security ...Security) ([]kgo.Opt, error) {
	return SecurityOptions(security...)
}

// Run — цикл потребления; Runnable для Runner'а. Завершается по отмене ctx.
func (s *Subscriber) Run(ctx context.Context) error {
	for {
		fetches := s.client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			s.client.AllowRebalance()

			return nil
		}
		if len(fetches.Errors()) > 0 {
			s.client.AllowRebalance()
			// Нельзя пропускать записи из частично успешного fetch: остановка даёт redelivery.
			return fmt.Errorf("kafka poll failed")
		}
		err := s.processFetch(ctx, fetches)
		s.client.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (s *Subscriber) processFetch(ctx context.Context, fetches kgo.Fetches) error {
	iter := fetches.RecordIter()
	for !iter.Done() {
		rec := iter.Next()
		// Ограниченный бюджет меньше rebalance timeout; handler обязан соблюдать ctx.
		recordCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := s.processRecord(recordCtx, rec)
		cancel()
		if err != nil {
			return fmt.Errorf("kafka record failed topic=%s partition=%d offset=%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
		}
	}

	return nil
}

func (s *Subscriber) processRecord(ctx context.Context, rec *kgo.Record) error {
	ctx = observability.ExtractKafkaTrace(ctx, rec)
	var span trace.Span
	if s.tracing != nil {
		ctx, span = s.tracing.KafkaSpan(ctx, "process", trace.SpanKindConsumer)
		defer span.End()
	}
	handlerCtx := context.WithValue(ctx, positionKey{}, RecordPosition{
		Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset,
	})
	if err := s.handleRecord(handlerCtx, rec); err != nil {
		observability.MarkKafkaError(span, "handler_failed")

		return err
	}
	if err := s.client.CommitRecords(ctx, rec); err != nil {
		observability.MarkKafkaError(span, "commit_failed")

		return err
	}

	return nil
}

func (s *Subscriber) handleRecord(ctx context.Context, rec *kgo.Record) error {
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.handler(ctx, string(rec.Key), rec.Value); err == nil {
			return nil
		}
		// Не записываем payload, key и произвольный текст ошибки handler в логи.
		if attempt == 2 {
			return fmt.Errorf("handler retry budget exhausted")
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()

			return ctx.Err()
		case <-timer.C:
		}
	}

	return nil
}

// Ping проверяет доступность кластера.
func (s *Subscriber) Ping(ctx context.Context) error { return s.client.Ping(ctx) }

// Close закрывает клиент.
func (s *Subscriber) Close() { s.client.Close() }
