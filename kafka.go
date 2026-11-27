package parker

import (
	"context"
	"log/slog"

	"github.com/overmindv/parker/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer — тонкая обёртка над franz-go клиентом для публикации записей.
// Реализует HealthCheck (Ping) для /ready.
type Producer kafka.Producer

// NewProducer создаёт producer к заданным брокерам.
func NewProducer(brokers []string, security ...KafkaSecurity) (*Producer, error) {
	producer, err := kafka.NewProducer(brokers, internalSecurity(security)...)
	if err != nil {
		return nil, err
	}

	return (*Producer)(producer), nil
}

// Publish публикует запись в топик с ключом (обеспечивает порядок в partition).
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	return (*kafka.Producer)(p).Publish(ctx, topic, key, value)
}

// Ping проверяет доступность кластера.
func (p *Producer) Ping(ctx context.Context) error {
	return (*kafka.Producer)(p).Ping(ctx)
}

// Close закрывает клиент.
func (p *Producer) Close() {
	(*kafka.Producer)(p).Close()
}

// Handler — обработчик записи из подписки. Ключ задаёт семантику (тип события).
type Handler func(ctx context.Context, key string, value []byte) error

// RecordPosition содержит координаты записи без ключа, тела и заголовков.
type RecordPosition kafka.RecordPosition

// PositionFromContext возвращает координаты записи внутри вызова Handler.
func PositionFromContext(ctx context.Context) (RecordPosition, bool) {
	position, ok := kafka.PositionFromContext(ctx)

	return RecordPosition(position), ok
}

// Subscriber — consumer-group подписчик: читает топики, вызывает Handler.
// Коммит offset'а выполняется после успешной обработки (at-least-once).
type Subscriber kafka.Subscriber

// NewSubscriber создаёт consumer-group подписчика.
func NewSubscriber(brokers []string, group string, topics []string, handler Handler, log *slog.Logger, security ...KafkaSecurity) (*Subscriber, error) {
	subscriber, err := kafka.NewSubscriber(brokers, group, topics, kafka.Handler(handler), log, internalSecurity(security)...)
	if err != nil {
		return nil, err
	}

	return (*Subscriber)(subscriber), nil
}

// KafkaClientOptions возвращает базовые kgo-опции (TLS/SASL) для собственных
// Kafka-клиентов сервиса.
func KafkaClientOptions(security ...KafkaSecurity) ([]kgo.Opt, error) {
	return kafka.ClientOptions(internalSecurity(security)...)
}

// Run — цикл потребления; Runnable для Runner'а. Завершается по отмене ctx.
func (s *Subscriber) Run(ctx context.Context) error {
	return (*kafka.Subscriber)(s).Run(ctx)
}

// Ping проверяет доступность кластера.
func (s *Subscriber) Ping(ctx context.Context) error {
	return (*kafka.Subscriber)(s).Ping(ctx)
}

// Close закрывает клиент.
func (s *Subscriber) Close() {
	(*kafka.Subscriber)(s).Close()
}

// NewProducer создаёт producer из broker-ов Options приложения и регистрирует его очистку.
func (a *App) NewProducer() (*Producer, error) {
	p, err := kafka.NewProducerWithRuntime(a.tracing, a.opts.KafkaBrokers, internalSecurity([]KafkaSecurity{a.kafkaSecurity()})...)
	if err != nil {
		return nil, err
	}
	producer := (*Producer)(p)
	a.trackCloser(producer.Close)

	return producer, nil
}

// NewSubscriber создаёт consumer-group подписчика из broker-ов Options и регистрирует очистку.
func (a *App) NewSubscriber(group string, topics []string, handler Handler) (*Subscriber, error) {
	s, err := kafka.NewSubscriberWithRuntime(
		a.tracing, a.opts.KafkaBrokers, group, topics, kafka.Handler(handler), a.log,
		internalSecurity([]KafkaSecurity{a.kafkaSecurity()})...,
	)
	if err != nil {
		return nil, err
	}
	subscriber := (*Subscriber)(s)
	a.trackCloser(subscriber.Close)

	return subscriber, nil
}

func (a *App) kafkaSecurity() KafkaSecurity {
	security := a.opts.KafkaSecurity
	security.Environment = a.opts.Environment

	return security
}

func internalSecurity(settings []KafkaSecurity) []kafka.Security {
	converted := make([]kafka.Security, 0, len(settings))
	for _, setting := range settings {
		converted = append(converted, kafka.Security(setting))
	}

	return converted
}
