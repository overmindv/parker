package parker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer — тонкая обёртка над franz-go клиентом для публикации записей.
// Реализует HealthCheck (Ping) для /ready.
type Producer struct {
	client *kgo.Client
}

// NewProducer создаёт producer к заданным брокерам.
func NewProducer(brokers []string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("не заданы kafka-брокеры")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("создать kafka producer: %w", err)
	}
	return &Producer{client: client}, nil
}

// Publish публикует запись в топик с ключом (обеспечивает порядок в partition).
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	result := p.client.ProduceSync(ctx, rec)
	if err := result.FirstErr(); err != nil {
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

// Handler — обработчик записи из подписки. Ключ задаёт семантику (тип события).
type Handler func(ctx context.Context, key string, value []byte) error

// Subscriber — consumer-group подписчик: читает топики, вызывает Handler.
// Коммит offset'а выполняется после успешной обработки (at-least-once).
type Subscriber struct {
	client  *kgo.Client
	log     *slog.Logger
	handler Handler
}

// NewSubscriber создаёт consumer-group подписчика.
func NewSubscriber(brokers []string, group string, topics []string, handler Handler, log *slog.Logger) (*Subscriber, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("не заданы kafka-брокеры")
	}
	if handler == nil {
		return nil, fmt.Errorf("не задан handler подписчика")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("создать kafka consumer: %w", err)
	}
	return &Subscriber{client: client, log: log, handler: handler}, nil
}

// Run — цикл потребления; Runnable для Runner'а. Завершается по отмене ctx.
func (s *Subscriber) Run(ctx context.Context) error {
	for {
		fetches := s.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			s.log.Warn("kafka poll errors", "count", len(errs))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			if err := s.handler(ctx, string(rec.Key), rec.Value); err != nil {
				s.log.Error("kafka handler error",
					"topic", rec.Topic, "key", string(rec.Key), "error", err)
				return // offset не коммитим → at-least-once передоставит
			}
			// CommitRecords коммитит offset+1 для partition записи (односторонне,
			// ошибки качаются через PollFetches/обратный вызов).
			_ = s.client.CommitRecords(ctx, rec)
		})
	}
}

// Ping проверяет доступность кластера.
func (s *Subscriber) Ping(ctx context.Context) error { return s.client.Ping(ctx) }

// Close закрывает клиент.
func (s *Subscriber) Close() { s.client.Close() }

// NewProducer создаёт producer из broker-ов Options приложения и регистрирует его очистку.
func (a *App) NewProducer() (*Producer, error) {
	p, err := NewProducer(a.opts.KafkaBrokers)
	if err != nil {
		return nil, err
	}
	a.trackCloser(p.Close)
	return p, nil
}

// NewSubscriber создаёт consumer-group подписчика из broker-ов Options и регистрирует очистку.
func (a *App) NewSubscriber(group string, topics []string, handler Handler) (*Subscriber, error) {
	s, err := NewSubscriber(a.opts.KafkaBrokers, group, topics, handler, a.log)
	if err != nil {
		return nil, err
	}
	a.trackCloser(s.Close)
	return s, nil
}
