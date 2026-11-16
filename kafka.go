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
type subscriberClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	Ping(context.Context) error
	Close()
}

type Subscriber struct {
	client  subscriberClient
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
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("создать kafka consumer: %w", err)
	}
	return &Subscriber{client: client, log: log, handler: handler}, nil
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
		err := s.handleRecord(recordCtx, rec)
		if err == nil {
			err = s.client.CommitRecords(recordCtx, rec)
		}
		cancel()
		if err != nil {
			return fmt.Errorf("kafka record failed topic=%s partition=%d offset=%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
		}
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
