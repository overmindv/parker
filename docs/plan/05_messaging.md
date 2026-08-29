# 05 — Фаза 4: Kafka и outbox (producer, consumer-group, outbox-dispatcher)

## Что делаем
Добавляем в parker мессенджинг на `github.com/twmb/franz-go` (та же библиотека, что в `tasks`):
конфигурируемый producer, consumer-group-подписчик с диспетчеризацией по ключу/типу, и
**outbox-dispatcher** — надёжную публикацию «одной транзакцией» с БД. Это убирает из сервисов
весь `kafkaadapter` из `tasks/internal/transport/kafka/` (идентичный у всех).

## Открытые библиотеки
- `github.com/twmb/franz-go` + `pkg/kmsg`, `pkg/kgo`.
- Для outbox потребуется таблица в БД сервиса — схему таблицы parker создаёт через свою миграцию
  (см. «таблица outbox» ниже).

## Целевой контракт

```go
// parker/kafka.go
type Kafka struct { brokers []string; log *slog.Logger }

// Producer — тонкая обёртка над kgo.Client с подтверждением записи.
// Маппинг с интерфейсом `Pinger` для /ready.
type Producer struct { /* *kgo.Client */ }
func (p *Producer) Ping(ctx) error
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error
func (p *Producer) Close()

// Subscriber — consumer-group подписчик: читает топик, вызывает хендлер по ключу/типу.
type Subscriber struct { /* ... */ }
type Handler func(ctx context.Context, key string, value []byte) error
func (s *Subscriber) Subscribe(topic string, h Handler)   // диспетчеризация по ключу (порядок в partition)
func (s *Subscriber) Run(ctx context.Context) error         // Runnable; commit offset после успешного хендлера
func (s *Subscriber) Ping(ctx) error

// App:
func (a *App) Kafka() *Kafka                                        // только если WithKafka
func (a *App) NewProducer(opts ...ProducerOption) (*Producer, error)
func (a *App) NewSubscriber(group, topics []string, opts ...) (*Subscriber, error) // регистрирует Runnable
```

Статусы из `KAFKA_BOOTSTRAP_SERVERS` (env, через `WithKafka`/env `KAFKA_BOOTSTRAP_SERVERS`).
Health: producer/subscriber по желанию добавляются в `/ready` (`AddHealthCheck("kafka", p)`).

## Outbox-dispatcher

Паттерн из `tasks`: сервис в одной БД-транзакции пишет бизнес-строку + запись в таблицу
`outbox`; фоновый воркер периодически вычитывает неотправленные и публикует в Kafka (key = бизнес-id),
затем помечает отправленными. Это даёт атомарность «БД + событие».

parker предоставляет:
```go
// parker/outbox.go
type Outbox interface {
    // FetchPending возвращает записи, готовые к отправке (limit, старше/с retry).
    FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error)
    // MarkSent помечает отправленными (батчем по id).
    MarkSent(ctx context.Context, ids []string) error
}

type OutboxRecord struct{ ID string; Topic string; Key string; Value []byte }

type OutboxDispatcher struct { /* ... */ }
func NewOutboxDispatcher(driver Outbox, producer *Producer, poll time.Duration, log *slog.Logger) *OutboxDispatcher
func (d *OutboxDispatcher) Run(ctx context.Context) error // Runnable: poll → publish → markSent; бэкофф при ошибке

// App:
func (a *App) AddOutbox(driver Outbox, producer *Producer, poll time.Duration) error // регистрирует Runnable "outbox"
```

**Таблица outbox** создаётся миграцией, которую parker кладёт в сгенерированный сервис:
```sql
CREATE TABLE outbox (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic             TEXT NOT NULL,
    message_key       TEXT NOT NULL,
    message_value     BYTEA NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',  -- pending|sent
    attempts          INT  NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at           TIMESTAMPTZ
);
CREATE INDEX outbox_status_idx ON outbox(status, next_attempt_at);
```
- Дефолтный драйвер (схема выше) parker реализует через `pgx` — `postgres.Outbox(pool)`;
  сервис может подсунуть свою реализацию `Outbox` (например, вместе со своей бизнес-транзакцией).
- **Важно**: бизнес-сервис пишет в `outbox` **в той же pgx-транзакции**, что и свои данные.
  Parker для этого даёт `postgres.WithTx(ctx, pool, func(tx pgx.Tx) error)` или экспортирует SQL
  для вставки — конкретный механизм доработать при интеграции с `adapter/postgres` сервиса.

## Файл за файлом: задачи

1. `kafka.go` — `Producer`, `Subscriber` (franz-go), `Kafka`/`App.NewProducer`/`NewSubscriber`, `Ping`s.
2. `outbox.go` — `Outbox` interface, `OutboxDispatcher`, дефолтный `postgres.Outbox`.
3. `cmd/parker`/`example` — в `example/` завести outbox-таблицу и продемонстрировать «записать в БД + событие».
4. Маппинг с HealthRegistry (`AddHealthCheck("kafka", producer/subscriber)` по желанию).

## Безопасность / консистентность
- Привязка к конкретным topic/Kafka-топики — на стороне сервиса (parker не создаёт топики сам);
  создание топика — Часть 1 workflow (`09_testing`/`08_service_bootstrap`), в локалке — через `kafka-topics` сервис в `infra`.
- Мёртвые/повторяющиеся сообщения: сервис решает идемпотентность (см. принципы `tasks`).
- `next_attempt_at`/`attempts` — защита от «вечного» ретрая в цикле при битой kafka.

## Приёмка / DOD
- Producer публикует, Subscriber читает в топик-группу (component против локальной kafka).
- Outbox: бизнес-запись + событие одной транзакцией; при выключенной kafka — пауза с бэкоффом,
  при появлении kafka — доставка; дублей нет благодаря `status`/ключу.
- `example/` проходит сквозной сценарий «query → outbox → kafka → consumer».

## Тесты
- `kafka_test.go`: producer→subscriber roundtrip, порядок по ключу, commit после успеха, retry при ошибке хендлера.
- `outbox_test.go`: публикация, markSent батчем, retry/бэкофф, идемпотентность.
- Component (см. `09_testing.md`) — c postgres:17 + kafka из compose.