package parker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxRecord — запись, ожидающая публикации в Kafka.
type OutboxRecord struct {
	ID    string
	Topic string
	Key   string
	Value []byte
}

// Outbox — хранилище исходящих событий. Реализуется поверх БД сервиса,
// чтобы публиковать событие атомарно с бизнес-данными (в той же транзакции).
type Outbox interface {
	// FetchPending возвращает до limit готовых к отправке записей.
	FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error)
	// MarkSent помечает записи отправленными.
	MarkSent(ctx context.Context, ids []string) error
}

// OutboxSchema — DDL таблицы outbox по умолчанию. Сервис накатывает её своей миграцией.
const OutboxSchema = `
CREATE TABLE IF NOT EXISTS outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic           TEXT NOT NULL,
    message_key     TEXT NOT NULL,
    message_value   BYTEA NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INT  NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_status_idx ON outbox(status, next_attempt_at);
`

// publisher — публикация записи в Kafka (реализуется *Producer). Выделен
// интерфейсом, чтобы диспетчер было легко юнит-тестировать без реального кластера.
type publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// OutboxDispatcher — фоновый воркер: периодически вычитывает pending-записи,
// публикует их в Kafka и помечает отправленными. При ошибке публикации запись
// остаётся pending (передоставится на следующем тике — бэкофф по next_attempt_at).
type OutboxDispatcher struct {
	outbox   Outbox
	producer publisher
	poll     time.Duration
	batch    int
	log      *slog.Logger
}

// NewOutboxDispatcher создаёт диспетчер.
func NewOutboxDispatcher(outbox Outbox, producer *Producer, poll time.Duration, log *slog.Logger) *OutboxDispatcher {
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	return newOutboxDispatcher(outbox, producer, poll, 100, log)
}

// newOutboxDispatcher — внутренний конструктор с инъекцией publish-интерфейса для тестов.
func newOutboxDispatcher(outbox Outbox, producer publisher, poll time.Duration, batch int, log *slog.Logger) *OutboxDispatcher {
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	if batch <= 0 {
		batch = 100
	}
	return &OutboxDispatcher{
		outbox:   outbox,
		producer: producer,
		poll:     poll,
		batch:    batch,
		log:      log,
	}
}

// Run — цикл диспетчера; Runnable для Runner'а.
func (d *OutboxDispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			d.flushOnce(ctx)
		}
	}
}

func (d *OutboxDispatcher) flushOnce(ctx context.Context) {
	records, err := d.outbox.FetchPending(ctx, d.batch)
	if err != nil {
		d.log.Warn("outbox: не удалось получить записи", "error", err)
		return
	}
	if len(records) == 0 {
		return
	}

	sentIDs := make([]string, 0, len(records))
	for _, rec := range records {
		if err := d.producer.Publish(ctx, rec.Topic, rec.Key, rec.Value); err != nil {
			d.log.Error("outbox: не удалось опубликовать",
				"id", rec.ID, "topic", rec.Topic, "error", err)
			continue
		}
		sentIDs = append(sentIDs, rec.ID)
	}
	if len(sentIDs) > 0 {
		if err := d.outbox.MarkSent(ctx, sentIDs); err != nil {
			d.log.Error("outbox: не удалось пометить отправленными", "error", err)
		}
	}
}

// PgOutbox — реализация Outbox поверх pgxpool (таблица outbox, см. OutboxSchema).
type PgOutbox struct {
	pool *pgxpool.Pool
}

// NewPgOutbox создаёт postgres-outbox.
func NewPgOutbox(pool *pgxpool.Pool) *PgOutbox {
	return &PgOutbox{pool: pool}
}

// FetchPending вычитывает готовые записи с блокировкой строк (SKIP LOCKED).
func (o *PgOutbox) FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error) {
	rows, err := o.pool.Query(ctx, `
		SELECT id, topic, message_key, message_value
		FROM outbox
		WHERE status = 'pending' AND next_attempt_at <= now()
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox fetch: %w", err)
	}
	defer rows.Close()

	var records []OutboxRecord
	for rows.Next() {
		var rec OutboxRecord
		if err := rows.Scan(&rec.ID, &rec.Topic, &rec.Key, &rec.Value); err != nil {
			return nil, fmt.Errorf("outbox scan: %w", err)
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

// MarkSent помечает записи отправленными.
func (o *PgOutbox) MarkSent(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := o.pool.Exec(ctx, `
		UPDATE outbox
		SET status = 'sent', sent_at = now(), attempts = attempts + 1
		WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("outbox mark sent: %w", err)
	}
	return nil
}
