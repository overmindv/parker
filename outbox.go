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
	ID         string
	Topic      string
	Key        string
	Value      []byte
	ClaimToken string
	LeaseUntil time.Time
	Attempts   int
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
    sent_at         TIMESTAMPTZ,
 claim_token UUID,
 lease_until TIMESTAMPTZ,
 replay_generation INT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_status_idx ON outbox(status, next_attempt_at);
` + OutboxRetryMigration

// OutboxLeaseMigration — append-only миграция владельца перед обновлением worker.
const OutboxLeaseMigration = `ALTER TABLE outbox ADD COLUMN IF NOT EXISTS claim_token UUID;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS outbox_lease_idx ON outbox(status,lease_until);`

// claimedOutbox добавляет CAS, не меняя существующие реализации Outbox сервисов.
type claimedOutbox interface {
	MarkClaimSent(context.Context, OutboxRecord) error
}

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
	metrics  *Metrics
}

// NewOutboxDispatcher создаёт диспетчер.
func NewOutboxDispatcher(outbox Outbox, producer *Producer, poll time.Duration, log *slog.Logger, metrics ...*Metrics) *OutboxDispatcher {
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	d := newOutboxDispatcher(outbox, producer, poll, 100, log)
	if len(metrics) > 0 {
		d.metrics = metrics[0]
	}
	return d
}

// newOutboxDispatcher — внутренний конструктор с инъекцией publish-интерфейса для тестов.
func newOutboxDispatcher(outbox Outbox, producer publisher, poll time.Duration, batch int, log *slog.Logger) *OutboxDispatcher {
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	if batch <= 0 || batch > 100 {
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
	if store, ok := d.outbox.(interface {
		QueueAge(context.Context) (time.Duration, error)
		CleanupSent(context.Context, time.Duration, int) (int64, error)
	}); ok {
		if d.metrics != nil {
			age, err := store.QueueAge(ctx)
			if err == nil {
				d.metrics.outboxAge.Set(age.Seconds())
			}
		}
		if _, err := store.CleanupSent(ctx, 7*24*time.Hour, 100); err != nil {
			d.log.Warn("outbox retention failed")
		}
	}
	records, err := d.outbox.FetchPending(ctx, d.batch)
	if err != nil {
		d.log.Warn("outbox: не удалось получить записи", "error", err)
		return
	}
	if len(records) == 0 {
		return
	}

	sentIDs := make([]string, 0, len(records))
	for index, rec := range records {
		publishCtx := ctx
		cancel := func() {}
		if !rec.LeaseUntil.IsZero() {
			// Делим остаток lease между оставшимися записями: один сбой не съедает batch.
			budget := time.Until(rec.LeaseUntil) / time.Duration(len(records)-index+1)
			if budget > 5*time.Second {
				budget = 5 * time.Second
			}
			publishCtx, cancel = context.WithTimeout(ctx, budget)
		}
		err := publishCtx.Err()
		if err == nil {
			err = d.producer.Publish(publishCtx, rec.Topic, rec.Key, rec.Value)
		}
		cancel()
		if err != nil {
			d.log.Error("outbox: не удалось опубликовать",
				"id", rec.ID, "topic", rec.Topic)
			if retry, ok := d.outbox.(interface {
				MarkClaimFailed(context.Context, OutboxRecord) error
			}); ok {
				if err := retry.MarkClaimFailed(ctx, rec); err != nil {
					d.log.Warn("outbox reschedule failed", "id", rec.ID)
				}
			}
			continue
		}
		if leased, ok := d.outbox.(claimedOutbox); ok {
			if err := leased.MarkClaimSent(ctx, rec); err != nil {
				d.log.Warn("outbox claim acknowledgement failed", "id", rec.ID)
			}
		} else {
			sentIDs = append(sentIDs, rec.ID)
		}
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
	now  func() time.Time
}

// NewPgOutbox создаёт postgres-outbox.
func NewPgOutbox(pool *pgxpool.Pool) *PgOutbox {
	return &PgOutbox{pool: pool, now: time.Now}
}

// FetchPending закрепляет строки одним коротким statement; publish не держит транзакцию.
func (o *PgOutbox) FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error) {
	if limit <= 0 {
		return nil, nil
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := o.pool.Query(ctx, `WITH ready AS (
 SELECT id FROM outbox WHERE (status='pending' AND next_attempt_at<=now())
 OR (status='publishing' AND lease_until<=now()) ORDER BY created_at,id LIMIT $1 FOR UPDATE SKIP LOCKED
 ) UPDATE outbox o SET status='publishing',claim_token=gen_random_uuid(),lease_until=now()+interval '30 seconds'
 FROM ready WHERE o.id=ready.id RETURNING o.id,o.topic,o.message_key,o.message_value,o.claim_token,o.lease_until,o.attempts`, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox claim: %w", err)
	}
	defer rows.Close()
	var records []OutboxRecord
	for rows.Next() {
		var rec OutboxRecord
		if err := rows.Scan(&rec.ID, &rec.Topic, &rec.Key, &rec.Value, &rec.ClaimToken, &rec.LeaseUntil, &rec.Attempts); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

// MarkClaimSent не позволяет старому worker подтвердить новую lease.
func (o *PgOutbox) MarkClaimSent(ctx context.Context, rec OutboxRecord) error {
	tag, err := o.pool.Exec(ctx, `UPDATE outbox SET status='sent',sent_at=now(),claim_token=NULL,lease_until=NULL,attempts=attempts+1
 WHERE id=$1 AND claim_token=$2 AND status='publishing' AND lease_until>now()`, rec.ID, rec.ClaimToken)
	if err != nil {
		return fmt.Errorf("outbox mark claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox claim lost")
	}
	return nil
}

// MarkSent помечает записи отправленными.
func (o *PgOutbox) MarkSent(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	tag, err := o.pool.Exec(ctx, `
		UPDATE outbox
		SET status = 'sent', sent_at = now(), attempts = attempts + 1
		WHERE id = ANY($1) AND status='pending' AND claim_token IS NULL`, ids)
	if err != nil {
		return fmt.Errorf("outbox mark sent: %w", err)
	}
	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("outbox requires claim acknowledgement")
	}
	return nil
}
