package parker

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/parker/internal/kafka"
	"github.com/overmindv/parker/internal/observability"
	"github.com/overmindv/parker/internal/outbox"
)

// OutboxRecord — запись, ожидающая публикации в Kafka.
type OutboxRecord outbox.Record

// Outbox — хранилище исходящих событий. Реализуется поверх БД сервиса,
// чтобы публиковать событие атомарно с бизнес-данными (в той же транзакции).
type Outbox interface {
	// FetchPending возвращает до limit готовых к отправке записей.
	FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error)
	// MarkSent помечает записи отправленными.
	MarkSent(ctx context.Context, ids []string) error
}

// OutboxSchema — DDL таблицы outbox по умолчанию. Сервис накатывает её своей миграцией.
const OutboxSchema = outbox.Schema

// OutboxLeaseMigration — append-only миграция владельца перед обновлением worker.
const OutboxLeaseMigration = outbox.LeaseMigration

// OutboxRetryMigration применяется новой append-only миграцией владельца.
const OutboxRetryMigration = outbox.RetryMigration

// OutboxDispatcher — фоновый воркер публикации outbox-событий.
type OutboxDispatcher outbox.Dispatcher

// NewOutboxDispatcher создаёт диспетчер.
func NewOutboxDispatcher(store Outbox, producer *Producer, poll time.Duration, log *slog.Logger, metrics ...*Metrics) *OutboxDispatcher {
	var internalMetrics []*observability.Metrics
	if len(metrics) > 0 {
		internalMetrics = []*observability.Metrics{(*observability.Metrics)(metrics[0])}
	}
	dispatcher := outbox.NewDispatcher(adaptOutbox(store), (*kafka.Producer)(producer), poll, log, internalMetrics...)

	return (*OutboxDispatcher)(dispatcher)
}

// Run — цикл диспетчера; Runnable для Runner'а.
func (d *OutboxDispatcher) Run(ctx context.Context) error {
	return (*outbox.Dispatcher)(d).Run(ctx)
}

// PgOutbox — реализация Outbox поверх pgxpool (таблица outbox, см. OutboxSchema).
type PgOutbox outbox.PgOutbox

// NewPgOutbox создаёт postgres-outbox.
func NewPgOutbox(pool *pgxpool.Pool) *PgOutbox {
	return (*PgOutbox)(outbox.NewPgOutbox(pool))
}

// FetchPending закрепляет строки одним коротким statement; publish не держит транзакцию.
func (o *PgOutbox) FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error) {
	records, err := (*outbox.PgOutbox)(o).FetchPending(ctx, limit)
	if err != nil {
		return nil, err
	}
	converted := make([]OutboxRecord, 0, len(records))
	for _, record := range records {
		converted = append(converted, OutboxRecord(record))
	}

	return converted, nil
}

// MarkClaimSent не позволяет старому worker подтвердить новую lease.
func (o *PgOutbox) MarkClaimSent(ctx context.Context, rec OutboxRecord) error {
	return (*outbox.PgOutbox)(o).MarkClaimSent(ctx, outbox.Record(rec))
}

// MarkSent помечает записи отправленными.
func (o *PgOutbox) MarkSent(ctx context.Context, ids []string) error {
	return (*outbox.PgOutbox)(o).MarkSent(ctx, ids)
}

// MarkClaimFailed сохраняет расписание, не сохраняя raw broker errors или payload.
func (o *PgOutbox) MarkClaimFailed(ctx context.Context, rec OutboxRecord) error {
	return (*outbox.PgOutbox)(o).MarkClaimFailed(ctx, outbox.Record(rec))
}

// ReplayExhausted вызывается владельцем после проверки доверенного admin actor.
func (o *PgOutbox) ReplayExhausted(ctx context.Context, id string, generation int, actor string, admin bool) error {
	return (*outbox.PgOutbox)(o).ReplayExhausted(ctx, id, generation, actor, admin)
}

// QueueAge — возраст старейшего неотправленного события, включая exhausted.
func (o *PgOutbox) QueueAge(ctx context.Context) (time.Duration, error) {
	return (*outbox.PgOutbox)(o).QueueAge(ctx)
}

// CleanupSent удаляет только старые sent небольшим batch, не трогая pending/exhausted.
func (o *PgOutbox) CleanupSent(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	return (*outbox.PgOutbox)(o).CleanupSent(ctx, retention, limit)
}

// fullOutbox — возможности PgOutbox, которые диспетчер использует через type assertion.
type fullOutbox interface {
	MarkClaimSent(context.Context, OutboxRecord) error
	MarkClaimFailed(context.Context, OutboxRecord) error
	QueueAge(context.Context) (time.Duration, error)
	CleanupSent(context.Context, time.Duration, int) (int64, error)
}

type outboxAdapter struct{ store Outbox }

func (a outboxAdapter) FetchPending(ctx context.Context, limit int) ([]outbox.Record, error) {
	records, err := a.store.FetchPending(ctx, limit)
	if err != nil {
		return nil, err
	}
	converted := make([]outbox.Record, 0, len(records))
	for _, record := range records {
		converted = append(converted, outbox.Record(record))
	}

	return converted, nil
}

func (a outboxAdapter) MarkSent(ctx context.Context, ids []string) error {
	return a.store.MarkSent(ctx, ids)
}

type fullOutboxAdapter struct{ outboxAdapter }

func (a fullOutboxAdapter) MarkClaimSent(ctx context.Context, rec outbox.Record) error {
	return a.store.(fullOutbox).MarkClaimSent(ctx, OutboxRecord(rec))
}

func (a fullOutboxAdapter) MarkClaimFailed(ctx context.Context, rec outbox.Record) error {
	return a.store.(fullOutbox).MarkClaimFailed(ctx, OutboxRecord(rec))
}

func (a fullOutboxAdapter) QueueAge(ctx context.Context) (time.Duration, error) {
	return a.store.(fullOutbox).QueueAge(ctx)
}

func (a fullOutboxAdapter) CleanupSent(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	return a.store.(fullOutbox).CleanupSent(ctx, retention, limit)
}

// adaptOutbox сохраняет опциональные возможности PgOutbox для диспетчера.
func adaptOutbox(store Outbox) outbox.Outbox {
	base := outboxAdapter{store: store}
	if _, ok := store.(fullOutbox); ok {
		return fullOutboxAdapter{outboxAdapter: base}
	}

	return base
}
