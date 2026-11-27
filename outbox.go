package parker

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/parker/internal/convert"
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
	metric := (*observability.Metrics)(convert.First(metrics))
	dispatcher := outbox.NewDispatcher(outbox.Adapt(store), (*kafka.Producer)(producer), poll, log, metric)

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
	return outbox.FetchPendingAs[OutboxRecord]((*outbox.PgOutbox)(o), ctx, limit)
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
