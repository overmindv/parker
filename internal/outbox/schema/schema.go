// Package schema содержит DDL стандартной очереди Parker.
package schema

// Schema — DDL таблицы outbox по умолчанию. Сервис накатывает её своей миграцией.
const Schema = `
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
` + RetryMigration

// LeaseMigration — append-only миграция владельца перед обновлением worker.
const LeaseMigration = `ALTER TABLE outbox ADD COLUMN IF NOT EXISTS claim_token UUID;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS outbox_lease_idx ON outbox(status,lease_until);`

// RetryMigration применяется новой append-only миграцией владельца.
const RetryMigration = `ALTER TABLE outbox ADD COLUMN IF NOT EXISTS replay_generation INT NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS outbox_replay_audit (
 outbox_id UUID NOT NULL, generation INT NOT NULL, actor_id UUID NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(outbox_id,generation)
);
CREATE INDEX IF NOT EXISTS outbox_sent_retention_idx ON outbox(sent_at,id) WHERE status='sent';
CREATE INDEX IF NOT EXISTS outbox_unsent_age_idx ON outbox(created_at) WHERE status<>'sent';
`
