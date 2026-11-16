-- Шаблон новой owner migration, после outbox_leases.
-- +goose Up
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS replay_generation INT NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS outbox_replay_audit (
    outbox_id UUID NOT NULL,
    generation INT NOT NULL,
    actor_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(outbox_id,generation)
);

CREATE INDEX IF NOT EXISTS outbox_sent_retention_idx ON outbox(sent_at,id) WHERE status='sent';
CREATE INDEX IF NOT EXISTS outbox_unsent_age_idx ON outbox(created_at) WHERE status<>'sent';

-- +goose Down
-- Сохраняем аудит и совместимое расширение при rollback приложения.
SELECT 1;
