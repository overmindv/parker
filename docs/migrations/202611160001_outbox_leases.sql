-- Шаблон append-only миграции: скопировать в сервис-владелец outbox.
-- Выполнить до переключения workers на новую версию Parker; не менять старые миграции.
-- +goose Up
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS claim_token UUID;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS outbox_lease_idx ON outbox(status,lease_until);

-- +goose Down
-- Столбцы сохраняются. Перед запуском старых workers остановить новые и
-- вернуть publishing в pending с очисткой claim_token/lease_until (README).
SELECT 1;
