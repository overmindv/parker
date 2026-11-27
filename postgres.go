package parker

import (
	"context"

	"github.com/overmindv/parker/internal/app"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/parker/internal/postgres"
)

// OpenPool создаёт пул подключений pgxpool и проверяет доступность базы Ping'ом.
func OpenPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return postgres.OpenPool(ctx, dsn)
}

// Postgres возвращает (открывая по запросу) пул pgxpool к DATABASE_URL сервиса.
// При первом вызове пул открывается, проверяется Ping'ом и регистрируется
// health-чек "postgres" (агрегируется в /ready). Повторные вызовы возвращают кеш.
func (a *App) Postgres() (*pgxpool.Pool, error) {
	return (*app.App)(a).Postgres()
}
