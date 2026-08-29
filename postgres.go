package parker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenPool создаёт пул подключений pgxpool и проверяет доступность базы Ping'ом.
func OpenPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("создать pgxpool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// Postgres возвращает (открывая по запросу) пул pgxpool к DATABASE_URL сервиса.
// При первом вызове пул открывается, проверяется Ping'ом и регистрируется
// health-чек "postgres" (агрегируется в /ready). Повторные вызовы возвращают кеш.
func (a *App) Postgres() (*pgxpool.Pool, error) {
	a.pgOnce.Do(func() {
		if strings.TrimSpace(a.opts.DatabaseURL) == "" {
			a.pgErr = errors.New("DATABASE_URL не задан")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pool, err := OpenPool(ctx, a.opts.DatabaseURL)
		if err != nil {
			a.pgErr = err
			return
		}
		a.pgPool = pool
		a.AddHealthCheck("postgres", pool) // pgxpool.Pool реализует Ping(ctx) error
		a.trackCloser(pool.Close)
	})
	return a.pgPool, a.pgErr
}
