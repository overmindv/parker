//go:build component

package parker

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestOpenPool_Ping(t *testing.T) {
	dsn := testDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := OpenPool(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestOpenPool_BadDSN(t *testing.T) {
	ctx := context.Background()
	if _, err := OpenPool(ctx, "postgres://user:pass@127.0.0.1:1/none?sslmode=disable"); err == nil {
		t.Fatal("ожидалась ошибка при недоступной БД")
	}
}

func TestApp_PostgresRegistersHealthAndReady(t *testing.T) {
	dsn := testDSN(t)
	app := newTestApp()
	app.opts.DatabaseURL = dsn

	pool, err := app.Postgres()
	if err != nil {
		t.Fatalf("Postgres: %v", err)
	}
	defer pool.Close()

	if !app.health.Enabled() {
		t.Fatal("после Postgres() должен быть зарегистрирован health-чек")
	}
	rec := serveRequest(t, app, http.MethodGet, "/ready")
	if rec.Code != http.StatusOK {
		t.Fatalf("/ready с поднятой БД: ожидался 200, получили %d", rec.Code)
	}
}
