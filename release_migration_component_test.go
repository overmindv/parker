//go:build component

package parker

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReleaseMigrationFromV010(t *testing.T) {

	ctx := context.Background()
	pool, err := OpenPool(ctx, testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// Отдельная схема сохраняет остальные component fixtures.
	if _, err = pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS release_compat`); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	cfg.ConnConfig.RuntimeParams["search_path"] = "release_compat"
	isolated, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer isolated.Close()
	defer func() { _, _ = pool.Exec(ctx, `DROP SCHEMA release_compat CASCADE`) }()
	if _, err = isolated.Exec(ctx, `CREATE TABLE outbox (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),topic TEXT NOT NULL,message_key TEXT NOT NULL,message_value BYTEA NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending',attempts INT NOT NULL DEFAULT 0,next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),sent_at TIMESTAMPTZ);
 INSERT INTO outbox(topic,message_key,message_value) VALUES('t','k','original');`); err != nil {
		t.Fatal(err)
	}
	for _, migration := range []string{OutboxLeaseMigration, OutboxRetryMigration} {
		if _, err = isolated.Exec(ctx, migration); err != nil {
			t.Fatal(err)
		}
	}
	outbox := NewPgOutbox(isolated)
	rows, err := outbox.FetchPending(ctx, 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("claim after migration: %v %v", rows, err)
	}
	// Rollback после остановки новых workers возвращает claim в старый pending-формат.
	if _, err = isolated.Exec(ctx, `UPDATE outbox SET status='pending',claim_token=NULL,lease_until=NULL WHERE status='publishing'`); err != nil {
		t.Fatal(err)
	}
	if err = outbox.MarkClaimSent(ctx, rows[0]); err == nil {
		t.Fatal("pre-rollback token accepted")
	}
	var value []byte
	if err = isolated.QueryRow(ctx, `SELECT message_value FROM outbox WHERE status='pending'`).Scan(&value); err != nil || string(value) != "original" {
		t.Fatalf("legacy replay %s %v", value, err)
	}
}
