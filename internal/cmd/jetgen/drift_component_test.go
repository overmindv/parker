//go:build component

package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/parker/internal/outbox/schema"
)

func TestJetCheckDetectsAddedColumn(t *testing.T) {
	dsn := os.Getenv("PARKER_TEST_DSN")
	if dsn == "" {
		t.Skip("PARKER_TEST_DSN required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "CREATE SCHEMA jet_drift; SET LOCAL search_path TO jet_drift;"+schema.Schema); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(ctx, "DROP SCHEMA jet_drift CASCADE") }()

	target := filepath.Join(t.TempDir(), "generated")
	run := func(action string) ([]byte, error) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "sh", "scripts/jet.sh", action, target)
		cmd.Dir = "../../.."
		cmd.Env = append(os.Environ(), "JET_DATABASE_DSN="+dsn, "JET_SCHEMA=jet_drift")

		return cmd.CombinedOutput()
	}
	if output, err := run("generate"); err != nil {
		t.Fatalf("initial generation: %v, %s", err, output)
	}
	if _, err := pool.Exec(ctx, "CREATE TABLE jet_drift.goose_db_version (id bigint)"); err != nil {
		t.Fatal(err)
	}
	if output, err := run("check"); err != nil {
		t.Fatalf("unrelated table changed models: %v, %s", err, output)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE jet_drift.outbox ADD COLUMN drift_marker bigint"); err != nil {
		t.Fatal(err)
	}
	// Повторная проверка обнаруживает тот же дрейф, пока файлы не обновлены.
	for range 2 {
		output, err := run("check")
		if err == nil || !bytes.Contains(output, []byte("DriftMarker")) {
			t.Fatalf("expected added column in diff: %v, %s", err, output)
		}
	}
}
