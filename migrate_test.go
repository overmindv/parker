//go:build component

package parker

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PARKER_TEST_DSN")
	if dsn == "" {
		t.Skip("PARKER_TEST_DSN не задан; пропускаю component-тест")
	}
	return dsn
}

func writeMigration(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func columnExists(t *testing.T, dsn, table, column string) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	err = db.QueryRow(
		`SELECT count(*) FROM information_schema.columns WHERE table_name=$1 AND column_name=$2`,
		table, column,
	).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n > 0
}

func TestRunMigrate_UpStatus(t *testing.T) {
	dsn := testDSN(t)
	dir := t.TempDir()
	writeMigration(t, dir, "0001_init.sql",
		"CREATE TABLE parker_migrate_a (id int PRIMARY KEY);")
	writeMigration(t, dir, "0002_second.sql",
		"ALTER TABLE parker_migrate_a ADD COLUMN name text;")

	if err := RunMigrate(dir, dsn, "up"); err != nil {
		t.Fatalf("up: %v", err)
	}
	if !columnExists(t, dsn, "parker_migrate_a", "name") {
		t.Fatal("после up должна существовать колонка name (вторая миграция применена)")
	}

	// status не должен падать на применённых миграциях.
	if err := RunMigrate(dir, dsn, "status"); err != nil {
		t.Fatalf("status: %v", err)
	}

	// down откатывает одну последнюю миграцию.
	if err := RunMigrate(dir, dsn, "down"); err != nil {
		t.Fatalf("down: %v", err)
	}
	if columnExists(t, dsn, "parker_migrate_a", "name") {
		t.Fatal("после down колонка name не должна существовать")
	}

	db, _ := sql.Open("pgx", dsn)
	defer db.Close()
	if _, err := db.Exec("DROP TABLE IF EXISTS parker_migrate_a"); err != nil {
		t.Fatal(err)
	}
}

func TestRunMigrate_Errors(t *testing.T) {
	dsn := testDSN(t)
	if err := RunMigrate(t.TempDir(), "", "up"); err == nil {
		t.Fatal("ожидалась ошибка при пустом DSN")
	}
	if err := RunMigrate(t.TempDir(), dsn, "bogus"); err == nil {
		t.Fatal("ожидалась ошибка при недопустимой команде")
	}
	if err := RunMigrate(t.TempDir()+"/missing", dsn, "up"); err == nil {
		t.Fatal("ожидалась ошибка при отсутствующем каталоге миграций")
	}
}
