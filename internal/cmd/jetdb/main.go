// Команда jetdb подготавливает локальную базу Compose для генератора.
package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/overmindv/parker/internal/outbox/schema"
	"github.com/pressly/goose/v3"
)

// Адрес соответствует compose.jet.yaml; JET_DATABASE_DSN используется только для чтения.
const fixtureDSN = "postgres://parker_jet@127.0.0.1:55436/parker_jet?sslmode=disable"

func main() {
	db, err := sql.Open("pgx", fixtureDSN)
	if err != nil {
		log.Fatal("open local Jet database")
	}
	defer func() { _ = db.Close() }()
	if _, err = db.Exec(schema.Schema); err != nil {
		log.Fatalf("apply OutboxSchema: %v", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("docs/migrations"),
		goose.WithTableName("jet_fixture_versions"))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := provider.Up(context.Background()); err != nil {
		log.Fatal(err)
	}
}
