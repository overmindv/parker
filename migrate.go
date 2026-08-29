package parker

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/pressly/goose/v3"

	// Регистрирует драйвер "pgx" для database/sql (нужен goose).
	_ "github.com/jackc/pgx/v5/stdlib"
)

// MigrateCommand — действия команды migrate.
var migrateCommands = map[string]func(db *sql.DB, dir string) error{
	"up": func(db *sql.DB, dir string) error {
		if err := goose.Up(db, dir); err != nil {
			return fmt.Errorf("up: %w", err)
		}
		return nil
	},
	"down": func(db *sql.DB, dir string) error {
		if err := goose.Down(db, dir); err != nil {
			return fmt.Errorf("down: %w", err)
		}
		return nil
	},
	"status": func(db *sql.DB, dir string) error {
		return goose.Status(db, dir)
	},
}

// RunMigrate применяет SQL-миграции goose из каталога dir к DSN.
// command: up | down | status.
// Перед вызовом должен существовать каталог с миграциями (обычно "migrations").
func RunMigrate(dir, dsn, command string) error {
	if dir == "" {
		return errors.New("не задан каталог миграций")
	}
	if dsn == "" {
		return errors.New("не задан DSN")
	}
	fn, ok := migrateCommands[command]
	if !ok {
		return fmt.Errorf("недопустимая команда миграций %q: ожидается up, down или status", command)
	}
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("каталог миграций: %w", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("открыть соединение: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping базы данных: %w", err)
	}

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("goose dialect: %w", err)
	}
	return fn(db, dir)
}
