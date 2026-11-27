package parker

import "github.com/overmindv/parker/internal/migrate"

// RunMigrate применяет SQL-миграции goose из каталога dir к DSN.
// command: up | down | status.
func RunMigrate(dir, dsn, command string) error {
	return migrate.RunMigrate(dir, dsn, command)
}
