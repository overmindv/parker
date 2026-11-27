// Команда jetgen читает каталог PostgreSQL и создаёт файлы во временном каталоге.
package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/go-jet/jet/v2/generator/metadata"
	generator "github.com/go-jet/jet/v2/generator/postgres"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	dsn := os.Getenv("JET_DATABASE_DSN")
	if len(os.Args) != 2 || dsn == "" {
		log.Fatal("expected staging directory and JET_DATABASE_DSN")
	}
	files, err := os.ReadDir(os.Args[1]) //nolint:gosec // каталог передаёт локальный скрипт генерации
	if err != nil || len(files) != 0 {
		log.Fatal("Jet requires an empty staging directory")
	}
	schema := os.Getenv("JET_SCHEMA")
	if schema == "" {
		schema = "public"
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal("invalid JET_DATABASE_DSN")
	}
	defer func() { _ = db.Close() }()
	// Ошибки подключения драйвера могут содержать исходный DSN.
	if err := generator.GenerateDB(db, schema, os.Args[1], template.Default(postgres.Dialect).UseSchema(outboxSchema)); err != nil {
		log.Fatal("Jet generation failed; verify connection, schema and read privileges")
	}
}

func outboxSchema(meta metadata.Schema) template.Schema {
	schema := template.DefaultSchema(meta).UsePath(".")
	schema.Model.Table = func(table metadata.Table) template.TableModel {
		model := template.DefaultTableModel(table)
		model.Skip = !ownedTable(table.Name)

		return model
	}
	schema.SQLBuilder.Table = func(table metadata.Table) template.TableSQLBuilder {
		builder := template.DefaultTableSQLBuilder(table)
		builder.Skip = !ownedTable(table.Name)

		return builder
	}
	schema.Model.View = func(metadata.Table) template.ViewModel { return template.ViewModel{Skip: true} }
	schema.SQLBuilder.View = func(metadata.Table) template.ViewSQLBuilder { return template.ViewSQLBuilder{Skip: true} }
	schema.Model.Enum = func(metadata.Enum) template.EnumModel { return template.EnumModel{Skip: true} }
	schema.SQLBuilder.Enum = func(metadata.Enum) template.EnumSQLBuilder { return template.EnumSQLBuilder{Skip: true} }

	return schema
}

func ownedTable(name string) bool { return name == "outbox" || name == "outbox_replay_audit" }
