package parker

import (
	"fmt"
	"os"
)

// runMigrateCLI — обработчик подкоманды migrate:
//
//	parker migrate [--dir migrations] [--dsn postgres://...] (up|down|status)
//
// По умолчанию: dir=migrations, dsn=env DATABASE_URL, command=up.
func runMigrateCLI(args []string) int {
	dir := "migrations"
	dsn := os.Getenv("DATABASE_URL")
	command := "up"

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--dir":
			if i+1 < len(args) {
				i++
				dir = args[i]
			}
		case "--dsn":
			if i+1 < len(args) {
				i++
				dsn = args[i]
			}
		default:
			command = args[i]
		}
	}

	if err := RunMigrate(dir, dsn, command); err != nil {
		fmt.Fprintf(os.Stderr, "parker: migrate: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "parker: migrate %s %q ok\n", command, dir)
	return 0
}

// runInitCLI — обработчик подкоманды init:
//
//	parker init <service-name> [--pg|--no-pg] [--kafka|--no-kafka]
//
// Генерирует каркас сервиса в текущем каталоге и печатает compose-блок и чек-лист.
func runInitCLI(args []string) int {
	var name string
	hasPG := true
	hasKafka := false
	for _, a := range args {
		switch a {
		case "--pg":
			hasPG = true
		case "--no-pg":
			hasPG = false
		case "--kafka":
			hasKafka = true
		case "--no-kafka":
			hasKafka = false
		default:
			if name == "" {
				name = a
			}
		}
	}

	if name == "" {
		fmt.Fprintln(os.Stderr, "parker: init: укажите имя сервиса, например: parker init <service-name> [--pg] [--kafka]")
		return 2
	}

	if err := GenerateService(".", name, hasPG, hasKafka); err != nil {
		fmt.Fprintf(os.Stderr, "parker: init: %v\n", err)
		return 1
	}

	fmt.Fprintf(os.Stderr, "parker: init %q: каркас создан в текущем каталоге\n", name)
	PrintComposeBlock(os.Stdout, name, hasPG, hasKafka)
	PrintPostInitChecklist(os.Stdout, name)
	return 0
}
