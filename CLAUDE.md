# CLAUDE.md

## Репозиторий

`parker` — лёгкий фреймворк-каркас сервисов Overmindv: берёт на себя
инфраструктурную часть Go-сервиса (конфиг, логирование, HTTP, health/readiness, PostgreSQL+миграции,
Kafka+outbox, метрики, жизненный цикл/graceful shutdown), чтобы в прикладных сервисах оставалась
только бизнес-логика. Плюс CLI `parker init` для генерации каркаса нового сервиса.

Подробный план разработки — в `docs/plan/` (файлы 00–09 + TASKS.md). Перед нетривиальными
изменениями читай релевантные файлы плана.

## Правила

- **Только открытые библиотеки**, без вендорских тулкитов.
- Ядро — std `net/http` (Go 1.22 method-паттерны) и `log/slog`.
- **Единый бинарник** `parker`, подкоманды: `serve` (по умолчанию), `migrate`, `init`, `version`.
- Сервисы подключают parker как `require github.com/overmindv/parker` + `replace => ../parker`
  (локальная разработка); в Docker-сборке `replace` снимается (parke подтягивается по тегу).
- Публичный API верхнего уровня (`Main`, `App`, `Options`, ...) — стабильный и минимальный;
  детали реализации — в `internal/`.
- Не логировать `Options`/DSN целиком (секреты). `os.Exit` — только внутри `Main` (возврат кода).

## Команды

```bash
make build        # go build ./...
make test         # go test -race ./... (без Docker)
make lint         # golangci-lint run
make tidy         # go mod tidy
```

CLI:
```bash
go run ./cmd/parker version
go run ./cmd/parker serve            # поднять пример/сервис (default subcommand)
```
