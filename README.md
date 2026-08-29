# Parker

Фреймворк-каркас Go-сервисов Overmindv: берёт на себя 
инфраструктурную часть сервиса — конфигурацию, логирование, HTTP-сервер, health/readiness,
PostgreSQL + миграции, Kafka + outbox, метрики, жизненный цикл/graceful shutdown — чтобы
в прикладном сервисе оставалась **только бизнес-логика**.

Состоит из:
- **библиотеки** `github.com/overmindv/parker` — сервис вызывает `parker.Main(run, ...)`;
- **CLI** `parker init` — генерирует каркас нового сервиса (*Фаза 6, в разработке*).

## Статус (итерация реализации)

- ✅ **Фазы 0–1** — каркас репозитория и ядро рантайма: `Main`/`App`, конфиг из env
  (+хелперы `Env*`), структурный JSON-логгер, `Runner` (сигналы, graceful shutdown,
  фоновые воркеры, fatal-остановка), HTTP с `/health` и `/ready`, CLI `serve|migrate|init|version`.
- ✅ **Фаза 2** — HTTP-транспорт: middleware `recover → request-id → access-log` (+метрики),
  `/metrics` (prometheus), `/debug/pprof` за флагом `PPROF_ENABLED`.
- ✅ **Фаза 3** — PostgreSQL и миграции: `App.Postgres()` (pgxpool + health-чек), подкоманда
  `parker migrate up|down|status` (goose-библиотека, флаги `--dir`/`--dsn`).
- ✅ **Фаза 4** — Kafka + outbox: `Producer`/`Subscriber` (franz-go, at-least-once по commit после
  успешной обработки), `OutboxDispatcher` + `PgOutbox` (атомарная публикация «БД + событие»).
- ✅ **Фаза 5** — наблюдаемость: W3C `traceparent` → лог-поля `request_id/trace_id/span_id`,
  `App.LoggerFor(ctx)`, метрика `parker_ready`, ассеты `docs/dashboard.json` + `docs/alerts.example.yml`.
- ✅ **Фаза 6** — CLI `parker init`: генерация каркаса сервиса (go.mod+replace, main.go, Makefile,
  Dockerfile, CI, migrations/, internal/), печать compose-блока и чек-листа. Проверено end-to-end:
  `parker init demo && go build && serve` → `/ping`, `/health`, `/ready`, `/metrics`.
- ⏳ Фазы 7–8 — примерный сервис + интеграция в `infra`; полировка. См. план в [`docs/plan/`](docs/plan/).

## Быстрый старт `parker init`

```bash
# в пустом git-репозитории нового сервиса (соседнем с ../parker):
../parker/bin/parker init myservice --pg [--kafka]
cp .env.example .env
make dev
# дальше — Части 2–5 (см. parker/docs/plan/08_service_bootstrap.md)
```

План разработки и целевой workflow создания сервиса — в [`docs/plan/`](docs/plan/README.md)
(README — индекс; 00–09 — фазы; `TASKS.md` — чек-лист).

## Локальная разработка

```bash
make build      # go build ./...
make test       # go test -race ./...
make lint       # golangci-lint run
make tidy       # go mod tidy

go build -o bin/parker ./cmd/parker
./bin/parker version        # parker 0.1.0
./bin/parker serve          # поднять HTTP-заглушку с /health и /ready
```

### Минимальный сервис на parker (target API)

```go
package main

import "github.com/overmindv/parker"

func main() { parker.Main(run, parker.WithAppName("mysvc")) }

func run(app *parker.App) error {
	app.HTTP().HandleFunc("GET /ping", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})
	app.AddRunnable("worker", func(ctx context.Context) error { return worker.Run(ctx) })
	return nil
}
```

## Конвенции

Только открытые библиотеки; ядро — std `net/http` (Go 1.22 method-паттерны) и `log/slog`.
Единый бинарник `parker` (`serve` по умолчанию, `migrate`, `init`, `version`).
Сервисы подключают parker через `replace => ../parker` локально; в Docker-сборке `replace`
снимается (parke подтягивается по тегу). Подробнее — в `CLAUDE.md` и `AGENTS.md`.
