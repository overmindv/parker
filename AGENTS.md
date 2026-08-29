# AGENTS.md — правила для агента, работающего с parker

Этот файл дополняет `CLAUDE.md` деталями, важными при изменении кода parker.

## Слои и границы

- `parker` (корень пакета) — **стабильный публичный API** для прикладных сервисов:
  `parker.Main`, `parker.App`, `parker.Options`, `parker.Option`, `parker.Runnable`,
  `parker.HealthCheck`, `parker.Env/EnvDuration/EnvInt64/EnvList`.
- `internal/` — неэкспортируемые детали (runner, http-сервер, postgres, kafka, outbox).
- `cmd/parker` — CLI: dispatch подкоманд `serve`/`migrate`/`init`/`version`.
- Не расширяй публичный API без нужды; каждое добавление проходится через код-ревью и тестами.

## Жизненный цикл (инвариант)

`parker.Main(run, opts...)`:
1. разбирает подкоманду; 2. загружает `Options` и логгер; 3. собирает `App`; 4. вызывает `run(*App)`;
5. `Runner` запускает HTTP-сервер и фоновые воркеры (runnables); 6. ждёт сигнал SIGINT/SIGTERM
или fatal-ошибку воркера; 7. graceful shutdown с `SHUTDOWN_TIMEOUT`.

Паттерн fatal-остановки: runnable, вернувший ошибку при `ctx.Err()==nil`, логируется и останавливает
весь процесс (как `runWorker`/`serve` в `tasks`). Это поведение не менять без веских причин.

## Конвенции

- Структурные JSON-логи без тел запросов и без чувствительных полей.
- `http.Server`-таймауты задаются из `Options` (`ReadTimeout`/`WriteTimeout`).
- `os.Exit` — только в `cmd/parker/main.go` (возврат кода); пакеты возвращают ошибки.
- Тесты: юнит — `go test -race ./...` без внешних зависимостей; integration/component — тег
  `component` (postgres:17 / kafka) с `PARKER_TEST_DSN`.

## План

Актуальный план разработки — `docs/plan/` (README = индекс, 00–09 = фазы, TASKS.md = чек-лист).
После каждой фазы обязателен зелёный CI (`make build && make test && make lint`).
