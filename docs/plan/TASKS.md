# TASKS — сводный чек-лист разработки parker

Порядок = порядок фаз (README). Отмечайте галочки по мере выполнения. После каждой фозы —
зелёный CI самого parker (`make build && make test && make lint`).

## Фаза 0 — Каркас репозитория parker
- [ ] `go mod init github.com/overmindv/parker` (Go 1.26)
- [ ] `Makefile`: build/test/lint/tidy
- [ ] `.github/workflows/ci.yml` для parker (lint/test/build/component)
- [ ] `.gitignore`, `.gitattributes`, `CODEOWNERS`
- [ ] `CLAUDE.md`, `AGENTS.md` (правила для агента)
- [ ] Скелет пакетов: `app.go`, `options.go`, `config.go`, `log.go`, `lifecycle.go`, `health.go`, `metrics.go`, `migrate.go`, `cmd/parker/main.go`
- [ ] `example/` — каркас (наполнить в Фазе 7)
- [ ] **DOD:** build/test/lint зелёные; `parker` собирается и печатает usage

## Фаза 1 — Ядро рантайма (02_core_runtime.md)
- [ ] `config.go`: env-хелперы `Env/EnvDuration/EnvInt64/EnvList` + `Load() (Options,error)` (из `tasks`)
- [ ] `log.go`: `NewLogger(level)`
- [ ] `lifecycle.go`: `Runner` (signals, graceful shutdown, runnables, fatal-остановка)
- [ ] `app.go`: `Main(run, opts...)`, базовый `App` (логгер+конфиг)
- [ ] `cmd/parker/main.go`: dispatch `serve|migrate|init|version` (serve работает, migrate/init — заглушки)
- [ ] Тесты: config_test, log_test, lifecycle_test, main_test
- [ ] **DOD:** serve + graceful shutdown по Ctrl+C; fatal runnable останавливает app; CI зелёный

## Фаза 2 — HTTP-транспорт (03_http_transport.md)
- [x] `httpserver.go`: `HTTPServer`, эндпоинты `/health`, `/ready`, `/metrics`
- [x] `middleware.go`: recover → request-id → access-log (порядок); `RequestIDFrom`
- [x] `metrics.go`: `parker_http_requests_total`, `parker_http_request_duration_seconds`; `/metrics`
- [x] `health.go`: `HealthRegistry`, `HealthCheck`/`HealthCheckFunc`
- [x] Встроить в `Runner`; опции `MetricsEnabled`/`PprofEnabled`
- [x] Тесты: middleware_test, health_test, metrics_test, app_test (request_id в access-логе)

## Фаза 3 — PostgreSQL и миграции (04_persistence.md)
- [x] `migrate.go`: подкоманда `migrate up|down|status`, флаги `--dir`, `--dsn` (goose library)
- [x] `postgres.go`: `OpenPool`, `App.Postgres()`, health-чек "postgres"
- [x] `cmd/parker/main.go`: полная диспетчеризация serve/migrate/init/version
- [ ] Parke Makefile: `migrate-up/migrate-down` через `go run ./cmd/parker migrate` (сгенерируется в Фазе 6)
- [ ] `example/`: `migrations/0001_init.sql` + repo-заглушка (Фаза 7)
- [x] CI parker: component-джоб с postgres:17 (уже в .github/workflows/ci.yml)
- [x] Тесты: migrate_test, postgres_test (component, `PARKER_TEST_DSN`)

## Фаза 4 — Kafka и outbox (05_messaging.md)
- [x] `kafka.go`: `Producer`, `Subscriber` (franz-go), `App.NewProducer/NewSubscriber`, `Ping`s
- [x] `outbox.go`: `Outbox` interface, `OutboxDispatcher`, `PgOutbox` + `OutboxSchema` DDL
- [ ] Таблица outbox (SQL в шаблон сервиса) + пример использования в `example/` (Фаза 7)
- [x] Маппинг kafka health → `/ready` (Producer/Subscriber реализуют HealthCheck.Ping)
- [x] Тесты: outbox_test (юнит с фейками), outbox_pg_test (component, PARKER_TEST_DSN)

## Фаза 5 — Наблюдаемость (06_observability.md)
- [x] middleware: W3C `traceparent` → контекст → лог-поля `request_id/trace_id/span_id`
- [x] path-нормализация в метриках (`/tasks/{id}`) (сделано в Фазе 2)
- [x] `App.LoggerFor(ctx)` — логгер с полями контекста
- [x] `docs/dashboard.json` (Grafana sample), `docs/alerts.example.yml` (Prometheus rules)
- [x] health-метрика `parker_ready` (gauge) для будущего алерта ServiceDown
- [x] Тесты: trace_test (parse/middleware), docs_test (dashboard JSON), metrics_test

## Фаза 6 — CLI `parker init` (07_cli_init.md)
- [x] `template/service/` (go:embed): go.mod, main.go, Makefile, Dockerfile, ci.yml, env.example, README, migrations, заготовки internal/*
- [x] `init.go`: валидация имени, копирование/подстановка (text/template), compose-блок, чек-лист
- [x] Генерация go.mod с `replace github.com/overmindv/parker => ../parker`
- [x] Печать compose-блока и постинт-чеклиста (Части 1–5)
- [x] Флаги `--pg/--no-pg/--kafka/--no-kafka` (прогрессивно; `--no-input` — CI-дефолты)
- [x] Тесты: init_test (состав файлов, go.mod, содержимое), compose-block
- [x] **DOD:** `parker init demo` → сервис собирается и поднимается (проверено end-to-end);
      compose-блок печатается без правок

## Фаза 7 — `example/` и интеграция (09_testing.md)
- [ ] Примерный сервис `example/` на parker (бизнес-логика: ping + одна сущность + outbox при --kafka)
- [ ] Component end-to-end: migrate → serve → /health/ready/metrics → outbox → kafka → consumer
- [ ] Сквозной тест генератора: init demo → go mod tidy → build/vet/test внутри demo
- [ ] Проверить вставку compose-блока в `infra/docker-compose.yml` и старт в стеке
- [ ] **DOD:** глобальный DOD из 09_testing.md выполнен

## Фаза 8 — Документация и полировка
- [ ] `docs/architecture.md` для parker
- [ ] README parker (установка, `parker init`, FAQ), ссылка на `08_service_bootstrap.md`
- [ ] Чистка: удалить заглушки, отполировать сообщения CLI
- [ ] Проверить пример использования на соседнем реальном MVP-сервисе (регрессия workflow Частей 1–5)
- [ ] Обновить `TASKS.md` на финальном состоянии

---

## Критерий завершённости (общий)

Создание нового сервиса = Части 1–5 из `08_service_bootstrap.md`, и в сервисе **нет**
инфраструктурного кода (config-подсистемы, HTTP-сервера, lifecycle, миграционного тулинга) —
всё это живёт в parker.