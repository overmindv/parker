# 00 — Обзор: цели, объём, границы

## Зачем parker

Каждый Go-сервис Overmindv сегодня самостоятельно решает одни и те же инфраструктурные задачи:

- `main.go`: загрузка конфига → логгер → pgxpool → kafka → usecase'ы → HTTP-сервер → outbox-dispatcher → result-consumer → graceful shutdown (~150 строк boilerplate).
- `internal/config`: ~250 строк env-парсинга и валидации (helpers `env`/`envDuration`/`envInt64`/`envList`, валидация `DATABASE_URL`, таймаутов и т.д.).
- `Makefile`: goose-миграции, jet-генерация, golangci-lint, run/build/test/ctest.
- `Dockerfile`: многостадийная сборка + goose + копирование `migrations/`.
- health/readiness (`/health`, `/ready`) с пингом PostgreSQL и Kafka.
- Kafka (franz-go) producer/consumer + outbox dispatcher.
- GitHub Actions CI (lint/test/build/component/e2e).

**parker** собирает весь этот слой в одну переиспользуемую зависимость + CLI. После подключения parker разработчик нового сервиса пишет только: `domain`, `usecase`, `repository`/`adapter`, `transport` (обработчики), свои миграции и специфичные env. Всё инфраструктурное — конфиг, логгер, HTTP, health, postgres, kafka, метрики, жизненный цикл — приходит из parker.

## Что parker берёт на себя (scope)

1. **Загрузка и валидация конфигурации** из env (стандартные переменные с дефолтами) + переиспользуемые helpers для бизнес-конфигурации сервиса.
2. **Структурное логирование**: `log/slog` JSON, уровни из `LOG_LEVEL`, поля `request_id`/trace добавляются middleware.
3. **Жизненный цикл**: `Runner` — подписка на SIGINT/SIGTERM, запуск HTTP-сервера и фоновых воркеров, graceful shutdown с таймаутом, остановка процесса при неожиданной ошибке воркера (паттерн `runWorker`/`serve` из `tasks`).
4. **HTTP-сервер**: `net/http` + Go 1.22 method-паттерны, GET `/health` (liveness), GET `/ready` (readiness по зарегистрированным чеккам), GET `/metrics` (prometheus), middleware: request-id, recovery, безопасный access-лог. Бизнес регистрирует свои роуты на том же сервере через `app.HTTP().Handle(...)`.
5. **PostgreSQL**: пул `pgxpool`, проверка здоровья (Ping), **миграции** через подкоманду `migrate` (goose как библиотека). 
6. **Kafka**: producer (franz-go), consumer-group-подписчик, outbox-dispatcher.
7. **Метрики и наблюдаемость**: prometheus-регистр, `/metrics`, trace-контекст (`X-Request-ID`, W3C `traceparent`), вспомогательные метрики запросов.
8. **CLI `parker init`**: генерация каркаса нового сервиса.

## Что остаётся сервису (business-only)

- `internal/pkg/domain` — бизнес-типы, инварианты.
- `internal/pkg/usecase` — сценарии, транзакции (boundary транзакций — в сервисе, не в parker).
- `internal/pkg/store` — взаимодействие с базой данных.
- `internal/app` — HTTP-обработчики (регистрация на HTTP-сервере parker).
- `migrations/` — SQL-миграции сервиса.
- Специфичные для сервиса переменные окружения (пример: `TASK_HUNTER_INGEST_TOKEN`).
- `cmd/<svc>/main.go` — тонкий: вызов `parker.Main(run, ...)` + `run(*parker.App) error` с регистрацией роутов/чекков/воркеров.

## Не-цели (что parker НЕ делает)

- **НЕ** решает, как устроена бизнес-логика (слои domain/usecase/app — остаются в сервисе; parker не диктует гексагональность принудительно).
- **НЕ** генератор ORM/Jet-моделей: jet-модели и `adapter/postgres` остаются ответственностью сервиса (как в `tasks`). parker даёт только пул и миграции.
- **НЕ** запускает пользовательский код / песочницу (это `internal/execution` в `tasks` + `sandbox`).
- **НЕ** заменяет API-шлюз и GraphQL (это `api-gateway`).
- **НЕ** реализует платформенные интеграции, которых ещё нет в проекте: Vault, k8s (`.o3`), GitLab CI. Вместо них parker работает с реальной локалкой (docker-compose + GitHub Actions) и оставляет явные точки для будущей платформы.

## Принципы

- **Открытые библиотеки**, без привязки к вендорским тулкитам.
- **Один бинарник**, подкоманды `serve` (по умолчанию) и `migrate` — нет лишнего goose-контейнера и дублирования в Makefile.
- Мгновенная локальная разработка между соседними репозиториями.
- **Лёгкость**: parker не тащит тяжёлые фреймворки; core строится на std `net/http`/`log/slog`.
- **Понятность агенту**: каждый пакет parker решает ровно одну инфраструктурную задачу, API контракты фиксированы (см. файлы 02–05).

## Критерии успеха

Новый сервис «start-of-the-art» создаётся за:
1. `parker init <svc>` в клонированном репозитории;
2. вставить compose-блок в `infra/docker-compose.yml`;
3. завести миграции, реализовать бизнес-логику;
4. пайплайны зелёные; `/health`, `/ready`, `/metrics` отвечают из коробки.

Без правки инфраструктурного кода в сервисе.