# Parker — лёгкий каркас сервисов Overmindv

## Что строим

**parker** — это **фреймворк-библиотека + CLI**, который берёт на себя всю инфраструктурную часть Go-сервиса (конфигурация, логирование, HTTP-сервер, health/readiness, PostgreSQL и миграции, Kafka с outbox, метрики, жизненный цикл/graceful shutdown), а в самом сервисе остаётся только **бизнес-логика**.

Сегодня каждый сервис Overmindv (`tasks`, `users`, `entities`, `task-hunter`, `media`, `content`) тащит одинаковый boilerplate: `main.go`-wiring, `internal/config` (~250 строк env-парсинга), Makefile, Dockerfile, health-чекки, Kafka-adapter с outbox, CI. Цель parker — выразить всё это **один раз** и дать команде «создать новый сервис из пары команд».

## Ключевые решения (зафиксированы)

| Параметр | Решение |
|---|---|
| Форма | **Гибрид**: Go-модуль-библиотека `github.com/overmindv/parker` + CLI `parker init` |
| Как сервис подключает parker | `require github.com/overmindv/parker` + `replace => ../parker` (соседние репозитории) |
| Стек | только **открытые** библиотеки: `net/http`, `log/slog`, `jackc/pgx/v5`, `pressly/goose/v3` (как библиотека), `twmb/franz-go`, `prometheus/client_golang` |
| Язык сервиса | Go 1.26+ (совпадает с конвенцией репозитория) |
| `parker init` охват | репозиторий целиком (go.mod, main.go, Makefile, Dockerfile, migrations/, CI, .env.example, README) + печать готового блока для `infra/docker-compose.yml` |
| Транспорт | `net/http` с Go 1.22 method-паттернами (совпадает с `tasks`) |
| Миграции | подкоманда `parker migrate` на базе goose-библиотеки; единый бинарник (`serve`/`migrate`) |
| Kafka | франц-go; реализуются producer, consumer-group-подписчик, outbox-dispatcher |
| Наблюдаемость | `/metrics` (prometheus), JSON-логи с `request_id`/trace, `/health` + `/ready`; Grafana/алертинг — заглушки «добавить позже» |
| Платформенные термины (Vault, .o3/k8s, Grafana) | маппятся на реальную локалку; под каждым шагом указан «платформенный аналог» (если платформа появится) |
| Репозиторий parker | `overmindv/parker` (этот каталог), сервисы — соседние git-репозитории |

## Почему именно такой дизайн

- **Библиотека, а не копипаст-шаблон**: инфраструктурный код живёт один раз в parker; правки доезжают во все сервисы через зависимость. Это ключевое отличие от «кодогенератор, копирующий шаблон».
- **`parker init` — это тонкий каркас**, а не полный сгенерированный сервис: он выдаёт `main.go`, в котором уже вызван `parker.Run(...)`, и структуру каталогов, где разработчик пишет только domain/usecase/repository/transport.
- **Единый бинарник `serve`/`migrate`** убирает отдельный goose-контейнер и дублирование миграционного тулинга из Makefile каждого сервиса.
- **`replace => ../parker`** даёт мгновенную локальную разработку без публикации версии; публикация тегов — опционально.

## Архитектура

```
                 ┌──────────────────────────────────────────────┐
                 │              ПРИКЛАДНОЙ СЕРВИС               │
                 │  (overmindv/<svc>, github.com/overmindv/<svc>) │
                 │                                              │
                 │   internal/<svc>/                             │
                 │     domain/    — бизнес-типы и инварианты      │
                 │     usecase/   — оркестрация сценариев          │
                 │     repository/ + adapter/ — persistence        │
                 │     transport/ — HTTP-обработчики (routes)      │
                 │                                              │
                 │   cmd/<svc>/main.go  —  толМКО: parker.Run(...) │
                 └───────────────┬──────────────────────────────┘
                                 │ import github.com/overmindv/parker
                 ┌───────────────▼──────────────────────────────┐
                 │                 PARKER (фреймворк)            │
                 │  config · logging · lifecycle/Runner          │
                 │  HTTPServer (router, middleware, health)      │
                 │  postgres (pool + migrate) · kafka + outbox   │
                 │  metrics /metrics · trace-контекст            │
                 └───────────────┬──────────────────────────────┘
                                 │ сгенерированный docker-compose-блок
                 ┌───────────────▼──────────────────────────────┐
                 │  overmindv/infra  (docker-compose + GH Actions)│
                 │  <svc>-postgres · <svc>-migrate · <svc>        │
                 └──────────────────────────────────────────────┘
```

## Целевой API сервиса (что «останется» разработчику)

Главный контракт, на который реализуем parker — сгенерированный `main.go` прикладного сервиса:

```go
package main

import (
    "github.com/overmindv/parker"
    "github.com/overmindv/parker/postgres"
    "github.com/overmindv/<svc>/internal/<svc>"
)

func main() {
    parker.Main(run, parker.WithAppName("<svc>"))
}

func run(app *parker.App) error {
    // Всё инфраструктурное (конфиг, логгер, pg, kafka, метрики) — уже в parker.
    pg, err := app.Postgres()                 // *pgxpool, pool/health готовы
    if err != nil { return err }

    repo := mysvc.NewRepository(pg)
    uc   := mysvc.NewUsecase(repo, app.Logger())
    h    := mysvc.NewHTTP(uc, app.Logger())

    // Сервис РЕГИСТРИРУЕТ только бизнес-роуты на HTTP-сервере parker.
    app.HTTP().Handle("GET /tasks", h.List)
    app.HTTP().Handle("POST /tasks", h.Create)

    // Регистрирует свои health-чекки и фоновые воркеры.
    app.AddHealthCheck("postgres", pg)         // pg реализует Ping
    app.AddRunnable("outbox", outbox.Run)     // фоновый воркер с graceful shutdown
    return nil
}
```

## Фазы разработки parker

```
Фаза 0  Каркас репозитория parker   (go.mod, Makefile, CI, скелет пакетов)
   │
Фаза 1  Ядро рантайма               (config, logging, lifecycle/Runner, App, `serve`)
   │
Фаза 2  HTTP-транспорт              (server, router, middleware, /health /ready /metrics)
   │
Фаза 3  Постгрес + миграции         (pool, `parker migrate`, pg health-чек)
   │
Фаза 4  Kafka + outbox              (producer, consumer-group, outbox-dispatcher)
   │
Фаза 5  Наблюдаемость               (metrics-helpers, trace-контекст, доки по алертам)
   │
Фаза 6  CLI `parker init`           (go:embed-шаблоны, скаффолд сервиса, compose-блок)
   │
Фаза 7  Примерный сервис + интеграция (example/ end-to-end, проверка в infra-compose)
   │
Фаза 8  Документация и полировка    (Workflow Частей 1–5, TASKS, README)
```

## Навигация по файлам плана

| Файл | Содержание |
|---|---|
| [`00_overview.md`](00_overview.md) | цели, объём, что parker берёт/не берёт, не-цели |
| [`01_architecture.md`](01_architecture.md) | структура репозитория parker, пакеты, границы, модульность |
| [`02_core_runtime.md`](02_core_runtime.md) | Фаза 1: config, logging, lifecycle/Runner, `App`, подкоманды `serve`/`migrate` |
| [`03_http_transport.md`](03_http_transport.md) | Фаза 2: HTTP-сервер, роутер, middleware, health/readiness, метрики-эндпоинт |
| [`04_persistence.md`](04_persistence.md) | Фаза 3: postgres pool, миграции (goose), pg health-чек |
| [`05_messaging.md`](05_messaging.md) | Фаза 4: kafka producer/consumer, outbox |
| [`06_observability.md`](06_observability.md) | Фаза 5: метрики, trace/grequest-id, dashboards/алерты (TODO) |
| [`07_cli_init.md`](07_cli_init.md) | Фаза 6: CLI `parker init`, шаблоны, скаффолд, compose-блок |
| [`08_service_bootstrap.md`](08_service_bootstrap.md) | **Ключевой результат**: workflow создания сервиса (Части 1–5) + платформенные аналоги |
| [`09_testing.md`](09_testing.md) | Фаза 7: как тестировать parker и сгенерированный сервис |
| [`TASKS.md`](TASKS.md) | сводный чек-лист задач (галочки для отслеживания) |

## Что сделать в первую очередь

1. Фаза 0 (каркас репозитория parker) — тривиально.
2. **Фаза 1 (ядро рантайма)** — фундамент, от него зависит всё остальное.
3. Параллельно смотреть Фазу 7 (пример) как эталон целевого API — держит остальные фазы в фокусе «только бизнес-логика в сервисе».