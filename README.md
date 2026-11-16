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
- ✅ **Фаза 6** — CLI `parker init`: генерация каркаса сервиса (go.mod с опубликованным тегом, main.go, Makefile,
  Dockerfile, CI, migrations/, internal/), печать compose-блока и чек-листа. Проверено end-to-end:
  `parker init demo && go build && serve` → `/ping`, `/health`, `/ready`, `/metrics`.
- ⏳ Фазы 7–8 — примерный сервис + интеграция в `infra`; полировка. См. план в [`docs/plan/`](docs/plan/).

## Быстрый старт `parker init`

```bash
# в пустом git-репозитории нового сервиса:
parker init myservice --pg [--kafka]
go mod tidy
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
Сервисы подключают parker как `require github.com/overmindv/parker` по тегу (сейчас
`v0.1.0`); `replace => ../parker` — только временный локальный dev-приём, в репозиториях
не используется. Подробнее — в `CLAUDE.md` и `AGENTS.md`.

## Гарантии consumer (OVM-181)

Subscriber обрабатывает одну запись за poll, блокируя rebalance до обработки и
коммита. Handler получает контекст с бюджетом 15 секунд и обязан учитывать отмену.
Текущая запись повторяется максимум трижды с паузами 100/200 мс. Исчерпание бюджета,
ошибка fetch или commit останавливает Runnable; supervisor перезапускает сервис,
а неподтверждённая запись доставляется снова. Следующий offset не подтверждается
через ошибку предыдущего. Payload и key не включаются в сообщения об ошибках.

Kafka component: `PARKER_TEST_KAFKA=localhost:19092 go test -race -tags=component . -run TestKafka`.
Обновление потребителей до нового тега выполняется отдельно в OVM-187.

## Lease стандартного PgOutbox (OVM-182)

Перед обновлением worker владелец применяет новую append-only миграцию из
`docs/migrations/202611160001_outbox_leases.sql` (также `OutboxLeaseMigration`).
Сначала остановить старые workers, затем включить новые: смешанный rollout со старым
алгоритмом без claim недопустим. `OutboxSchema` предназначена для новых таблиц.
Специализированная execution outbox Tasks не меняется.

`FetchPending` атомарно выдаёт lease на 30 секунд с уникальным `ClaimToken`.
Dispatcher публикует вне транзакции, с deadline lease, и вызывает `MarkClaimSent`.
После crash lease истекает: возможен безопасный дубль (at-least-once), но старый
worker уже не может подтвердить новый claim. Прямые пользователи `PgOutbox.MarkSent`
должны перейти на `MarkClaimSent`; tokenless ack leased-записей отвергается.
Существующий интерфейс `Outbox` и собственные адаптеры сервисов совместимы по коду;
им требуется отдельное внедрение leases. Новый тег и rollout — OVM-187.

Rollback: остановить все новые workers и дождаться завершения публикаций. Затем
в транзакции выполнить `UPDATE outbox SET status='pending', claim_token=NULL,
lease_until=NULL WHERE status='publishing'`. Только после этого запускать старую
версию. Столбцы миграции сохраняются; replay после ack может дать безопасный дубль.

## Retry, replay и retention (OVM-183)

После migration `202611160002_outbox_retries.sql` publish failure сохраняет attempts
и `next_attempt_at`: exponential backoff 1–150 секунд плюс jitter 0–100%, максимум
300 секунд; после 8 ошибок статус `exhausted` больше не планируется. Ошибка broker
и payload не записываются в логи. Crash до сохранения ошибки восстанавливается по
истечении lease и может дать дубль. Бюджет публикации оставляет время для записи retry.

Владелец вызывает `ReplayExhausted(ctx, id, expectedGeneration, actorID, isAdmin)`
только после авторизации оператора. Replay атомарно пишет audit без payload, увеличивает
поколение, обнуляет attempts и сохраняет исходное событие. Повтор старого generation
возвращает конфликт даже после следующего exhaustion. Endpoint публично не создаётся.

Передайте `app.Metrics()` последним необязательным аргументом `NewOutboxDispatcher`,
чтобы `/metrics` включал актуальный `parker_outbox_oldest_age_seconds` (в том числе
exhausted). Каждый цикл очищает до 100 sent старше 7 дней; pending/exhausted и audit
не удаляются. Метод `CleanupSent` ограничен 1000 строк и retention не менее суток.
При rollback exhausted сохраняются и требуют явного решения оператора; не переводить
их автоматически в pending. Обновление закреплённых service dependencies — OVM-187.


## Изолированный шаблон (OVM-186)

`init` закрепляет опубликованный v0.1.0 без replace. Новый сервис собирается без
соседнего Parker; локальная совместная разработка включается отдельным временным
`go.work` (см. сгенерированный README). Migration получает дату UTC и номер 0001;
последующие файлы append-only. `--no-pg` не создаёт миграций и не требует PostgreSQL
в Makefile, Dockerfile и Compose-примере. Go-файлы форматируются перед записью.

Проверка standalone: `go test -tags=component . -run TestGeneratedServiceBuildsWithoutSibling`.
Тест создаёт оба варианта в temp directory и выполняет tidy/build/test с GOWORK=off.
