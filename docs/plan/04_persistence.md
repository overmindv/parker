# 04 — Фаза 3: PostgreSQL и миграции (pool, `parker migrate`, health)

## Что делаем
Добавляем в parker работу с PostgreSQL: пул `pgxpool`, чек готовности (Ping), и подкоманду
`parker migrate` на базе **goose как библиотеки** (pressly/goose/v3), чтобы убрать отдельный
goose-бинарник и дублирование миграционного тулинга из Makefile каждого сервиса.

## Зависимости
- Фазы 1–2 (runner + http + health) выполнены.
- `github.com/jackc/pgx/v5/pgxpool`, `github.com/pressly/goose/v3` (как библиотека, с
  `no_*` build-tags на драйверы как в `tasks` Makefile/Dockerfile).

## Миграции: дизайн

Единый бинарник `parker` получает подкоманду `migrate`:
```
parker migrate up                 # применить все миграции из MIGRATIONS_DIR
parker migrate down               # откатить одну
parker migrate status             # показать применённые/оставшиеся
parker migrate --dir migrations --dsn postgres://...                # явные параметры
parker migrate --dir migrations --dsn "$DATABASE_URL" up
```
- Каталог по умолчанию — `MIGRATIONS_DIR` (default `migrations`), DSN — флаг `--dsn` или env `DATABASE_URL`.
- SQL-миграции — **append-only** (конвенция Overmindv: не менять применённые).
- goose работает как библиотека: `goose.SetBaseFS`, `goose.UpContext`, и т.д.
  Миграции могут быть и embed в бинарь (чтобы в Docker не копировать отдельно `migrations/`),
  и/или из файловой системы — **решение по умолчанию: goose из файловой системы** `MIGRATIONS_DIR`
  (как в `tasks`, чтобы один и тот же каталог использовал локальный `make` и контейнер).
  Вариант embed-fs — флага `--embed`, решаем при реализации.

Сгенерированный сервис использует подкоманду из своего бинари:
- локально: `make migrate-up` = `go run ./cmd/<svc> migrate up`;
- в `infra/docker-compose.yml`: `*-migrate` контейнер = `parker migrate up`, затем апп.

## Pool и health

```go
// parker/postgres.go (или internal/postgres)
func OpenPool(ctx context.Context, opts Options) (*pgxpool.Pool, error) // pgxpool.New + Ping
// pool реализует HealthCheck.Ping(ctx) -> pool.Ping(ctx)

// App:
func (a *App) Postgres() (*pgxpool.Pool, error)
// - если WithPostgres (default true) и DATABASE_URL задан — открывает (лениво) и регистрирует
//   health-чек "postgres"; возвращает пул; ошибка => возвращается из run и валит приложение на старте.
```

## Файл за файлом: задачи

1. `migrate.go` — команда migrate (goose library): `up/down/status`, флаги `--dir`, `--dsn`.
2. `postgres.go` — `OpenPool`, интеграция в `App.Postgres()` + health-чек "postgres".
3. `cmd/parker/main.go` — полная диспетчеризация: `serve`/`migrate`/`init`/`version`.
4. `Makefile` parker: таргеты `migrate-up/migrate-down` через `go run ./cmd/parker migrate`.
5. `example/` — добавить `migrations/0001_init.sql` и `repo` (схема-заглушка), чтобы проверить `migrate up` + `ping`.

## Безопасность
- DSN никогда не печатать в лог целиком (маскировать пароль).
- SQL миграций — только разработчик сервиса; parker не хранит схему бизнес-сервисов.

## Приёмка / DOD
- `parker migrate up` на пустой БД применяет `example/migrations/0001_init.sql`; `status` показывает applied.
- `app.Postgres()` пингует БД; `/ready` без БД → `503` (когда чек постгреса зарегистрирован), с БД → `200`.
- `parker migrate down` откатывает одну миграцию.
- CI parker: component-джоб поднимает postgres:17 (по образцу `tasks/.github/workflows/ci.yml`) и гоняет `parker migrate up` + тест пула.

## Тесты
- `migrate_test.go`: в temp-dir создаётся 2 SQL-миграции; `up` применяет обе, `down` откатывает одну, `status` корректен (на выделенной тестовой БД из env `PARKER_TEST_DSN`).
- `postgres_test.go`: `OpenPool` поднимает пул, `Ping` ок; при неверном DSN — ошибка.
- Контракт `app.Postgres()` через `example/` component-тест (см. `09_testing.md`).