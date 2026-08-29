# 01 — Архитектура parker: структура репозитория и пакеты

## Репозиторий parker

```
parker/                                  module github.com/overmindv/parker
├── go.mod
├── Makefile                             (build/test/lint; сам parker проходит те же CI-джобы)
├── CLAUDE.md                            (инструкции для агента, работающего с parker)
├── AGENTS.md
├── .github/workflows/ci.yml             (linter + test + build + component для САМОГО parker)
├── run.go                               parker.Run / parker.Main / parker.WithAppName
├── app.go                               тип App (связка конфиг/логгер/pg/kafka/http/health)
├── options.go                           Options: конфигурация parker (дефолты + валидация)
├── config.go                            загрузка env в Options (helpers env/envDuration/envInt64/envList)
├── log.go                               сборка *slog.Logger (JSON, уровень)
├── lifecycle.go                         Runner: signals, graceful shutdown, runnables
├── health.go                            HealthRegistry + тип HealthCheck
├── metrics.go                           prometheus-регистр + метрики запросов
├── migrate.go                           подкоманда migrate (goose-библиотека)
├── cmd/parker/main.go                   CLI entrypoint: dispatch serve|migrate|init
│   └── internal/init/                   реализация `parker init`
├── internal/...                         неэкспортируемые детали фреймворка
├── template/                            go:embed-шаблоны каркаса сервиса («parker init»)
│   └── service/                         .tmpl-файлы: main.go, Makefile, Dockerfile, CI, .env.example, ...
└── example/                             эталонный сервис на parker (используется в тестах)
```

**Принцип пакетов:** верхний уровень `parker` экспортирует ограниченный, стабильный API
(`Main`, `App`, `Options`, `NewHealthCheck`, ...). Всё остальное — приватные пакеты.
Подпапки с реализацией инфраструктуры (postgres/kafka/httpsrv) могут быть публичными, если нужны
сервису для расширения, но по умолчанию — `internal/`.

## Ключевые типы (целевой контракт)

```go
// Файл: parker/app.go
package parker

// App — единая точка входа для прикладного сервиса: всё инфраструктурное уже собрано.
type App struct {
    opts   Options
    log    *slog.Logger
    pool   *pgxpool.Pool            // nil, если postgres не запрошен
    http   *HTTPServer
    kafka  *Kafka
    health *HealthRegistry
    runnables []Runnable
}

// Main запускает приложение: разбирает подкоманду (serve|migrate), собирает App,
// вызывает run(app), запускает HTTP/воркеров и ждёт graceful shutdown.
func Main(run func(*App) error, opts ...Option) int

type Option func(*Options)

func WithAppName(name string) Option      // SERVICE_NAME по умолчанию
func WithPostgres(required bool) Option   // поднимать pgxpool (default true)
func WithKafka(required bool) Option      // поднимать kafka clients (default false)
func WithReadinessDefault(requirePG bool, requireKafka bool) Option

// Методы App:
func (a *App) Config() Options
func (a *App) Logger() *slog.Logger
func (a *App) Postgres() (*pgxpool.Pool, error)     // инициализация по запросу
func (a *App) HTTP() *HTTPServer                     // роутер с уже встроенными middlewares
func (a *App) AddHealthCheck(name string, check HealthCheck)
func (a *App) AddRunnable(name string, run func(context.Context) error)
```

```go
// Файл: parker/health.go
package parker

type HealthCheck interface{ Ping(ctx context.Context) error }

type HealthCheckFunc func(ctx context.Context) error
func (f HealthCheckFunc) Ping(ctx context.Context) error { return f(ctx) }

type HealthRegistry struct{ /* name -> check */ }
func NewHealthRegistry() *HealthRegistry
func (h *HealthRegistry) Add(name string, c HealthCheck)
func (h *HealthRegistry) Ready(ctx context.Context) error  // все чекки разом; первый failure возвращается
```

```go
// Файл: parker/lifecycle.go
package parker

type Runnable func(ctx context.Context) error

// Runner управляет сигналами, HTTP-сервером и фоновыми runnables.
type Runner struct{ /* ... */ }

func NewRunner(log *slog.Logger, shutdownTimeout time.Duration) *Runner
func (r *Runner) AddRunnable(name string, rn Runnable)
func (r *Runner) Run(ctx context.Context, server *http.Server) error
// - стартует server.ListenAndServe и каждый runnable в отдельной горутине;
// - fatal-ошибка воркера (при ctx не отменён) => аварийный stop() (паттерн runWorker/serve из tasks);
// - ожидает ctx.Done() (сигнал) либо fatal => graceful shutdown всех воркеров + http.Server.Shutdown.
```

## Options: конфигурация (env → поля)

```go
// Файл: parker/options.go
type Options struct {
    ServiceName    string
    HTTPAddress    string
    Environment    string            // local|development|test|staging|production
    LogLevel       string            // debug|info|warn|error
    ReadTimeout    time.Duration
    WriteTimeout   time.Duration
    ShutdownTimeout time.Duration

    // Postgres
    DatabaseURL    string
    DBRequired     bool
    MigrationsDir  string            // default "migrations"

    // Kafka
    KafkaBrokers   []string
    KafkaRequired  bool

    // Metrics / pprof
    MetricsEnabled bool
    PprofEnabled   bool
}
```

Стандартные env-переменные, читаемые parker (все с дефолтами и валидацией):

| env | default | примечание |
|---|---|---|
| `SERVICE_NAME` | из `WithAppName` | |
| `HTTP_ADDR` | `:8080` | |
| `ENV` | `local` | |
| `LOG_LEVEL` | `info` | |
| `READ_TIMEOUT` | `10s` | |
| `WRITE_TIMEOUT` | `20s` | |
| `SHUTDOWN_TIMEOUT` | `10s` | |
| `DATABASE_URL` | — | обязателен, если `WithPostgres` |
| `MIGRATIONS_DIR` | `migrations` | |
| `KAFKA_BOOTSTRAP_SERVERS` | — | из `WithKafka` |
| `METRICS_ENABLED` | `true` | |
| `PPROF_ENABLED` | `false` | |

Бизнес-конфиг сервиса читается через экспортируемые helpers:
```go
parker.Env("TASK_HUNTER_INGEST_TOKEN", "")
parker.EnvDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond)
parker.EnvInt64("MEMORY_LIMIT_BYTES", 64<<20)
parker.EnvList("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
```
Это убирает из сервиса дублирование `internal/config`.

## Границы и зависимости

- **Внутри parker:** `config` ← `log` ← `lifecycle` ← `app`. `app` собирает всё и передаёт сервису.
- **HTTP и health:** `HTTPServer` владеет роутером; middleware (request-id, recovery, access-log) встроены до регистрации бизнес-роутов; `/health`, `/ready`, `/metrics` регистрируются автоматически рядом с бизнес-роутами.
- **Сервис → parker:** прикладной сервис зависит только от `github.com/overmindv/parker` (+ сам ходит в `pgx`, `franz-go`, если ему нужно больше контроля — но обычно достаточно методов `App`).
- **parker НЕ знает** про `api-gateway`, про бизнес-типы сервисов, про схему БД сервиса (схему и jet-модели сервис держит сам).

## Команды/подкоманды CLI

`parker` — один бинарник, диспетчеризация по первому аргументу:
- `parker serve` (по умолчанию) — запускает прикладной сервис (вызывает `run(*App)`).
- `parker migrate [--dir migrations] [--dsn ...] [up|down]` — применяет/откатывает миграции (goose).
- `parker init <service-name>` — генерирует каркас сервиса (Фаза 6, файл 07).

В Docker-образе сервиса лежит этот же бинарник: `*-migrate` контейнер запускает `parker migrate up`,
апп-контейнер — `parker serve` (default).

## Файл за файлом: задачи Фазы 0

1. `go mod init github.com/overmindv/parker` + `go.mod` (Go 1.26).
2. `Makefile`: `build`, `test` (`go test -race ./...`), `lint` (golangci-lint v2), `tidy`.
3. `.github/workflows/ci.yml` для самого parker: lint / test / build / component (по образцу `tasks`).
4. `.gitignore`, `.gitattributes`, `CLAUDE.md`, `AGENTS.md`, `CODEOWNERS`.
5. Пустые пакеты-заглушки: `app.go`, `options.go`, `config.go`, `log.go`, `lifecycle.go`, `health.go`, `metrics.go`, `migrate.go`, `cmd/parker/main.go`.
6. Эталонный `example/` (можно сначала каркас, наполнить в Фазе 7).

**DOD Фазы 0:** `make build && make test && make lint` проходят; CLI `parker` собирается и печатает usage.