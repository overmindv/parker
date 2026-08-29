# 02 — Фаза 1: Ядро рантайма (config, logging, lifecycle, App)

## Что делаем
Собираем фундамент, на котором живут все остальные фазы: загрузка конфига, логгер,
Runner (сигналы + graceful shutdown + фоновые воркеры), тип `App`, подкоманда `serve`,
и CLI-диспетчер (`serve` / `migrate` / `init`-заглушка).

## Зависимости
- Фаза 0 (каркас репозитория) — выполнена.
- Открытые библиотеки: только std (`net/http`, `log/slog`, `os/signal`, `context`).
  `pgx`/`franz-go` на этой фазе НЕ нужны.

## Целевые контракты — уже определены в `01_architecture.md`
- `Options` + `config.go` (env helpers: `Env`, `EnvDuration`, `EnvInt64`, `EnvList`; валидация).
- `parker.Main(run, opts...)` + `Option` (functional options).
- `App` (пока с HTTP и health на следующей фазе; на этой — логгер+конфиг+lifecycle).
- `Runner` (signals, graceful shutdown, runnables, fatal-остановка).

## Файл за файлом

### `config.go`
Перенести логику env-парсинга/валидации из эталонного `tasks/internal/config/config.go` как
публичные helpers, плюс загрузка стандартных `Options`:
- `Load() (Options, error)` — читает стандартные env в `Options`, применяет дефолты, валидирует
  (непустой `SERVICE_NAME`, корректный `HTTP_ADDR`, допустимый `ENV` и `LOG_LEVEL`,
  положительные таймауты, корректный `DATABASE_URL` если требуется).
- `Env/EnvDuration/EnvInt64/EnvList` — те же сигнатуры, что в `tasks` (они уже гоняются тестами в `tasks/internal/config/config_test.go` — перенести тесты).

### `log.go`
```go
func NewLogger(level string) (*slog.Logger, error)
// slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
```
Маппинг уровня: debug/warn/error default info (см. `logLevel` из `tasks/cmd/tasks/main.go`).

### `lifecycle.go`
```go
type Runnable func(ctx context.Context) error

type Runner struct { log *slog.Logger; shutdownTimeout time.Duration; runnables []struct{name string; fn Runnable} }

func NewRunner(log *slog.Logger, shutdownTimeout time.Duration) *Runner

func (r *Runner) AddRunnable(name string, fn Runnable)

func (r *Runner) Run(ctx context.Context, server *http.Server) error
```
Поведение (аналог `tasks` `serve`/`runWorker`):
- подписка на `syscall.SIGINT`/`SIGTERM` → отменяет `ctx`;
- стартует `server` и каждый runnable в горутине;
- неожиданная ошибка воркера при `ctx.Err()==nil` → логирует и аварийно останавливает всё;
- по `ctx.Done()` → `http.Server.Shutdown` с таймаутом + корректное завершение воркеров.

### `app.go` (на этой фазе — базовая версия)
- `Main(run func(*App) error, opts ...Option) int`:
  1. разбор подкоманды (`serve` по умолчанию; `migrate` — заглушка, реализация в Фазе 3);
  2. `Options` + `NewLogger`;
  3. создать `App`, вызвать `run(app)`;
  4. `Runner.Run(...)`; вернуть код выхода (0 — ok, 1 — ошибка).
- `App.Load()`/`Logger()`; `Postgres()`/`HTTP()`/`Kafka()` пока возвращают заглушки с ясной ошибкой
  «реализовано в следующей фазе» (или отсутствуют — добавим по мере фаз).

### `cmd/parker/main.go`
- CLI-диспетчер: `parker [serve|migrate|init] [args...]`.
- Флаг версии `parker version`.
- `serve` → `parker.Main(run)`; `migrate`/`init` — команды-заглушки («coming in next phases»).
- При старте логирует: `{service} запущен, address, environment` (как в `tasks`).

## Безопасность и конвенции
- Структурные JSON-логи без body запросов (access-лог — в Фазе 2, без payload).
- `Options` никогда не логировать целиком (секреты в `DATABASE_URL` и т.п.) — логировать только имя/адрес.
- Запрещён `os.Exit` в середине пакета кроме `Main` (возврат кода).

## Приёмка / DOD Фазы 1
- `parker serve` поднимает HTTP-заглушку (можно только health — пока на пустом маршруте), graceful shutdown по Ctrl+C за `SHUTDOWN_TIMEOUT`.
- fatal-ошибка runnable останавливает приложение (проверяется тестом).
- `go test -race ./... && go vet ./... && make lint` зелёные.
- Тестовая команда `parker version` печатает версию.

## Тесты
- `config_test.go` (перенесены из `tasks`).
- `log_test.go` (уровни).
- `lifecycle_test.go`: старт/стоп воркера, fatal-остановка, graceful shutdown по сигналу.
- `main_test.go`: разбор подкоманды.