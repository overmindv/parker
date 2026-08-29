# 03 — Фаза 2: HTTP-транспорт (server, router, middleware, health, metrics)

## Что делаем
Реализуем HTTP-слой parker: сервер на `net/http` (Go 1.22 method-паттерны), встроенные
middleware, эндпоинты `/health` (liveness), `/ready` (readiness), `/metrics` (prometheus),
и способ сервиса регистрировать свои бизнес-роуты на этом же сервере.

## Целевой контракт

```go
// parker/app.go — на HTTP:
func (a *App) HTTP() *HTTPServer

// parker/httpserver.go
type HTTPServer struct { /* *http.ServeMux + middleware-цепочка */ }

// Handle регистрирует бизнес-роут на том же муксе, поверх built-in middleware.
func (s *HTTPServer) Handle(pattern string, handler http.Handler)

func (s *HTTPServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))

// Реализация мукса из std (тот же, что в tasks transport): уже поддерживает "GET /tasks",
// "POST /tasks/{id}" и т.п.

// Встроенные эндпоинты (регистрируются автоматически):
//   GET /health  — всегда 200 «ok» (liveness, процесс жив)
//   GET /ready   — инвертирует HealthRegistry: 200 если все чекки ok, иначе 503 с телом
//   GET /metrics — promhttp.Handler() (если MetricsEnabled)
```

`/ready` должен отвечать **так же**, как в `tasks`: `200`/`503`, быстрый, не зависит от
внешних сервисов (иначе healthcheck в инфре будет каскадно валить).

## Middleware (порядок важен)

Снаружи → внутрь: **recover** → **request-id** → **access-log** → роутер/обработчик.

1. **recover** — ловить panic, логировать stack, отдавать `500` (не «уронить» процесс).
2. **request-id** — брать `X-Request-ID` из заголовка (или генерировать UUID), класть в
   `context.Context` и в ответ (`X-Request-ID`); логгер внутри запроса дополняется этим id.
3. **access-log** — после обработки: метод, путь, статус, длительность, request-id;
   **без** тела запроса/ответа и без чувствительных query-параметров (выдержка из принципов `tasks`).

Помощники для сервиса:
```go
// parker/middleware.go
func RequestIDFrom(ctx context.Context) string

// parker/slogx (или метод App) — логгер с request_id и trace context:
func (a *App) LoggerFor(ctx context.Context) *slog.Logger
```

## HealthRegistry (from `01`)

- `NewHealthRegistry()`, `Add(name, check)`, `Ready(ctx) error`.
- Паркер автоматически добавляет чек постгреса (в Фазе 3), когда `WithPostgres` и поле `Ping`.

## Метрики (минимум на этой фазе)

```go
// parker/metrics.go
func NewMetrics(service string) *Metrics
// Регистрирует в prometheus client:
//   parker_http_requests_total{service,method,path,status}
//   parker_http_request_duration_seconds{service,method,path} (histogram)

type Metrics struct{ /* counters/histograms */ }

func (m *Metrics) Wrap(next http.Handler) http.Handler // middleware-обёртка, считает метрики по роуту
```

## Файл за файлом: задачи

1. `httpserver.go` — `HTTPServer`, регистрация встроенных эндпоинтов.
2. `middleware.go` — recover / request-id / access-log (+ тесты).
3. `metrics.go` — prometheus-регистр, метрики запросов, `/metrics` через `promhttp`.
4. `health.go` — `HealthRegistry` + `HealthCheck` (`NewHealthCheck(f)`).
5. Подключить в `Runner`: `server.Handler = muxWithMiddlewares`.
6. Опция `WithMetricsEnabled(bool)` / env `METRICS_ENABLED`, `PPROF_ENABLED` (pprof = net/http/pprof, выключен по умолчанию).

## Безопасность и конвенции
- **Никаких** логирований тела; query-параметры с именами `token`/`idempotency` не логировать.
- `/debug/pprof` — только при `PPROF_ENABLED=true`.

## Приёмка / DOD
- `GET /health` → `200 ok` без внешних зависимостей; `GET /ready` без зарегистрированных чекков → `200`.
- Middleware по порядку: panic в обработчике → `500` + access-лог с request-id, процесс жив.
- `GET /metrics` отдаёт `parke_http_requests_total` после запроса на бизнес-роут.
- `example/` (Фаза 7) на этой фазе может пока зарегистрировать «ping»-роут для ручной проверки.

## Тесты
- `middleware_test.go` (recover, request-id roundtrip, access-log без тела).
- `health_test.go` (ready 200/503 по чеккам).
- `httpserver_test.go` (метод-паттерны, встроенные эндпоинты).
- `metrics_test.go` (метрики инкрементятся per route/status).