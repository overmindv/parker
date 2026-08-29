# 06 — Фаза 5: Наблюдаемость (метрики, trace-контекст, алерты-заглушки)

## Что делаем
Завершаем наблюдаемость parker: метрики запросов уже есть (Фаза 2), добавляем trace-контекст
и документацию для будущих Grafana/алертов. Цель — из коробки «логи + метрики + /ready + trace»
без внешних платформ, плюс явные точки, куда воткнуть платформенные интеграции, когда появятся.

## Метрики (уже заложены в Фазе 2)
- `parker_http_requests_total{service,method,path,status}`
- `parker_http_request_duration_seconds{service,method,path}` (histogram)
- `/metrics` через `promhttp.Handler()`, включается `METRICS_ENABLED` (default true).

`example/` и сгенерированные сервисы должны давать измеримые метрики без доработки.

## Trace / request-id
- Middleware request-id из Фазы 2 кладёт `X-Request-ID` в контекст и в ответ.
- **Trace-контекст**: принимать W3C `traceparent` (`trace-id`/`span-id`) из заголовка и класть в
  контекст/лог. Логи получают поля `request_id`, `trace_id`, `span_id`.
- **OpenTelemetry — за флагом** (`OTEL_ENABLED=false` по умолчанию): если будет нужен export,
  обернуть HTTP через `otelhttp` и добавить OTLP-exporter. На этой фазе — только контекст-поля,
  без OTel-зависимости (лёгкость).

Помощники:
```go
// parker/middleware.go
func RequestIDFrom(ctx) string
func TraceIDFrom(ctx) string
// LoggerFor(ctx) — логгер с request_id/trace_id/span_id
```

## Логи (JSON)
- Формат: `{"time":..., "level":..., "msg":..., ...поля}`.
- access-лог: метод, путь, статус, длительность, request_id, trace_id — без тела.

## Dashboards и алерты (сейчас — только документируем)

Платформенного Grafana/алертинга в проекте ещё нет. Поэтому на этой фазе:

1. `docs/dashboard.json` в parker (и/или в `example/`) — **пример Grafana-дашборды** для сервиса,
   построенный на метриках `parker_*` (RPS, p50/p95/p99 latency, error rate, /ready-статус).
   Дашборда не «работает», пока нет Grafana; файл — готовый импорт-артефакт на будущее.
2. `docs/alerts.example.yml` — **пример алертов** (Prometheus-правила): 
   - `HighErrorRate`: `parker_http_requests_total{status=~"5.."}` доля высока;
   - `ServiceDown`: `/ready` → 0 в течение N минут;
   - `KafkaPublishingStuck`: outbox pending не убывает.
   Подключение при появлении алертинга.
3. Каждый сгенерированный сервис (в `08_service_bootstrap`, Часть 4) получает шаги
   «проверь логи/тренды», «импортируй dashboard», «заведи алерты» — с пометками, что реально
   выполнимо сейчас (логи/метрики) и что отложено до платформы.

## Файл за файлом: задачи Фазы 5

1. `middleware.go` — доработка: W3C traceparent → контекст → лог-поля.
2. `metrics.go` — убедиться, что histogram/бары корректно бьются по `path` (без кардинальности
   query-параметров; пути нормализовать, если используются `{id}`).
3. Создать `docs/dashboard.json`, `docs/alerts.example.yml`.
4. `slogx.LoggerFor(ctx)` — сборка логгера с полями контекста.
5. Экспорт HealthRegistry-метрик (`/ready` 200/503) — чтобы алерт `ServiceDown` был возможен.

## Приёмка / DOD
- Лог запроса содержит `request_id` и `trace_id` (если incoming `traceparent`).
- `/metrics` отдаёт latency-гистограмму и error-rate; путь нормализован.
- `docs/dashboard.json` валиден (JSON) и консистентен с названиями метрик.
- `example/` показывает: double request → видно в /metrics и в логах.

## Тесты
- `middleware_test.go`: traceparent парсится/копируется в контекст и лог.
- `metrics_test.go`: path-нормализация (`/tasks/{id}` vs `/tasks/123` не плодит серии).
- `dashboard.json` — schema-чек лёгким парсером (просто JSON-валиден).