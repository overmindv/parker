# 09 — Тестирование parker и сгенерированного сервиса

## Два уровня тестов

1. **Тесты самого parker** (в `overmindv/parker`) — проверяют фреймворк.
2. **Сгенерированный сервис + `example/`** — интеграционные, проверяют, что каркас реально
   «поднимается с нуля» против локальной БД/kafka.

Общее правило: юнит/логика без внешних зависимостей — `go test -race ./...`; всё, что требует
postgres/kafka/файловой системы — component-тесты с тегом `component` (как в `tasks`).

## 1. Юнит-тесты parker (без Docker)

| Пакет | Что тестируем |
|---|---|
| `config` | env-хелперы, дефолты, валидация (перенести из `tasks/internal/config/config_test.go`) |
| `log` | уровни, JSON-формат |
| `lifecycle` | старт/стоп воркера, fatal-остановка, graceful shutdown по сигналу, shutdown-timeout |
| `middleware` | recover (500+лог+жив), request-id roundtrip, access-log без тела, traceparent→контекст |
| `health` | Ready 200/503 по чеккам, порядок, первый failure |
| `metrics` | инкремент по route/status, path-нормализация (`/tasks/{id}` vs `/tasks/123`) |
| `init` | генерация: состав файлов, содержимое main.go/go.mod(replace), имена, идемпотентность |

## 2. Component-тесты parker + `example/`

Исполняются в CI parker и локально (`make ctest` с поднятой БД). Требуемые сервисы:
- **PostgreSQL 17** — через `postgres:17-alpine` service (как в `tasks/.github/workflows/ci.yml`).
- **Kafka** — для Фаз 4/7; локально через `infra` compose (`make up`), в CI — Redis нет, только
  Kafka можно поднять `bitnami/kafka:3` service или тест-контейнер.

Тест-сценарии:
1. `migrate up` применяет `example/migrations/*.sql` к чистой БД; `status` корректен; `down` откатывает.
2. `example/` сервис: `parker serve` → `/health` 200, `/ready` 200 (с БД), `/metrics` отдаёт метрики.
3. Outbox: бизнес-запись + outbox-запись одной транзакцией → dispatcher публикует в kafka →
   subscriber получает; дублей нет; при выключенной kafka — бэкофф, не «сгорает» процесс.
4. Производитель/подписчик roundtrip по ключу (порядок в partition).

Env для component-тестов parker:
```bash
PARKER_TEST_DSN='postgres://postgres:postgres@localhost:5432/parker_test?sslmode=disable'
PARKER_TEST_KAFKA='localhost:29092'   # если применимо
```

## 3. Интеграционное тестирование в `infra` (сквозное)

Когда новый сервис добавлен в `infra/docker-compose.yml` и проходит `/ready`, он попадает в
общий сценарий `overmindv/infra` (`make integration`) и e2e `overmindv/tests` — это уже
ответственность **infra/tests**, не parker, но parker гарантирует, что сгенерированный сервис
отвечает по контракту `/health`,`/ready`,`/metrics` и корректно стартует в compose.

## 4. Тест генератора end-to-end

`init_test.go` (component, тег `component`):
1. создать temp-dir → `parker init demo --no-input --pg --kafka`;
2. `go mod tidy` внутри demo (замена parker => ../parker должна резолвиться);
3. `go build ./...` и `go vet ./...` внутри demo — кодопоставка компилируется;
4. `make test` внутри demo — сгенерированные тесты проходят.

Это ловит «сломанный шаблон» до того, как пользователь дойдёт до Части 4/5.

## CI‑джобы parker (`.github/workflows/ci.yml`)

```
lint       — golangci-lint (v2.x)
test       — go test -race ./...            (без Docker)
build      — make build
component  — services: postgres:17 (+ опц. kafka) → make ctest PARKER_TEST_DSN=...
```
`e2e`-делегирования у самого parker нет (это фреймворк, не сервис), но он должен проверять
сквозной `example/`.

## DOD (глобальный, для всего паркера)

- Все CI-джобы паркера зелёные.
- `parker init demo` в temp-dir → компилируется, поднимается против postgres (`/health`,`/ready`,`/metrics`).
- `example/` проходит сквозной сценарий «HTTP → usecase → outbox → kafka → consumer» (если --kafka).
- Сгенерированный compose-блок копируется в `infra` без правок и сервис стартует в стеке.