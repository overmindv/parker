# 07 — Фаза 6: CLI `parker init` (шаблоны, скаффолд сервиса)

## Что делаем
Реализуем `parker init <service-name>` — генератор **тонкого каркаса** прикладного сервиса,
в котором уже подключён parker и остаётся писать только бизнес-логику. Сгенерированный сервис
самодостаточен для локального запуска и для вставки в `infra/docker-compose.yml`.

## Как запускается
Целевой сценарий (после клонирования пустого репозитория `overmindv/<svc>`):
```
cd <репо>/../parker
go build -o bin/parker ./cmd/parker
cd ../<svc>            # пустой git-репозиторий
../parker/bin/parker init <service-name>
# печатает: «скопируй этот блок в infra/docker-compose.yml ...», «дальше: make dev»
```
Также `parker init` может запускаться из любого места как `go run github.com/overmindv/parker/cmd/parker init ...`, но из-за `replace` в scaffold проще локальный `bin/parker`.

## Аргументы/флаги
```
parker init <service-name> [flags]
  --pg        интерактив или --no-pg: нужен ли postgres (default: спросить; fallback true)
  --kafka     нужен ли kafka/outbox (default false)
  --migrate   добавить migrations/ + migrate-таргет (по умолчанию true, если --pg)
  --ci        сгенерировать .github/workflows/ci.yml (default true)
  --compose   напечатать compose-блок (default true)
  --no-input  взять дефолты (для CI/скриптов)
```
`<service-name>` валидируется: нижний регистр, `[a-z0-9][a-z0-9-]*`, не длиннее 40 символов.

## Что генерируется (go:embed из `template/service/`)

```
<svc>/
├── go.mod                       # module github.com/overmindv/<svc>; require parker; replace => ../parker
├── .env.example
├── .gitignore  .gitattributes
├── README.md                    (со ссылкой на этот workflow)
├── Makefile                     (run/build/test/lint/migrate-up -> parker migrate)
├── Dockerfile                   (мультистейдж; COPY cmd/<svc>; ENTRYPOINT ["<svc>"])
├── cmd/<svc>/main.go            (parker.Main(run, WithAppName("<svc>")) [+ WithKafka if --kafka])
├── internal/
│   ├── pkg/
│   │   ├── domain/              (заглушка + README «сюда бизнес-типы»)
│   │   ├── usecase/             (заглушка)
│   │   └── store/               (заглушка: interface + postgres adapter на pgx)
│   └── app/                     (httpapi: example-контроллер + регистрация роутов на app.HTTP())
├── migrations/                  (0001_init.sql — пустой/пример; только при --pg)
└── .github/workflows/ci.yml     (lint/test/build/component + e2e-delegate на overmindv/tests)
```

### Сгенерированный `main.go` (эталон — «только бизнес-логика»)
```go
package main

import (
    "github.com/overmindv/parker"
    mysvc "github.com/overmindv/<svc>/internal/<svc>"
)

func main() { parker.Main(run, parker.WithAppName("<svc>")) }

func run(app *parker.App) error {
    pg, err := app.Postgres()                     // pool + /ready чек автоматически
    if err != nil { return err }

    repo := mysvc.NewRepository(pg)
    uc   := mysvc.NewUsecase(repo, app.Logger())

    h := mysvc.NewHTTP(uc, app.Logger())
    app.HTTP().Handle("GET /ping", h.Ping)         // бизнес-роут (пример)

    app.AddRunnable("outbox", /* по желанию */)
    return nil
}
```

### Сгенерированный `Makefile`
```make
run:        go run ./cmd/<svc>
build:      go build ./...
test:       go test -race ./...
lint:       golangci-lint run
migrate-up: go run ./cmd/<svc> migrate --dir migrations up
migrate-down: go run ./cmd/<svc> migrate --dir migrations down
```

### Сгенерированный обрезок `Dockerfile`
```dockerfile
FROM golang:1.26-alpine AS build
ARG GOPROXY ; ENV GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}
WORKDIR /src
COPY go.mod go.sum ./
# GOFLAGS=-mod=mod из-за replace => ../parker: НЕ копировать соседний parker, а:
# скачать parker (или vendor), см. нюанс ниже.
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/<svc> ./cmd/<svc>

FROM alpine:3.23
RUN adduser -S <svc> ; WORKDIR /app
COPY --from=build /out/<svc> /usr/local/bin/<svc>
COPY migrations /app/migrations
USER <svc>
EXPOSE 8080
ENTRYPOINT ["<svc>"]
```

> **Нюанс Docker + `replace ../parker`.** В контейнере `../parker` нет. Два варианта:
> 1. **Подтягивать parker по тегу** (рекомендуется для прод): в `Dockerfile` перед `COPY`
>    выполнить `go mod download github.com/overmindv/parker` (тег `vX.Y.Z` на `overmindv/parker`,
>    версия — из go.sum; `replace` при сборке в контейнере снят).
> 2. **Vendor** сервиса: `go mod vendor`, репозиторий содержит `vendor/`.
> Сгенерированный Dockerfile по умолчанию использует вариант 1 (паркер публикуется, например,
> тег `vX.Y.Z` на `overmindv/parker`); локальная разработка — по прежнему через `replace` + `make run`.

## Compose-блок (печатается в конце)

`parker init --compose` в конце печатает готовый блок для вставки в `infra/docker-compose.yml`
(по образцу `tasks` из `infra/docker-compose.yml:228`):
```yaml
  <svc>-postgres:
    image: postgres:17-alpine
    environment: { POSTGRES_DB: <svc>, POSTGRES_USER: <svc>, POSTGRES_PASSWORD: "${SVC_POSTGRES_PASSWORD?set in .env}" }
    volumes: [ <svc>-postgres-data:/var/lib/postgresql/data ]
    healthcheck: { test: ["CMD-SHELL","pg_isready -U <svc> -d <svc>"], interval: 5s, retries: 20 }

  <svc>-migrate:                                  # (только если --pg)
    build:
      context: ../<svc>
      args: { GOPROXY: "${GOPROXY:-https://proxy.golang.org,direct}" }
    entrypoint: ["<svc>", "migrate", "--dir", "migrations", "up"]
    environment:
      DATABASE_URL: "postgres://<svc>:${SVC_POSTGRES_PASSWORD}@<svc>-postgres:5432/<svc>?sslmode=disable"

  <svc>:
    build:
      context: ../<svc>
      args: { GOPROXY: "${GOPROXY:-https://proxy.golang.org,direct}" }
    environment:
      SERVICE_NAME: <svc>
      HTTP_ADDR: ":8080"
      ENV: local
      DATABASE_URL: "postgres://<svc>:${SVC_POSTGRES_PASSWORD}@<svc>-postgres:5432/<svc>?sslmode=disable"
      KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"        # (если --kafka)
    depends_on:
      <svc>-migrate: { condition: service_completed_successfully }
    healthcheck:
      test: ["CMD-SHELL","wget -qO- http://127.0.0.1:8080/ready >/dev/null"]
      interval: 5s; timeout: 3s; retries: 20; start_period: 5s
    restart: unless-stopped
```
В `infra/.env.example` и `infra/docker-compose.yml` добавить `SVC_POSTGRES_PASSWORD=__GENERATE__`
(и, при `--kafka`, топики в сервис `kafka-topics`). Паркер печатает и эти правки.

## Файл за файлом: задачи Фазы 6

1. `template/service/` — go:embed шаблоны (`main.go.tmpl`, `Makefile.tmpl`, `Dockerfile.tmpl`,
   `ci.yml.tmpl`, `env.example.tmpl`, `README.tmpl`, заглушки internal/*).
2. `cmd/parker/internal/init` — парсинг аргументов, валидация имени, копирование с подстановкой
   `{{.ServiceName}}`, walk по каталогу, `go mod tidy` сгенерированного сервиса (вызов
   `go mod tidy` в новом каталоге), печать compose-блока и постинт-чеклиста.
3. Генерация go.mod: `module github.com/overmindv/<svc>`, `require github.com/overmindv/parker v0.0.0`,
   `replace github.com/overmindv/parker => ../parker`.
4. Печать постинт-чеклиста «Части 1–5» (см. `08_service_bootstrap.md`).
5. `--no-input` + юнит-тесты на генерацию (снимки файлов).

## Приёмка / DOD
- В пустом каталоге `parker init demo --no-input --pg --kafka` создаёт полный сервис,
  `cd demo && make run` поднимает его против локальной БД; `/health`,`/ready`,`/metrics` работают.
- Сгенерированный `main.go` идентичен эталону выше (только бизнес-логика).
- compose-блок копируется в `infra/docker-compose.yml` без правок — сервис стартует в стеке.
- Тесты генератора: идемпотентность, валидность имён, корректность go.mod/replace.

## Тесты
- `init_test.go`: temp-dir, проверить набор файлов, содержимое main.go, go.mod (replace).
- `init_name_test.go`: некорректные имена отклоняются.
- Component: в temp-dir сгенерировать demo, `go build ./...` и `make test` внутри него (см. `09_testing.md`).