# Parker Infrastructure Boundaries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Логически разделить инфраструктурную реализацию Parker и обновить принадлежащие Александру сервисы на стабильный релиз.

**Architecture:** Корневой пакет остаётся совместимым фасадом. Реализация env, health и logging перемещается в `internal/*`; сервисы обновляются независимо после устранения release prerequisites.

**Tech Stack:** Go 1.26, Go modules, Make.

**Spec:** `docs/superpowers/specs/2026-11-23-parker-infrastructure-boundaries-design.md`

## Global Constraints

- Не создавать сетевой runtime Parker.
- Не переносить бизнес-логику в Parker.
- Не менять task-hunter и сервисы Леонида.

## Review Focus

- Сохранены сигнатуры корневого пакета.
- Env parsing и readiness errors не изменились.
- Сервисы не получили `replace` или pseudo-version.
- Каждая версия сервиса обновляется независимо.
- Dependency warning о retracted transitive lz4 явно зафиксирован.

---

### Task 1: Внутренние пакеты Parker

**Files:** `internal/env/env.go`, `internal/health/registry.go`, `internal/logging/logger.go`, корневые фасады и тесты.

- [x] Добавить падающий structural test для ожидаемых пакетов.
- [x] Вынести реализации, сохранив публичные функции и типы.
- [x] Выполнить `go test -race ./...`.

### Task 2: Документировать архитектурную границу

**Files:** `docs/architecture.md`, `README.md`.

- [x] Описать принадлежность инфраструктуры и бизнес-логики.
- [x] Зафиксировать отсутствие сетевой точки отказа и независимые версии.

### Task 3: Обновить потребителей

**Files:** `../users/go.mod`, `../tasks/go.mod`, `../recommendations/go.mod` и go.sum.

- [x] Проверить обновление users, tasks и recommendations до v0.2.0 и откатить небезопасные bumps.
- [x] Подготовить исправление conditional Kafka TLS validation: production без Kafka стартует, Kafka client без TLS отклоняется.
- [ ] Для tasks добавить owner migrations lease/retry, TLS consumer config и component test; до этого оставить v0.1.0.
- [x] Выполнить build/test затронутых сервисов; lint диагностирован отдельно как несовместимость stale toolchain.
- [ ] После выпуска совместимого Parker обновить и отправить каждый сервис отдельным изменением.
