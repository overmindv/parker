# Reliability release v0.2.0

OVM-181/182/183/184/186 включены в v0.2.0. Выпуск — OVM-187.
[Release notes и процедура](../releases/v0.2.0.md) описывают несовместимости поведения,
миграции стандартного PgOutbox, порядок запуска и rollback. Контракт custom Outbox
сохранён; собственные adapters должны отдельно внедрить lease/retry semantics.

Gate: main CI → annotated semver tag → release workflow → go get точного тега в
изолированный generated consumer → GitHub Release. Production deployment отдельно.
