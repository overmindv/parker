package outbox

import (
	"time"

	pg "github.com/go-jet/jet/v2/postgres"
	"github.com/overmindv/parker/internal/adapter/postgres/generated/table"
)

// Таблицы принадлежат сервису, который выбирает схему через search_path своего пула.
var (
	queue = table.Outbox.FromSchema("")
	audit = table.OutboxReplayAudit.FromSchema("")

	pending    = pg.Text("pending")
	publishing = pg.Text("publishing")
	exhausted  = pg.Text("exhausted")
	// Константа в предикате позволяет generic plan использовать partial indexes для sent/unsent.
	sent = pg.RawString("'sent'")
)

func claimPending(limit int) pg.Statement {
	ready := pg.CTE("ready")
	due := queue.Status.EQ(pending).AND(queue.NextAttemptAt.LT_EQ(pg.NOW()))
	expired := queue.Status.EQ(publishing).AND(queue.LeaseUntil.LT_EQ(pg.NOW()))
	selectReady := pg.SELECT(queue.ID).FROM(queue).
		WHERE(due.OR(expired)).
		ORDER_BY(queue.CreatedAt.ASC(), queue.ID.ASC()).
		LIMIT(int64(limit)).FOR(pg.UPDATE().SKIP_LOCKED())
	claim := queue.UPDATE().SET(
		queue.Status.SET(publishing),
		queue.ClaimToken.SET(pg.StringExp(pg.Func("gen_random_uuid"))),
		queue.LeaseUntil.SET(pg.NOW().ADD(pg.INTERVAL(30, pg.SECOND))),
	).FROM(ready).WHERE(queue.ID.EQ(queue.ID.From(ready))).
		RETURNING(queue.ID, queue.Topic, queue.MessageKey, queue.MessageValue, queue.ClaimToken, queue.LeaseUntil, queue.Attempts)

	return pg.WITH(ready.AS(selectReady))(claim)
}

func liveClaim(rec Record) pg.BoolExpression {
	return queue.ID.EQ(uuidParam(rec.ID)).
		AND(queue.ClaimToken.EQ(uuidParam(rec.ClaimToken))).
		AND(queue.Status.EQ(publishing)).
		AND(queue.LeaseUntil.GT(pg.NOW()))
}

func uuidParam(value string) pg.StringExpression {
	return pg.CAST(pg.Text(value)).AS_UUID()
}

func markClaimSent(rec Record) pg.Statement {
	return queue.UPDATE().SET(
		queue.Status.SET(sent),
		queue.SentAt.SET(pg.NOW()),
		queue.ClaimToken.SET(pg.StringExp(pg.NULL)),
		queue.LeaseUntil.SET(pg.TimestampzExp(pg.NULL)),
		queue.Attempts.SET(queue.Attempts.ADD(pg.Int(1))),
	).WHERE(liveClaim(rec))
}

func markSent(ids []string) pg.Statement {
	// У Jet нет конструктора uuid[]; один параметр сохраняет поддержку больших batch.
	batch := pg.ArrayExp[pg.StringExpression](pg.CAST(pg.StringArray(ids...)).AS("uuid[]"))

	return queue.UPDATE().SET(
		queue.Status.SET(sent),
		queue.SentAt.SET(pg.NOW()),
		queue.Attempts.SET(queue.Attempts.ADD(pg.Int(1))),
	).WHERE(queue.ID.EQ(pg.ANY(batch)).AND(queue.Status.EQ(pending)).AND(queue.ClaimToken.IS_NULL()))
}

func markClaimFailed(rec Record, next time.Time) pg.Statement {
	attempts := queue.Attempts.ADD(pg.Int(1))
	status := pg.CASE().WHEN(attempts.GT_EQ(pg.Int(maxAttempts))).THEN(exhausted).ELSE(pending)

	return queue.UPDATE().SET(
		queue.Attempts.SET(attempts),
		queue.Status.SET(pg.StringExp(status)),
		queue.NextAttemptAt.SET(pg.TimestampzT(next)),
		queue.ClaimToken.SET(pg.StringExp(pg.NULL)),
		queue.LeaseUntil.SET(pg.TimestampzExp(pg.NULL)),
	).WHERE(liveClaim(rec))
}

func replayExhausted(id string, generation int, actor string) pg.Statement {
	replayed := pg.CTE("replayed")
	replay := queue.UPDATE().SET(
		queue.Status.SET(pending),
		queue.Attempts.SET(pg.Int(0)),
		queue.NextAttemptAt.SET(pg.NOW()),
		queue.ClaimToken.SET(pg.StringExp(pg.NULL)),
		queue.LeaseUntil.SET(pg.TimestampzExp(pg.NULL)),
		queue.ReplayGeneration.SET(queue.ReplayGeneration.ADD(pg.Int(1))),
	).WHERE(queue.ID.EQ(uuidParam(id)).AND(queue.Status.EQ(exhausted)).AND(queue.ReplayGeneration.EQ(pg.Int(int64(generation))))).
		RETURNING(queue.ID, queue.ReplayGeneration)
	recordAudit := audit.INSERT(audit.OutboxID, audit.Generation, audit.ActorID).
		QUERY(pg.SELECT(queue.ID.From(replayed), queue.ReplayGeneration.From(replayed), uuidParam(actor)).FROM(replayed))

	return pg.WITH(replayed.AS(replay))(recordAudit)
}

func queueAge() pg.Statement {
	// Метод Timestampz.SUB принимает interval. PostgreSQL также вычитает две метки времени.
	elapsed := pg.CustomExpression(pg.NOW(), pg.Token("-"), pg.MIN(queue.CreatedAt))
	seconds := pg.EXTRACT(pg.EPOCH, elapsed)

	return pg.SELECT(pg.CAST(pg.COALESCE(pg.GREATEST(seconds, pg.Int(0)), pg.Int(0))).AS_DOUBLE()).
		FROM(queue).WHERE(queue.Status.NOT_EQ(sent))
}

func cleanupSent(before time.Time, limit int) pg.Statement {
	stale := pg.CTE("stale")
	selectStale := pg.SELECT(queue.ID).FROM(queue).
		WHERE(queue.Status.EQ(sent).AND(queue.SentAt.LT(pg.TimestampzT(before)))).
		ORDER_BY(queue.SentAt.ASC()).LIMIT(int64(limit)).FOR(pg.UPDATE().SKIP_LOCKED())
	remove := queue.DELETE().WHERE(queue.ID.IN(pg.SELECT(queue.ID.From(stale)).FROM(stale)))

	return pg.WITH(stale.AS(selectStale))(remove)
}
