package outbox

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

const maxAttempts = 8

func retryAt(now time.Time, attempt int, jitter float64) time.Time {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 9 {
		attempt = 9
	}
	delay := time.Second * time.Duration(1<<uint(attempt-1))
	if delay > 150*time.Second {
		delay = 150 * time.Second
	}
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}

	return now.Add(delay + time.Duration(float64(delay)*jitter))
}

// MarkClaimFailed сохраняет расписание, не сохраняя raw broker errors или payload.
func (o *PgOutbox) MarkClaimFailed(ctx context.Context, rec Record) error {
	next := retryAt(o.now(), rec.Attempts+1, rand.Float64()) //nolint:gosec // jitter не используется для криптографии
	tag, err := o.pool.Exec(ctx, `UPDATE outbox SET attempts=attempts+1,
 status=CASE WHEN attempts+1 >= $3 THEN 'exhausted' ELSE 'pending' END,
 next_attempt_at=$4,claim_token=NULL,lease_until=NULL
 WHERE id=$1 AND claim_token=$2 AND status='publishing' AND lease_until>now()`, rec.ID, rec.ClaimToken, maxAttempts, next)
	if err != nil {
		return fmt.Errorf("outbox reschedule: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox claim lost")
	}

	return nil
}

// ReplayExhausted вызывается владельцем после проверки доверенного admin actor.
// Generation исключает повтор старого запроса после нового исчерпания бюджета.
func (o *PgOutbox) ReplayExhausted(ctx context.Context, id string, generation int, actor string, admin bool) error {
	if !admin || actor == "" {
		return fmt.Errorf("administrator required")
	}
	tag, err := o.pool.Exec(ctx, `WITH replayed AS (
 UPDATE outbox SET status='pending',attempts=0,next_attempt_at=now(),claim_token=NULL,lease_until=NULL,replay_generation=replay_generation+1
 WHERE id=$1 AND status='exhausted' AND replay_generation=$2 RETURNING id,replay_generation
 ) INSERT INTO outbox_replay_audit(outbox_id,generation,actor_id) SELECT id,replay_generation,$3::uuid FROM replayed`, id, generation, actor)
	if err != nil {
		return fmt.Errorf("outbox replay: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("outbox replay conflict")
	}

	return nil
}

// QueueAge — возраст старейшего неотправленного события, включая exhausted.
func (o *PgOutbox) QueueAge(ctx context.Context) (time.Duration, error) {
	var seconds float64
	err := o.pool.QueryRow(ctx, `SELECT COALESCE(GREATEST(EXTRACT(EPOCH FROM now()-min(created_at)),0),0)::float8 FROM outbox WHERE status<>'sent'`).Scan(&seconds)

	return time.Duration(seconds * float64(time.Second)), err
}

// CleanupSent удаляет только старые sent небольшим batch, не трогая pending/exhausted.
func (o *PgOutbox) CleanupSent(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	if retention < 24*time.Hour {
		return 0, fmt.Errorf("retention must be at least 24 hours")
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	tag, err := o.pool.Exec(ctx, `WITH stale AS (SELECT id FROM outbox WHERE status='sent' AND sent_at<$1 ORDER BY sent_at LIMIT $2 FOR UPDATE SKIP LOCKED)
 DELETE FROM outbox WHERE id IN (SELECT id FROM stale)`, o.now().Add(-retention), limit)

	return tag.RowsAffected(), err
}
