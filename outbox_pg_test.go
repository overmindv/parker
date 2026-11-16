//go:build component

package parker

import (
	"context"
	"testing"
	"time"
)

func TestPgOutbox_FetchAndMark(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	pool, err := OpenPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, OutboxSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM outbox`); err != nil {
		t.Fatal(err)
	}

	o := NewPgOutbox(pool)
	// Вставляем две записи напрямую.
	if _, err := pool.Exec(ctx,
		`INSERT INTO outbox (topic, message_key, message_value) VALUES ('t','k1','v1'),('t','k2','v2')`); err != nil {
		t.Fatal(err)
	}

	records, err := o.FetchPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("ожидались 2 pending-записи, получили %d", len(records))
	}

	for _, rec := range records {
		if err := o.MarkClaimSent(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}

	records, err = o.FetchPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("после MarkSent не должно быть pending-записей, получили %d", len(records))
	}
}

func TestPgOutboxExclusiveLeaseAndStaleAck(t *testing.T) {
	ctx := context.Background()
	pool, err := OpenPool(ctx, testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err = pool.Exec(ctx, OutboxSchema); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `DELETE FROM outbox; INSERT INTO outbox(topic,message_key,message_value) VALUES('t','k','v')`); err != nil {
		t.Fatal(err)
	}
	first := NewPgOutbox(pool)
	second := NewPgOutbox(pool)
	results := make(chan []OutboxRecord, 2)
	errs := make(chan error, 2)
	for _, o := range []*PgOutbox{first, second} {
		go func(o *PgOutbox) { r, e := o.FetchPending(ctx, 1); results <- r; errs <- e }(o)
	}
	a, b := <-results, <-results
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if len(a)+len(b) != 1 {
		t.Fatalf("duplicate lease %d+%d", len(a), len(b))
	}
	old := append(a, b...)[0]
	// Имитация crash после broker ack, до mark; срок lease истекает.
	if _, err = pool.Exec(ctx, `UPDATE outbox SET lease_until=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	renewed, err := second.FetchPending(ctx, 1)
	if err != nil || len(renewed) != 1 {
		t.Fatalf("recovery %v %v", renewed, err)
	}
	if renewed[0].ClaimToken == old.ClaimToken {
		t.Fatal("token reused")
	}
	if err = first.MarkClaimSent(ctx, old); err == nil {
		t.Fatal("stale owner acknowledged")
	}
	if err = second.MarkClaimSent(ctx, renewed[0]); err != nil {
		t.Fatal(err)
	}
}

func TestPgOutboxRetryExhaustionReplayAndRetention(t *testing.T) {
	ctx := context.Background()
	pool, err := OpenPool(ctx, testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err = pool.Exec(ctx, OutboxSchema); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `DELETE FROM outbox; INSERT INTO outbox(topic,message_key,message_value,created_at) VALUES('t','k','secret-payload',now()-interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	o := NewPgOutbox(pool)
	var id string
	for attempt := 1; attempt <= outboxMaxAttempts; attempt++ {
		rows, err := o.FetchPending(ctx, 1)
		if err != nil || len(rows) != 1 {
			t.Fatalf("claim %d: %v %v", attempt, rows, err)
		}
		id = rows[0].ID
		if err = o.MarkClaimFailed(ctx, rows[0]); err != nil {
			t.Fatal(err)
		}
		if err = o.MarkClaimFailed(ctx, rows[0]); err == nil {
			t.Fatal("stale failure accepted")
		}
		early, err := o.FetchPending(ctx, 1)
		if err != nil || len(early) != 0 {
			t.Fatalf("hot retry %v %v", early, err)
		}
		var count int
		var status string
		var next time.Time
		if err = pool.QueryRow(ctx, `SELECT attempts,status,next_attempt_at FROM outbox WHERE id=$1`, id).Scan(&count, &status, &next); err != nil {
			t.Fatal(err)
		}
		if count != attempt || !next.After(time.Now()) {
			t.Fatalf("schedule %d %s", count, next)
		}
		if attempt == outboxMaxAttempts && status != "exhausted" {
			t.Fatal("not exhausted")
		}
		if _, err = pool.Exec(ctx, `UPDATE outbox SET next_attempt_at=now()-interval '1 second' WHERE id=$1`, id); err != nil {
			t.Fatal(err)
		}
	}
	age, err := o.QueueAge(ctx)
	if err != nil || age < time.Hour {
		t.Fatalf("queue age %s %v", age, err)
	}
	actor := "11111111-1111-4111-8111-111111111111"
	if err = o.ReplayExhausted(ctx, id, 0, actor, false); err == nil {
		t.Fatal("nonadmin replay")
	}
	if err = o.ReplayExhausted(ctx, id, 0, actor, true); err != nil {
		t.Fatal(err)
	}
	if err = o.ReplayExhausted(ctx, id, 0, actor, true); err == nil {
		t.Fatal("duplicate replay")
	}
	var audits int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_replay_audit WHERE outbox_id=$1`, id).Scan(&audits); err != nil || audits != 1 {
		t.Fatalf("audit %d %v", audits, err)
	}
	rows, err := o.FetchPending(ctx, 1)
	if err != nil || len(rows) != 1 || rows[0].Attempts != 0 || string(rows[0].Value) != "secret-payload" {
		t.Fatalf("replay %v %v", rows, err)
	}
	if err = o.MarkClaimSent(ctx, rows[0]); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE outbox SET sent_at=now()-interval '10 days' WHERE id=$1;`, id); err != nil {
		t.Fatal(err)
	}
	n, err := o.CleanupSent(ctx, 7*24*time.Hour, 1)
	if err != nil || n != 1 {
		t.Fatalf("cleanup %d %v", n, err)
	}
}
