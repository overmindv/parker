//go:build component

package parker

import (
	"context"
	"testing"
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
