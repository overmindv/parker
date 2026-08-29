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

	ids := make([]string, 0, len(records))
	for _, rec := range records {
		ids = append(ids, rec.ID)
	}
	if err := o.MarkSent(ctx, ids); err != nil {
		t.Fatal(err)
	}

	records, err = o.FetchPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("после MarkSent не должно быть pending-записей, получили %d", len(records))
	}
}
