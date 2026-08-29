package parker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

type fakeOutbox struct {
	pending []OutboxRecord
	sent    []string
	limit   int
}

func (f *fakeOutbox) FetchPending(_ context.Context, limit int) ([]OutboxRecord, error) {
	f.limit = limit
	return f.pending, nil
}

func (f *fakeOutbox) MarkSent(_ context.Context, ids []string) error {
	f.sent = append(f.sent, ids...)
	return nil
}

type fakePublisher struct {
	published []string // keys
	failTopic string
}

func (f *fakePublisher) Publish(_ context.Context, topic, key string, _ []byte) error {
	if f.failTopic != "" && topic == f.failTopic {
		return errors.New("publish failed")
	}
	f.published = append(f.published, key)
	return nil
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestOutboxDispatcher_FlushPublishesAndMarks(t *testing.T) {
	outbox := &fakeOutbox{pending: []OutboxRecord{
		{ID: "1", Topic: "t", Key: "k1"},
		{ID: "2", Topic: "t", Key: "k2"},
	}}
	pub := &fakePublisher{}

	d := newOutboxDispatcher(outbox, pub, 0, 100, discardLog())
	d.flushOnce(context.Background())

	if len(pub.published) != 2 {
		t.Fatalf("ожидалась публикация 2 записей, опубликовано %d", len(pub.published))
	}
	if len(outbox.sent) != 2 || outbox.sent[0] != "1" || outbox.sent[1] != "2" {
		t.Fatalf("ожидались sent=[1 2], получили %v", outbox.sent)
	}
}

func TestOutboxDispatcher_PublishFailureNotMarked(t *testing.T) {
	outbox := &fakeOutbox{pending: []OutboxRecord{
		{ID: "1", Topic: "bad", Key: "k1"},
		{ID: "2", Topic: "good", Key: "k2"},
	}}
	pub := &fakePublisher{failTopic: "bad"}

	d := newOutboxDispatcher(outbox, pub, 0, 100, discardLog())
	d.flushOnce(context.Background())

	// Только успешная запись отправлена и помечена.
	if len(pub.published) != 1 || pub.published[0] != "k2" {
		t.Fatalf("ожидалась публикация только k2, получили %v", pub.published)
	}
	if len(outbox.sent) != 1 || outbox.sent[0] != "2" {
		t.Fatalf("ожидались sent=[2], получили %v", outbox.sent)
	}
}

func TestOutboxDispatcher_EmptyNoOp(t *testing.T) {
	outbox := &fakeOutbox{}
	pub := &fakePublisher{}
	d := newOutboxDispatcher(outbox, pub, 0, 100, discardLog())
	d.flushOnce(context.Background())
	if len(pub.published) != 0 || len(outbox.sent) != 0 {
		t.Fatal("при пустой очереди не должно быть публикаций")
	}
}
