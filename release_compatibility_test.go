package parker

import (
	"context"
	"testing"
)

// Старые сервисные adapters сохраняют минимальный контракт FetchPending/MarkSent.
func TestReleasePreservesLegacyOutboxAdapter(t *testing.T) {
	t.Parallel()

	var adapter Outbox = &fakeOutbox{pending: []OutboxRecord{{ID: "legacy", Topic: "t", Key: "k", Value: []byte("payload")}}}
	publisher := &fakePublisher{}
	newOutboxDispatcher(adapter, publisher, 0, 1, discardLog()).flushOnce(context.Background())
	if len(publisher.published) != 1 || len(adapter.(*fakeOutbox).sent) != 1 {
		t.Fatal("legacy adapter incompatible")
	}
}
