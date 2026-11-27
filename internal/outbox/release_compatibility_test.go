package outbox

import (
	"context"
	"testing"
)

// Старые сервисные adapters сохраняют минимальный контракт FetchPending/MarkSent.
func TestReleasePreservesLegacyOutboxAdapter(t *testing.T) {
	t.Parallel()

	var adapter Outbox = &fakeOutbox{pending: []Record{{ID: "legacy", Topic: "t", Key: "k", Value: []byte("payload")}}}
	publisher := &fakePublisher{}
	NewDispatcherWithPublisher(adapter, publisher, 0, 1, discardLog()).flushOnce(context.Background())
	if len(publisher.published) != 1 || len(adapter.(*fakeOutbox).sent) != 1 {
		t.Fatal("legacy adapter incompatible")
	}
}
