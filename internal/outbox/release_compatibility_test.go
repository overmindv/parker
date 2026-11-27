package outbox

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"
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

type serviceRecord Record

type serviceStore struct {
	pending []serviceRecord
	sent    []string
}

func (s *serviceStore) FetchPending(context.Context, int) ([]serviceRecord, error) {
	return s.pending, nil
}

func (s *serviceStore) MarkSent(_ context.Context, ids []string) error {
	s.sent = append(s.sent, ids...)

	return nil
}

type leasedServiceStore struct {
	serviceStore
	claimed, failed serviceRecord
}

func (s *leasedServiceStore) MarkClaimSent(_ context.Context, r serviceRecord) error {
	s.claimed = r

	return nil
}

func (s *leasedServiceStore) MarkClaimFailed(_ context.Context, r serviceRecord) error {
	s.failed = r

	return nil
}

func (*leasedServiceStore) QueueAge(context.Context) (time.Duration, error) { return time.Minute, nil }
func (*leasedServiceStore) CleanupSent(context.Context, time.Duration, int) (int64, error) {
	return 1, nil
}

func TestAdaptPreservesOptionalLeaseOperations(t *testing.T) {
	t.Parallel()

	rec := serviceRecord{ID: "id", Topic: "topic", Key: "key", Value: []byte("value"), ClaimToken: "claim", LeaseUntil: time.Now(), Attempts: 2}
	legacy := &serviceStore{pending: []serviceRecord{rec}}
	base := Adapt(legacy)
	if _, ok := base.(claimedOutbox); ok {
		t.Fatal("legacy-хранилище получило лишний метод MarkClaimSent")
	}
	records, err := base.FetchPending(t.Context(), 1)
	if err != nil || len(records) != 1 || !reflect.DeepEqual(records[0], Record(rec)) {
		t.Fatal("адаптер потерял поля записи", err)
	}
	if err := base.MarkSent(t.Context(), []string{rec.ID}); err != nil || !slices.Equal(legacy.sent, []string{rec.ID}) {
		t.Fatal("адаптер не подтвердил запись в legacy-хранилище", err)
	}

	store := &leasedServiceStore{}
	full, ok := Adapt(store).(capabilities[Record])
	if !ok {
		t.Fatal("хранилище потеряло операции lease и обслуживания очереди")
	}
	if err := full.MarkClaimSent(t.Context(), Record(rec)); err != nil || !reflect.DeepEqual(store.claimed, rec) {
		t.Fatal("неверная запись подтверждения lease", err)
	}
	if err := full.MarkClaimFailed(t.Context(), Record(rec)); err != nil || !reflect.DeepEqual(store.failed, rec) {
		t.Fatal("неверная запись ошибки lease", err)
	}
}
