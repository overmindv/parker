package outbox

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/parker/internal/observability"
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
	ageChecks       int
	retention       time.Duration
	cleanupLimit    int
}

func (s *leasedServiceStore) MarkClaimSent(_ context.Context, r serviceRecord) error {
	s.claimed = r

	return nil
}

func (s *leasedServiceStore) MarkClaimFailed(_ context.Context, r serviceRecord) error {
	s.failed = r

	return nil
}

func (s *leasedServiceStore) QueueAge(context.Context) (time.Duration, error) {
	s.ageChecks++

	return time.Minute, nil
}
func (s *leasedServiceStore) CleanupSent(_ context.Context, retention time.Duration, limit int) (int64, error) {
	s.retention = retention
	s.cleanupLimit = limit

	return 1, nil
}

func TestAdaptPreservesOptionalLeaseOperations(t *testing.T) {
	t.Parallel()

	rec := serviceRecord{ID: "id", Topic: "topic", Key: "key", Value: []byte("value"), ClaimToken: "claim", LeaseUntil: time.Now(), Attempts: 2}
	legacy := &serviceStore{pending: []serviceRecord{rec}}
	base := adapt(legacy)
	if base.markClaimSent != nil {
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
	full := adapt(store)
	if full.markClaimSent == nil || full.markClaimFailed == nil || full.maintenance == nil {
		t.Fatal("хранилище потеряло операции lease и обслуживания очереди")
	}
	if err := full.markClaimSent(t.Context(), Record(rec)); err != nil || !reflect.DeepEqual(store.claimed, rec) {
		t.Fatal("неверная запись подтверждения lease", err)
	}
	if err := full.markClaimFailed(t.Context(), Record(rec)); err != nil || !reflect.DeepEqual(store.failed, rec) {
		t.Fatal("неверная запись ошибки lease", err)
	}
}

func TestDispatcherPreservesIndependentStoreCapabilities(t *testing.T) {
	t.Parallel()

	type ack interface {
		MarkClaimSent(context.Context, serviceRecord) error
	}
	type retry interface {
		MarkClaimFailed(context.Context, serviceRecord) error
	}
	type maintenance interface {
		QueueAge(context.Context) (time.Duration, error)
		CleanupSent(context.Context, time.Duration, int) (int64, error)
	}

	for _, name := range []string{"legacy", "ack", "retry", "maintenance", "ack+retry", "ack+maintenance", "retry+maintenance", "ack+retry+maintenance"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			okRecord := serviceRecord{ID: "success", Topic: "ok", Key: "k1", Value: []byte("data"), ClaimToken: "claim-one", LeaseUntil: time.Now().Add(time.Minute)}
			failedRecord := serviceRecord{ID: "failure", Topic: "bad", Key: "k2", Value: []byte("retry"), ClaimToken: "claim-two", Attempts: 2}
			s := &leasedServiceStore{serviceStore: serviceStore{pending: []serviceRecord{okRecord, failedRecord}}}
			base := &s.serviceStore
			stores := map[string]store[serviceRecord]{
				"legacy": base,
				"ack": struct {
					*serviceStore
					ack
				}{base, s},
				"retry": struct {
					*serviceStore
					retry
				}{base, s},
				"maintenance": struct {
					*serviceStore
					maintenance
				}{base, s},
				"ack+retry": struct {
					*serviceStore
					ack
					retry
				}{base, s, s},
				"ack+maintenance": struct {
					*serviceStore
					ack
					maintenance
				}{base, s, s},
				"retry+maintenance": struct {
					*serviceStore
					retry
					maintenance
				}{base, s, s},
				"ack+retry+maintenance": s,
			}
			pub := &fakePublisher{failTopic: failedRecord.Topic}
			d := NewDispatcherWithPublisher(stores[name], pub, 0, 100, discardLog())
			d.metrics = observability.NewMetrics(true)
			d.flushOnce(t.Context())

			if !slices.Equal(pub.published, []string{okRecord.Key}) {
				t.Fatal("должна публиковаться только успешная запись")
			}
			if strings.Contains(name, "ack") {
				if !reflect.DeepEqual(s.claimed, okRecord) || len(s.sent) != 0 {
					t.Error("публикация должна подтверждаться исходной записью с claim token")
				}
			} else if !slices.Equal(s.sent, []string{okRecord.ID}) {
				t.Error("для базового хранилища ожидается пакетный MarkSent")
			}
			if strings.Contains(name, "retry") {
				if !reflect.DeepEqual(s.failed, failedRecord) {
					t.Error("ошибка публикации должна передавать исходную запись в MarkClaimFailed")
				}
			} else if s.failed.ID != "" {
				t.Error("неожиданный retry")
			}
			if strings.Contains(name, "maintenance") {
				if s.ageChecks != 1 || s.retention != 7*24*time.Hour || s.cleanupLimit != 100 {
					t.Error("диспетчер должен измерить возраст очереди и выполнить очистку")
				}
			} else if s.ageChecks != 0 || s.cleanupLimit != 0 {
				t.Error("неожиданное обслуживание очереди")
			}
		})
	}
}
