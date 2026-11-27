package outbox

import (
	"context"
	"time"

	"github.com/overmindv/parker/internal/convert"
)

// store принимает записи публичного типа, включая реализации из сервисов.
type store[R ~recordFields] interface {
	FetchPending(context.Context, int) ([]R, error)
	MarkSent(context.Context, []string) error
}

// queueMaintenance объединяет операции обслуживания, которые диспетчер использует вместе.
type queueMaintenance interface {
	QueueAge(context.Context) (time.Duration, error)
	CleanupSent(context.Context, time.Duration, int) (int64, error)
}

type adapter[R ~recordFields] struct{ store[R] }

func (a adapter[R]) FetchPending(ctx context.Context, limit int) ([]Record, error) {
	return recordsAs[Record](a.store.FetchPending(ctx, limit))
}

type dispatcherStore struct {
	Outbox
	markClaimSent   func(context.Context, Record) error
	markClaimFailed func(context.Context, Record) error
	maintenance     queueMaintenance
}

// adapt независимо находит возможности исходного хранилища до преобразования типа записи.
func adapt[R ~recordFields](s store[R]) dispatcherStore {
	d := dispatcherStore{Outbox: adapter[R]{s}}
	if claimed, ok := s.(interface {
		MarkClaimSent(context.Context, R) error
	}); ok {
		d.markClaimSent = func(ctx context.Context, r Record) error { return claimed.MarkClaimSent(ctx, R(r)) }
	}
	if retry, ok := s.(interface {
		MarkClaimFailed(context.Context, R) error
	}); ok {
		d.markClaimFailed = func(ctx context.Context, r Record) error { return retry.MarkClaimFailed(ctx, R(r)) }
	}
	d.maintenance, _ = s.(queueMaintenance)

	return d
}

// FetchPendingAs возвращает записи PgOutbox в публичном типе.
func FetchPendingAs[R ~recordFields](s *PgOutbox, ctx context.Context, limit int) ([]R, error) {
	return recordsAs[R](s.FetchPending(ctx, limit))
}

func recordsAs[To, From ~recordFields](records []From, err error) ([]To, error) {
	if err != nil || records == nil {
		return nil, err
	}

	return convert.Slice(records, func(r From) To { return To(r) }), nil
}
