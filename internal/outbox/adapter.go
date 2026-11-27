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

// Эти возможности PgOutbox диспетчер использует только при их совместной поддержке.
type capabilities[R ~recordFields] interface {
	MarkClaimSent(context.Context, R) error
	MarkClaimFailed(context.Context, R) error
	QueueAge(context.Context) (time.Duration, error)
	CleanupSent(context.Context, time.Duration, int) (int64, error)
}

type adapter[R ~recordFields] struct{ store[R] }

func (a adapter[R]) FetchPending(ctx context.Context, limit int) ([]Record, error) {
	return recordsAs[Record](a.store.FetchPending(ctx, limit))
}

type fullAdapter[R ~recordFields] struct {
	adapter[R]
	capabilities[R]
}

func (a fullAdapter[R]) MarkClaimSent(ctx context.Context, record Record) error {
	return a.capabilities.MarkClaimSent(ctx, R(record))
}

func (a fullAdapter[R]) MarkClaimFailed(ctx context.Context, record Record) error {
	return a.capabilities.MarkClaimFailed(ctx, R(record))
}

// Adapt сохраняет опциональные возможности публичного хранилища для диспетчера.
func Adapt[R ~recordFields](s store[R]) Outbox {
	base := adapter[R]{s}
	if full, ok := s.(capabilities[R]); ok {
		return fullAdapter[R]{base, full}
	}

	return base
}

// FetchPendingAs возвращает записи PgOutbox в публичном типе.
func FetchPendingAs[R ~recordFields](s *PgOutbox, ctx context.Context, limit int) ([]R, error) {
	return recordsAs[R](s.FetchPending(ctx, limit))
}

func recordsAs[To, From ~recordFields](records []From, err error) ([]To, error) {
	if err != nil {
		return nil, err
	}

	return convert.Slice(records, func(r From) To { return To(r) }), nil
}
