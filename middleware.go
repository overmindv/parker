package parker

import (
	"context"

	"github.com/overmindv/parker/internal/requestid"
)

// RequestHeader — заголовок, из которого берётся/в который кладётся request_id.
const RequestHeader = requestid.Header

// RequestIDFrom возвращает request_id из контекста (пусто, если не задан).
func RequestIDFrom(ctx context.Context) string {
	return requestid.From(ctx)
}
