// Package requestid хранит request_id в контексте запроса.
package requestid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Header — заголовок, из которого берётся/в который кладётся request_id.
const Header = "X-Request-ID"

type contextKey uint8

const ctxKey contextKey = iota

// From возвращает request_id из контекста (пусто, если не задан).
func From(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKey).(string); ok {
		return id
	}

	return ""
}

// With возвращает контекст с заданным request_id.
func With(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey, id)
}

// New генерирует случайный 32-символьный hex-идентификатор.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand не падает практически никогда; на всякий случай — fallback.
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b[:])
}
