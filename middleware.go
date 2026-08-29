package parker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

type contextKey uint8

const (
	ctxKeyRequestID contextKey = iota
	ctxKeyTraceID
	ctxKeySpanID
)

// RequestHeader — заголовок, из которого берётся/в который кладётся request_id.
const RequestHeader = "X-Request-ID"

// TraceparentHeader — W3C заголовок трассировки (traceparent).
const TraceparentHeader = "traceparent"

// RequestIDFrom возвращает request_id из контекста (пусто, если не задан).
func RequestIDFrom(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKeyRequestID).(string); ok {
		return id
	}
	return ""
}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

// TraceIDFrom возвращает trace_id из контекста (пусто, если не задан).
func TraceIDFrom(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKeyTraceID).(string); ok {
		return id
	}
	return ""
}

// SpanIDFrom возвращает span_id из контекста (пусто, если не задан).
func SpanIDFrom(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKeySpanID).(string); ok {
		return id
	}
	return ""
}

// recoverMiddleware ловит panic в обработчике, логирует stack и отдаёт 500,
// не «роняя» процесс.
func recoverMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic в обработчике",
						"panic", fmt.Sprint(rec),
						"request_id", RequestIDFrom(r.Context()),
						"stack", string(debug.Stack()),
					)
					http.Error(w, "internal server error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// requestIDMiddleware обеспечивает сквозной request_id: берёт из заголовка или
// генерирует, кладёт в контекст и в ответ.
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestHeader)
		if id == "" {
			id = newRequestID()
		}
		w.Header().Set(RequestHeader, id)
		next.ServeHTTP(w, r.WithContext(withRequestID(r.Context(), id)))
	})
}

// newRequestID генерирует случайный 32-символьный hex-идентификатор.
func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand не падает практически никогда; на всякий случай — fallback.
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// traceMiddleware разбирает W3C traceparent и кладёт trace_id/span_id в контекст
// (для дальнейшей трассировки и полей логов). Не генерирует новый trace — только
// пробрасывает входящий; генерация/экспорт — задача будущего OTel-интегратора.
func traceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if traceID, spanID, ok := parseTraceparent(r.Header.Get(TraceparentHeader)); ok {
			ctx = context.WithValue(ctx, ctxKeyTraceID, traceID)
			ctx = context.WithValue(ctx, ctxKeySpanID, spanID)
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

// parseTraceparent разбирает W3C traceparent: "version-traceid-spanid-flags".
func parseTraceparent(value string) (traceID, spanID string, ok bool) {
	parts := strings.Split(value, "-")
	if len(parts) != 4 {
		return "", "", false
	}
	if parts[0] != "00" { // version (пока поддерживается только 00)
		return "", "", false
	}
	if len(parts[1]) != 32 || !isHex(parts[1]) { // trace-id
		return "", "", false
	}
	if len(parts[2]) != 16 || !isHex(parts[2]) { // span-id
		return "", "", false
	}
	return parts[1], parts[2], true
}

// isHex проверяет, что строка состоит только из hex-цифр.
func isHex(s string) bool {
	for _, r := range s {
		ok := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
		if !ok {
			return false
		}
	}
	return true
}

// accessLogMiddleware логирует метод/путь/статус/длительность без тела запроса.
// Оборачивается вокруг recorder'а, чтобы знать фактический код ответа.
func accessLogMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			ctx := r.Context()
			log.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", RequestIDFrom(ctx),
				"trace_id", TraceIDFrom(ctx),
				"span_id", SpanIDFrom(ctx),
			)
		})
	}
}

// statusRecorder запоминает код ответа для access-лога и метрик.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.status = http.StatusOK
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(b)
}

// statusRecorder также реализует http.Flusher и http.Hijacker, если обёрнутый writer их поддерживает.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
