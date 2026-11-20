package parker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type contextKey uint8

const ctxKeyRequestID contextKey = iota

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
	if id := trace.SpanContextFromContext(ctx).TraceID(); id.IsValid() {
		return id.String()
	}

	return ""
}

// SpanIDFrom возвращает span_id из контекста (пусто, если не задан).
func SpanIDFrom(ctx context.Context) string {
	if id := trace.SpanContextFromContext(ctx).SpanID(); id.IsValid() {
		return id.String()
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

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

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
