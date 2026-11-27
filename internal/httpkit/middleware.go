package httpkit

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/overmindv/parker/internal/observability"
	"github.com/overmindv/parker/internal/requestid"
)

// RecoverMiddleware ловит panic в обработчике, логирует stack и отдаёт 500,
// не «роняя» процесс.
func RecoverMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic в обработчике",
						"panic", fmt.Sprint(rec),
						"request_id", requestid.From(r.Context()),
						"stack", string(debug.Stack()),
					)
					http.Error(w, "internal server error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RequestIDMiddleware обеспечивает сквозной request_id: берёт из заголовка или
// генерирует, кладёт в контекст и в ответ.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestid.Header)
		if id == "" {
			id = requestid.New()
		}
		w.Header().Set(requestid.Header, id)
		next.ServeHTTP(w, r.WithContext(requestid.With(r.Context(), id)))
	})
}

// AccessLogMiddleware логирует метод/путь/статус/длительность без тела запроса.
func AccessLogMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := observability.NewStatusRecorder(w)
			next.ServeHTTP(rec, r)
			ctx := r.Context()
			log.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", requestid.From(ctx),
				"trace_id", observability.TraceIDFrom(ctx),
				"span_id", observability.SpanIDFrom(ctx),
			)
		})
	}
}
