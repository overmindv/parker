package observability

import "net/http"

// StatusRecorder запоминает код ответа для access-лога, метрик и трейсинга.
type StatusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

// NewStatusRecorder оборачивает writer и считает статус по умолчанию 200.
func NewStatusRecorder(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{ResponseWriter: w, status: http.StatusOK}
}

// Unwrap возвращает исходный writer для http.ResponseController.
func (r *StatusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// WriteHeader запоминает первый записанный статус.
func (r *StatusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

// Write записывает тело и фиксирует неявный статус 200.
func (r *StatusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.status = http.StatusOK
		r.wroteHeader = true
	}

	return r.ResponseWriter.Write(b)
}

// Flush пробрасывает Flush, если обёрнутый writer его поддерживает.
func (r *StatusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Status возвращает записанный код ответа.
func (r *StatusRecorder) Status() int { return r.status }

// WroteHeader сообщает, был ли записан заголовок.
func (r *StatusRecorder) WroteHeader() bool { return r.wroteHeader }
