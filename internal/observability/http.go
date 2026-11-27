package observability

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ServerMiddleware извлекает входящий traceparent и создаёт серверный спан.
func (t *Runtime) ServerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if headers := r.Header.Values(TraceparentHeader); len(headers) == 1 {
			ctx = ExtractTraceparent(ctx, headers[0])
			if t == nil {
				r = r.WithContext(ctx)
			}
		}
		if t == nil {
			next.ServeHTTP(w, r)

			return
		}
		method := traceHTTPMethod(r.Method)
		ctx, span := t.tracer.Start(ctx, "HTTP "+method, trace.WithSpanKind(trace.SpanKindServer))
		r = r.WithContext(ctx)
		if !span.IsRecording() {
			next.ServeHTTP(w, r)

			return
		}
		rec := NewStatusRecorder(w)
		finished := false
		defer func() {
			status := rec.Status()
			if !finished && !rec.WroteHeader() {
				status = http.StatusInternalServerError
			}
			span.SetAttributes(attribute.String("http.request.method", method), attribute.Int("http.response.status_code", status))
			if route := traceRoute(r.Pattern); route != "" {
				span.SetAttributes(attribute.String("http.route", route))
			}
			if !finished || status >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, "server_error")
			}
			span.End()
		}()
		next.ServeHTTP(rec, r)
		finished = true
	})
}

func traceRoute(pattern string) string {
	if i := strings.IndexByte(pattern, '/'); i >= 0 && len(pattern)-i <= 128 {
		return pattern[i:]
	}

	return ""
}

func traceHTTPMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	default:
		return "_OTHER"
	}
}

// HTTPTransport передает traceparent во внутренние HTTP-вызовы.
// Для внешних адресов используйте обычный транспорт. nil означает http.DefaultTransport.
func HTTPTransport(rt *Runtime, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &traceTransport{base: base, tracing: rt}
}

type traceTransport struct {
	base    http.RoundTripper
	tracing *Runtime
}

func (t *traceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Response != nil {
		// Не передаем трассу на адрес, выбранный чужим HTTP-ответом.
		r = r.Clone(r.Context())
		clearTraceHeaders(r.Header)

		return t.base.RoundTrip(r)
	}
	ctx := r.Context()
	var span trace.Span
	var method string
	if t.tracing != nil {
		method = traceHTTPMethod(r.Method)
		ctx, span = t.tracing.tracer.Start(ctx, "HTTP "+method, trace.WithSpanKind(trace.SpanKindClient))
		defer span.End()
	}
	if parent := TraceparentFrom(ctx); parent != "" {
		r = r.Clone(ctx)
		if r.Header == nil {
			r.Header = make(http.Header)
		}
		clearTraceHeaders(r.Header)
		r.Header.Set(TraceparentHeader, parent)
	}
	resp, err := t.base.RoundTrip(r)
	if span != nil && span.IsRecording() {
		span.SetAttributes(attribute.String("http.request.method", method))
		if resp != nil {
			span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
		}
		if err != nil || (resp != nil && resp.StatusCode >= http.StatusBadRequest) {
			span.SetStatus(codes.Error, "client_error")
		}
	}

	return resp, err
}

func clearTraceHeaders(headers http.Header) {
	for key := range headers {
		if strings.EqualFold(key, TraceparentHeader) || strings.EqualFold(key, "tracestate") || strings.EqualFold(key, "baggage") {
			delete(headers, key)
		}
	}
}

func (t *traceTransport) CloseIdleConnections() {
	if base, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		base.CloseIdleConnections()
	}
}
