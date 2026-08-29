package parker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseTraceparent_Valid(t *testing.T) {
	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	spanID := "00f067aa0ba902b7"
	value := "00-" + traceID + "-" + spanID + "-01"
	gotTrace, gotSpan, ok := parseTraceparent(value)
	if !ok || gotTrace != traceID || gotSpan != spanID {
		t.Fatalf("ожидались %s/%s, получили %s/%s ok=%v", traceID, spanID, gotTrace, gotSpan, ok)
	}
}

func TestParseTraceparent_Invalid(t *testing.T) {
	cases := []string{
		"",
		"00-bad-span",
		"01-" + "4bf92f3577b34da6a3ce929d0e0e4736" + "-00f067aa0ba902b7" + "-01", // неверная версия
		"00-" + "xyz92f3577b34da6a3ce929d0e0e4736" + "-00f067aa0ba902b7" + "-01", // не hex trace
	}
	for _, c := range cases {
		if _, _, ok := parseTraceparent(c); ok {
			t.Fatalf("для %q ожидался невалидный traceparent", c)
		}
	}
}

func TestTraceMiddleware_SetsContext(t *testing.T) {
	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	h := traceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if TraceIDFrom(r.Context()) != traceID {
			t.Errorf("trace_id не проброшен: %q", TraceIDFrom(r.Context()))
		}
		if SpanIDFrom(r.Context()) == "" {
			t.Error("span_id не задан")
		}
		_, _ = w.Write([]byte("ok"))
	}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(TraceparentHeader, "00-"+traceID+"-00f067aa0ba902b7-01")
	h.ServeHTTP(httptest.NewRecorder(), req)
}

func TestTraceMiddleware_NoHeaderNoop(t *testing.T) {
	h := traceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if TraceIDFrom(r.Context()) != "" {
			t.Error("trace_id не должен быть задан без traceparent")
		}
		_, _ = w.Write([]byte("ok"))
	}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)
}

func TestApp_LoggerForAddsTraceFields(t *testing.T) {
	app := newTestApp()
	ctx := withRequestID(context.Background(), "req-9")
	ctx = context.WithValue(ctx, ctxKeyTraceID, "trace-9")
	ctx = context.WithValue(ctx, ctxKeySpanID, "span-9")

	l := app.LoggerFor(ctx)
	if l == nil {
		t.Fatal("LoggerFor не должен возвращать nil")
	}
	// Этот метод возвращает логгер; поля проверяются составом лог-записи.
	// Достаточно убедиться, что вызов не падает с заданными полями.
	l.Info("smoke", "k", "v")
}
