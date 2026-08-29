package parker

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func bufferLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestRequestIDMiddleware_Generated(t *testing.T) {
	h := requestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := RequestIDFrom(r.Context())
		if id == "" {
			t.Error("request_id должен быть в контексте")
		}
		_, _ = w.Write([]byte(id))
	}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get(RequestHeader); got == "" {
		t.Fatal("ответ должен содержать X-Request-ID")
	}
	if len(rec.Header().Get(RequestHeader)) != 32 {
		t.Fatalf("ожидался 32-символьный id, получили %q", rec.Header().Get(RequestHeader))
	}
}

func TestRequestIDMiddleware_PreservesIncoming(t *testing.T) {
	h := requestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(RequestIDFrom(r.Context())))
	}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(RequestHeader, "incoming-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get(RequestHeader); got != "incoming-123" {
		t.Fatalf("ожидался incoming-123, получили %q", got)
	}
}

func TestRecoverMiddleware_Returns500AndStaysAlive(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := bufferLogger(buf)
	h := recoverMiddleware(logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("kaboom")
	}))
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	req.Header.Set(RequestHeader, "req-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("ожидался 500, получили %d", rec.Code)
	}
	if !strings.Contains(buf.String(), "kaboom") {
		t.Fatalf("лог должен содержать panic-сообщение, получили %q", buf.String())
	}
}

func TestAccessLogMiddleware_LogsStatus(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := bufferLogger(buf)
	h := accessLogMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	req := httptest.NewRequest(http.MethodGet, "/created", nil)
	ctx := withRequestID(context.Background(), "req-log")
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out := buf.String()
	if !strings.Contains(out, "http request") {
		t.Fatalf("ожидался access-лог, получили %q", out)
	}
	if !strings.Contains(out, "status=201") {
		t.Fatalf("access-лог должен содержать status=201, получили %q", out)
	}
}
