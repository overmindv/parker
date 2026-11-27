package httpkit

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/overmindv/parker/internal/requestid"
)

func bufferLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestRequestIDMiddleware_Generated(t *testing.T) {
	t.Parallel()

	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := requestid.From(r.Context())
		if id == "" {
			t.Error("request_id должен быть в контексте")
		}
		_, _ = w.Write([]byte(id))
	}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get(requestid.Header); got == "" {
		t.Fatal("ответ должен содержать X-Request-ID")
	}
	if len(rec.Header().Get(requestid.Header)) != 32 {
		t.Fatalf("ожидался 32-символьный id, получили %q", rec.Header().Get(requestid.Header))
	}
}

func TestRequestIDMiddleware_PreservesIncoming(t *testing.T) {
	t.Parallel()

	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(requestid.From(r.Context())))
	}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(requestid.Header, "incoming-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get(requestid.Header); got != "incoming-123" {
		t.Fatalf("ожидался incoming-123, получили %q", got)
	}
}

func TestRecoverMiddleware_Returns500AndStaysAlive(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	logger := bufferLogger(buf)
	h := RecoverMiddleware(logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("kaboom")
	}))
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	req.Header.Set(requestid.Header, "req-1")
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
	t.Parallel()

	buf := &bytes.Buffer{}
	logger := bufferLogger(buf)
	h := AccessLogMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	req := httptest.NewRequest(http.MethodGet, "/created", nil)
	ctx := requestid.With(context.Background(), "req-log")
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
