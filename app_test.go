package parker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestApp() *App {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	app := &App{opts: DefaultOptions(), log: logger, health: NewHealthRegistry()}
	app.initServer()
	return app
}

func TestApp_PostgresWithoutDSN_ReturnsError(t *testing.T) {
	app := newTestApp() // DefaultOptions: DatabaseURL=""
	if _, err := app.Postgres(); err == nil {
		t.Fatal("Postgres() без DATABASE_URL должна вернуть ошибку")
	}
}

// TestApp_AccessLogContainsRequestID проверяет, что request_id попадает в access-лог
// сквозь всю middleware-цепочку (регресс: requestID должен быть снаружи accessLog).
func TestApp_AccessLogContainsRequestID(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	app := &App{opts: DefaultOptions(), log: logger, health: NewHealthRegistry()}
	app.initServer()

	app.HTTP().HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})
	_ = serveRequest(t, app, http.MethodGet, "/ping")

	if !strings.Contains(buf.String(), "request_id=") {
		t.Fatalf("access-лог должен содержать request_id, получили %q", buf.String())
	}
}

func serveRequest(t *testing.T, app *App, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	app.server.Handler.ServeHTTP(rec, req)
	return rec
}

func TestApp_HealthEndpoint(t *testing.T) {
	app := newTestApp()
	if rec := serveRequest(t, app, http.MethodGet, "/health"); rec.Code != http.StatusOK {
		t.Fatalf("/health: ожидался 200, получили %d", rec.Code)
	}
}

func TestApp_ReadyEndpointEmptyOk(t *testing.T) {
	app := newTestApp()
	if rec := serveRequest(t, app, http.MethodGet, "/ready"); rec.Code != http.StatusOK {
		t.Fatalf("/ready без чекков: ожидался 200, получили %d", rec.Code)
	}
}

func TestApp_ReadyEndpointFailsWithCheck(t *testing.T) {
	app := newTestApp()
	app.AddHealthCheck("db", HealthCheckFunc(func(context.Context) error {
		return errors.New("down")
	}))
	if rec := serveRequest(t, app, http.MethodGet, "/ready"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/ready с упавшим чеком: ожидался 503, получили %d", rec.Code)
	}
}

func TestApp_RegisterBusinessRoute(t *testing.T) {
	app := newTestApp()
	app.HTTP().HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})
	rec := serveRequest(t, app, http.MethodGet, "/ping")
	if rec.Code != http.StatusOK {
		t.Fatalf("/ping: ожидался 200, получили %d", rec.Code)
	}
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, rec.Result().Body)
	if !strings.Contains(buf.String(), "pong") {
		t.Fatalf("ожидался pong, получили %q", buf.String())
	}
}
