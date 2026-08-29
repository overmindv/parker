package parker

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNormalizeRoute(t *testing.T) {
	cases := map[string]string{
		"/health":       "/health",
		"/tasks":        "/tasks",
		"/tasks/123":    "/tasks/{id}",
		"/tasks/42/sub": "/tasks/{id}/sub",
		"/users/550e8400-e29b-41d4-a716-446655440000": "/users/{id}",
	}
	for in, want := range cases {
		if got := normalizeRoute(in); got != want {
			t.Errorf("normalizeRoute(%q) = %q, ожидалось %q", in, got, want)
		}
	}
}

func TestLooksLikeID(t *testing.T) {
	if !looksLikeID("123") || !looksLikeID("550e8400-e29b-41d4-a716-446655440000") {
		t.Fatal("число и UUID должны распознаваться как id")
	}
	if looksLikeID("alpha") || looksLikeID("") {
		t.Fatal("строка/пустота не должны распознаваться как id")
	}
}

func TestMetrics_MiddlewareObservesAndExposes(t *testing.T) {
	// Полная связка App: бизнес-роут + /metrics, который должен показывать метрику.
	app := newTestApp() // DefaultOptions: MetricsEnabled=true
	if app.metrics == nil {
		t.Fatal("метрики должны быть включены в тестовом App")
	}
	app.HTTP().HandleFunc("GET /tasks/123", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Запрос на бизнес-роут (ид-сегмент нормализуется).
	if rec := serveRequest(t, app, http.MethodGet, "/tasks/123"); rec.Code != http.StatusOK {
		t.Fatalf("/tasks/123: ожидался 200, получили %d", rec.Code)
	}

	// /metrics отдаёт метрику запросов.
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	app.server.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: ожидался 200, получили %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Result().Body)
	out := string(body)
	if !strings.Contains(out, `parker_http_requests_total`) {
		t.Fatalf("/metrics должен содержать parker_http_requests_total, получили:\n%s", out)
	}
	if !strings.Contains(out, `route="/tasks/{id}"`) {
		t.Fatalf("метрика должна содержать нормализованный route=/tasks/{id}, получили:\n%s", out)
	}
	if !strings.Contains(out, `parker_http_requests_total{method="GET",route="/tasks/{id}",service="parker",status="200"} 1`) {
		t.Fatalf("метрика должна показывать 1 запрос по route=/tasks/{id}, получили:\n%s", out)
	}
}
