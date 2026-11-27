package parker

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetrics_MiddlewareObservesAndExposes(t *testing.T) {
	t.Parallel()

	// Полная связка App: бизнес-роут + /metrics, который должен показывать метрику.
	app := newTestApp(t) // DefaultOptions: MetricsEnabled=true
	if app.Metrics() == nil {
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
	app.state.Handler().ServeHTTP(rec, req)
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
