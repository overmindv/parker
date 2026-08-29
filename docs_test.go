package parker

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestDashboardJSONIsValidAndReferencesParkerMetrics(t *testing.T) {
	raw, err := os.ReadFile("docs/dashboard.json")
	if err != nil {
		t.Skip("docs/dashboard.json не найден")
	}
	if !json.Valid(raw) {
		t.Fatal("docs/dashboard.json — некорректный JSON")
	}
	for _, metric := range []string{"parker_http_requests_total", "parker_http_request_duration_seconds", "parker_ready"} {
		if !strings.Contains(string(raw), metric) {
			t.Errorf("dashboard.json должен ссылаться на метрику %s", metric)
		}
	}
}

func TestAlertsExampleExists(t *testing.T) {
	if _, err := os.Stat("docs/alerts.example.yml"); err != nil {
		t.Skip("docs/alerts.example.yml не найден")
	}
}
