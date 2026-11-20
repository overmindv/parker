package parker

import (
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsRegister(t *testing.T) {
	app := newTestApp()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "fixture_pending", Help: "Pending fixture."})
	gauge.Set(42)

	if err := app.Metrics().Register(gauge); err != nil {
		t.Fatal(err)
	}
	if err := app.Metrics().Register(gauge); err == nil {
		t.Fatal("duplicate metric registered")
	}
	rec := serveRequest(t, app, http.MethodGet, "/metrics")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "fixture_pending 42\n") {
		t.Fatal(rec.Code, rec.Body.String())
	}
}
