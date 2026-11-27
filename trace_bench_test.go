package parker

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

type benchWriter http.Header

func (w benchWriter) Header() http.Header       { return http.Header(w) }
func (benchWriter) WriteHeader(int)             {}
func (benchWriter) Write(b []byte) (int, error) { return len(b), nil }

func BenchmarkTraceHTTPWithoutExport(b *testing.B) {
	for _, name := range []string{"no-parent", "parent"} {
		b.Run(name, func(b *testing.B) {
			opts := DefaultOptions()
			opts.MetricsEnabled = false
			a := configuredTestApp(b, opts, slog.New(slog.DiscardHandler))
			a.HTTP().HandleFunc("GET /work/{id}", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
			r := httptest.NewRequest(http.MethodGet, "/work/example", nil)
			r.Header.Set(RequestHeader, "bench")
			if name == "parent" {
				r.Header.Set(TraceparentHeader, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			}
			w := make(benchWriter)
			b.ReportAllocs()
			for b.Loop() {
				a.state.Handler().ServeHTTP(w, r)
			}
		})
	}
}
