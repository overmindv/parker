package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const traceFixture = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

type benchWriter http.Header

func (w benchWriter) Header() http.Header       { return http.Header(w) }
func (benchWriter) WriteHeader(int)             {}
func (benchWriter) Write(b []byte) (int, error) { return len(b), nil }

func BenchmarkTraceHTTPRecording(b *testing.B) {
	for _, sampled := range []bool{false, true} {
		name := "unsampled"
		sampler := sdktrace.NeverSample()
		if sampled {
			name = "sampled"
			sampler = sdktrace.AlwaysSample()
		}
		b.Run(name, func(b *testing.B) {
			provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
			b.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			tracing := &Runtime{tracer: provider.Tracer("benchmark")}
			mux := http.NewServeMux()
			mux.HandleFunc("GET /work/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
			h := tracing.ServerMiddleware(mux)
			r := httptest.NewRequest(http.MethodGet, "/work/example", nil)
			r.Header.Set(TraceparentHeader, traceFixture)
			w := make(benchWriter)
			b.ReportAllocs()
			for b.Loop() {
				h.ServeHTTP(w, r)
			}
		})
	}
}
