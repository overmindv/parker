package parker

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type collectorFixture struct {
	mu    sync.Mutex
	spans []*tracepb.Span
	wire  []byte
}

func newCollectorFixture(t *testing.T) (string, *collectorFixture) {
	t.Helper()
	capture := &collectorFixture{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != traceExportPath || r.Method != http.MethodPost ||
			r.Header.Get("Content-Type") != "application/x-protobuf" || r.Header.Get(TraceparentHeader) != "" {
			t.Error("wrong OTLP/HTTP request")
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxOTLPRequest+1))
		var message collectorpb.ExportTraceServiceRequest
		if err != nil || len(body) > maxOTLPRequest || proto.Unmarshal(body, &message) != nil {
			t.Error("collector received invalid protobuf")
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		capture.mu.Lock()
		capture.wire = append(capture.wire, body...)
		for _, resources := range message.GetResourceSpans() {
			for _, scope := range resources.GetScopeSpans() {
				capture.spans = append(capture.spans, scope.GetSpans()...)
			}
		}
		capture.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	return server.URL, capture
}

func (c *collectorFixture) snapshot() ([]*tracepb.Span, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.spans), slices.Clone(c.wire)
}

func tracedApp(t *testing.T, endpoint, service string, ratio float64) *App {
	t.Helper()
	opts := DefaultOptions()
	opts.Tracing = TraceOptions{Endpoint: endpoint, SampleRatio: ratio}
	opts.ServiceName = service
	opts.MetricsEnabled = false
	log := slog.New(slog.DiscardHandler)
	tracing, err := newTraceRuntime(opts, log)
	if err != nil || tracing == nil {
		t.Fatal("cannot create trace provider", err)
	}
	t.Cleanup(func() { tracing.shutdown(context.Background()) })
	a := &App{opts: opts, log: log, health: NewHealthRegistry(), tracing: tracing}
	a.initServer()

	return a
}

func TestTraceHTTPExportsLinkedSpansWithoutRequestData(t *testing.T) {
	t.Parallel()

	endpoint, collector := newCollectorFixture(t)
	a, b := tracedApp(t, endpoint, "frontend-fixture", 1), tracedApp(t, endpoint, "backend-fixture", 1)
	const private = "private-user-field"
	b.HTTP().HandleFunc("POST /v1/profile/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Baggage") != "" || r.Header.Get("Tracestate") != "" ||
			TraceIDFrom(r.Context()) != traceFixture[3:35] {
			t.Error("downstream context was lost or carried private headers")
		}
		w.WriteHeader(http.StatusForbidden)
	})
	downstream := httptest.NewServer(b.server.Handler)
	t.Cleanup(downstream.Close)
	client := &http.Client{Transport: a.TraceHTTPTransport(nil), Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	a.HTTP().HandleFunc("POST /v1/learners/{id}", func(w http.ResponseWriter, incoming *http.Request) {
		req, err := http.NewRequestWithContext(incoming.Context(), http.MethodPost, downstream.URL+"/v1/profile/"+private+"?data="+private,
			strings.NewReader(private))
		if err != nil {
			t.Error(err)

			return
		}
		req.Header.Set("Baggage", "context="+private)
		req.Header.Set("Tracestate", "vendor="+private)
		req.Header["tRaCePaReNt"] = []string{"invalid"}
		req.Header["bAgGaGe"] = []string{"legacy=" + private}
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)

			return
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden || req.Header.Get(TraceparentHeader) != "" {
			t.Error("the transport changed the application request or response")
		}
		w.WriteHeader(http.StatusCreated)
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/learners/"+private+"?q="+private, strings.NewReader(private))
	req.Header.Set(TraceparentHeader, traceFixture)
	req.Header.Set("Authorization", "Bearer "+private)
	response := httptest.NewRecorder()
	a.server.Handler.ServeHTTP(response, req)
	if response.Code != http.StatusCreated {
		t.Fatal("HTTP handling changed while recording spans", response.Code)
	}
	a.tracing.shutdown(t.Context())
	b.tracing.shutdown(t.Context())
	spans, wire := collector.snapshot()
	if len(spans) != 3 || bytes.Contains(wire, []byte(private)) {
		t.Fatal("HTTP trace missing spans or leaking data", len(spans))
	}
	var entry, call, child *tracepb.Span
	for _, span := range spans {
		if hex.EncodeToString(span.GetTraceId()) != traceFixture[3:35] {
			t.Fatal("trace forked on an HTTP hop")
		}
		switch {
		case span.GetKind() == tracepb.Span_SPAN_KIND_CLIENT:
			call = span
		case spanRoute(span) == "/v1/learners/{id}":
			entry = span
		case spanRoute(span) == "/v1/profile/{id}":
			child = span
		}
	}
	if entry == nil || call == nil || child == nil || hex.EncodeToString(entry.GetParentSpanId()) != traceFixture[36:52] ||
		!bytes.Equal(entry.GetSpanId(), call.GetParentSpanId()) || !bytes.Equal(call.GetSpanId(), child.GetParentSpanId()) ||
		call.GetStatus().GetMessage() != "client_error" {
		t.Fatal("incorrect HTTP parent chain or error status")
	}
}

func TestTraceHTTPDoesNotPropagateAfterRedirect(t *testing.T) {
	t.Parallel()

	var redirected atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Store(true)
		if r.Header.Get(TraceparentHeader) != "" || r.Header.Get("Baggage") != "" {
			t.Error("trace context escaped through an HTTP redirect")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(target.Close)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(TraceparentHeader) != traceFixture {
			t.Error("context was not propagated before the redirect")
		}
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirect.Close)
	app := &App{}
	client := &http.Client{Transport: app.TraceHTTPTransport(nil), Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	ctx := extractTraceparent(t.Context(), traceFixture)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, redirect.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if !redirected.Load() || response.StatusCode != http.StatusNoContent {
		t.Fatal("tracing transport interrupted the application's redirect")
	}
}

func spanRoute(span *tracepb.Span) string {
	for _, attr := range span.GetAttributes() {
		if attr.GetKey() == "http.route" {
			return attr.GetValue().GetStringValue()
		}
	}

	return ""
}

func TestTraceExporterHonorsSampling(t *testing.T) {
	t.Parallel()

	endpoint, collector := newCollectorFixture(t)
	for _, tc := range []struct {
		ratio  float64
		parent string
	}{{0, traceFixture}, {1, traceFixture[:53] + "00"}} {
		a := tracedApp(t, endpoint, "sampling-fixture", tc.ratio)
		a.HTTP().HandleFunc("GET /work", func(_ http.ResponseWriter, r *http.Request) {
			if sc := trace.SpanContextFromContext(r.Context()); !sc.IsValid() || sc.IsSampled() {
				t.Error("sampling disabled context propagation or recorded an unsampled parent")
			}
		})
		r := httptest.NewRequest(http.MethodGet, "/work", nil)
		r.Header.Set(TraceparentHeader, tc.parent)
		a.server.Handler.ServeHTTP(httptest.NewRecorder(), r)
		a.tracing.shutdown(t.Context())
	}
	if spans, _ := collector.snapshot(); len(spans) != 0 {
		t.Fatal("unsampled spans reached the collector", len(spans))
	}
}

func TestOTLPClientRejectsRedirectsAndOversizeReplies(t *testing.T) {
	t.Parallel()

	var leaked atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Store(true) }))
	t.Cleanup(target.Close)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
		case "/large":
			_, _ = io.WriteString(w, strings.Repeat("x", maxOTLPResponse+1))
		case "/partial":
			body, err := proto.Marshal(&collectorpb.ExportTraceServiceResponse{
				PartialSuccess: &collectorpb.ExportTracePartialSuccess{RejectedSpans: 1, ErrorMessage: "private-collector-data"},
			})
			if err != nil {
				t.Error(err)

				return
			}
			_, _ = w.Write(body)
		}
	}))
	t.Cleanup(collector.Close)
	for path, want := range map[string]error{"/redirect": errTraceExport, "/large": errTraceExport, "/partial": errTracePartial} {
		endpoint, err := url.Parse(collector.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		client := newOTLPClient(endpoint)
		if err := client.UploadTraces(t.Context(), nil); !errors.Is(err, want) || strings.Contains(err.Error(), "private-collector-data") {
			t.Fatal("collector failure was not redacted for", path, err)
		}
		_ = client.Stop(t.Context())
	}
	if leaked.Load() {
		t.Fatal("collector redirect forwarded trace data")
	}
}

func TestTraceRunnerFlushHonorsShutdownDeadline(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	collector := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		collector.Close()
	})
	a := tracedApp(t, collector.URL, "shutdown-fixture", 1)
	_, span := a.tracing.tracer.Start(t.Context(), "fixture")
	span.End()
	const budget = 250 * time.Millisecond
	r := NewRunner(a.log, budget)
	r.afterShutdown = a.tracing.shutdown
	ctx, cancel := context.WithCancel(t.Context())
	r.AddRunnable("cancel", func(context.Context) error {
		cancel()

		return nil
	})
	start := time.Now()
	server := &http.Server{Addr: "127.0.0.1:0", Handler: a.server.Handler, ReadHeaderTimeout: time.Second}
	if err := r.Run(ctx, server); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("stalled collector exceeded the shutdown budget")
	}
	select {
	case <-started:
	default:
		t.Fatal("shutdown did not try to flush the queued span")
	}
}

//nolint:paralleltest // t.Setenv изменяет process-wide environment
func TestTracingConfigRejectsSensitiveOrInsecureEndpoints(t *testing.T) {
	for _, raw := range []string{
		"http://collector.invalid:4318",
		"https://user:private-fixture@collector.invalid",
		"https://collector.invalid?token=private-fixture",
		"https://collector.invalid/#private-fixture",
		"https://collector.invalid:65536",
	} {
		o := TraceOptions{Endpoint: raw, SampleRatio: 0.1}
		if _, err := o.collectorEndpoint("production", "test"); err == nil || strings.Contains(err.Error(), "private-fixture") {
			t.Fatal("unsafe endpoint accepted or repeated in error")
		}
	}
	t.Setenv("PARKER_TRACE_SAMPLE_RATIO", "NaN")
	if _, err := Load(); err == nil {
		t.Fatal("non-finite sampling ratio accepted")
	}
	valid, err := (TraceOptions{Endpoint: "https://collector.invalid", SampleRatio: 0.1}).collectorEndpoint("production", "test")
	if err != nil || valid.Path != traceExportPath {
		t.Fatal("secure endpoint rejected or missing default path", err)
	}
}

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
			tracing := &traceRuntime{tracer: provider.Tracer("benchmark")}
			mux := http.NewServeMux()
			mux.HandleFunc("GET /work/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
			h := tracing.serverMiddleware(mux)
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
