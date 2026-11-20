package parker

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

const traceFixture = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestParseTraceparent_Valid(t *testing.T) {
	ctx := extractTraceparent(t.Context(), traceFixture)
	if TraceparentFrom(ctx) != traceFixture || !trace.SpanContextFromContext(ctx).IsRemote() {
		t.Fatal("valid W3C trace did not round-trip")
	}
}

func TestParseTraceparent_Invalid(t *testing.T) {
	cases := []string{
		"",
		"00-bad-span",
		"01" + traceFixture[2:],
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		traceFixture + strings.Repeat("-", 65536),
		strings.TrimSuffix(traceFixture, "01") + "xx",
	}
	for _, raw := range cases {
		if ctx := extractTraceparent(t.Context(), raw); TraceparentFrom(ctx) != "" {
			t.Fatal("invalid W3C trace was accepted")
		}
	}
}

func TestTraceMiddleware_SetsContext(t *testing.T) {
	h := (*traceRuntime)(nil).serverMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if TraceIDFrom(r.Context()) != "4bf92f3577b34da6a3ce929d0e0e4736" || SpanIDFrom(r.Context()) != "00f067aa0ba902b7" {
			t.Error("middleware did not propagate the parent context")
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(TraceparentHeader, traceFixture)
	h.ServeHTTP(httptest.NewRecorder(), req)
}

func TestTraceMiddleware_NoHeaderNoop(t *testing.T) {
	h := (*traceRuntime)(nil).serverMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if TraceIDFrom(r.Context()) != "" {
			t.Error("missing or duplicate traceparent produced a parent")
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)
	req.Header.Add(TraceparentHeader, traceFixture)
	req.Header.Add(TraceparentHeader, traceFixture)
	h.ServeHTTP(httptest.NewRecorder(), req)
}

func TestApp_LoggerForAddsTraceFields(t *testing.T) {
	var logs bytes.Buffer
	app := &App{log: bufferLogger(&logs)}
	ctx := extractTraceparent(withRequestID(context.Background(), "req-9"), traceFixture)
	app.LoggerFor(ctx).Info("request")
	for _, field := range []string{"request_id=req-9", "trace_id=4bf92f3577b34da6a3ce929d0e0e4736", "span_id=00f067aa0ba902b7"} {
		if !strings.Contains(logs.String(), field) {
			t.Fatal("missing logger field", field)
		}
	}
}

func TestKafkaTraceReplacesDuplicateHeaders(t *testing.T) {
	ctx := t.Context()
	parent := extractTraceparent(ctx, traceFixture)
	record := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "Traceparent", Value: []byte(traceFixture)},
		{Key: TraceparentHeader, Value: []byte(traceFixture)},
		{Key: "content-type", Value: []byte("application/json")},
	}}
	if TraceparentFrom(ExtractKafkaTrace(ctx, record)) != "" {
		t.Fatal("ambiguous parent accepted")
	}
	InjectKafkaTrace(parent, record)
	if len(record.Headers) != 2 || record.Headers[0].Key != "content-type" ||
		TraceparentFrom(ExtractKafkaTrace(ctx, record)) != traceFixture {
		t.Fatal("Kafka propagation did not preserve the single valid header")
	}
}
