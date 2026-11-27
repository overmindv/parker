//go:build component

package parker

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/overmindv/parker/internal/requestid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestKafkaTraceHTTPToBrokerAndHandler(t *testing.T) {

	broker := os.Getenv("PARKER_TEST_KAFKA")
	if broker == "" {
		t.Skip("PARKER_TEST_KAFKA must point to a dedicated broker")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	brokers := strings.Split(broker, ",")
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	topic := "parker-private-topic-" + requestid.New()
	request := kmsg.NewPtrCreateTopicsRequest()
	request.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	created, err := request.RequestWith(ctx, client)
	if err != nil || len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
		t.Fatal("cannot create isolated Kafka topic", err)
	}

	endpoint, collector := newCollectorFixture(t)
	withBrokers := func(o *Options) { o.KafkaBrokers = brokers }
	sender := tracedApp(t, endpoint, "sender-fixture", 1, withBrokers)
	receiver := tracedApp(t, endpoint, "receiver-fixture", 1, withBrokers)
	producer, err := sender.NewProducer()
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	const key, payload = "private-user-key", "private-message-value"
	subscriber, err := receiver.NewSubscriber("parker-trace-"+requestid.New(), []string{topic}, func(ctx context.Context, gotKey string, value []byte) error {
		if TraceIDFrom(ctx) != traceFixture[3:35] || SpanIDFrom(ctx) == "" || gotKey != key || string(value) != payload {
			t.Error("Kafka handler did not receive context and unchanged payload")
		}
		position, ok := PositionFromContext(ctx)
		if !ok || position != (RecordPosition{Topic: topic, Partition: 0, Offset: 0}) {
			t.Error("traced handler did not receive the Kafka record position")
		}
		if attempts.Add(1) == 1 {
			return errors.New("private-handler-error")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- subscriber.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-result; err != nil {
			t.Error("Kafka subscriber reported an error", err)
		}
	})
	sender.HTTP().HandleFunc("POST /queued/{id}", func(w http.ResponseWriter, r *http.Request) {
		if err := producer.Publish(r.Context(), topic, key, []byte(payload)); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	in := httptest.NewRequestWithContext(ctx, http.MethodPost, "/queued/private-actor", strings.NewReader("private-code"))
	in.Header.Set(TraceparentHeader, traceFixture)
	out := httptest.NewRecorder()
	sender.state.Handler().ServeHTTP(out, in)
	if out.Code != http.StatusAccepted {
		t.Fatal("HTTP request did not publish")
	}
	sender.state.Tracing().Shutdown(ctx)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		spans, wire := collector.snapshot()
		if len(spans) >= 3 {
			assertKafkaChain(t, spans, wire, topic)
			if attempts.Load() != 2 {
				t.Fatal("tracing changed retry count", attempts.Load())
			}
			return
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("collector did not see HTTP, producer and consumer spans")
		}
	}
}

func assertKafkaChain(t *testing.T, spans []*tracepb.Span, wire []byte, topic string) {
	t.Helper()
	var server, producer, consumer *tracepb.Span
	for _, span := range spans {
		switch span.GetKind() {
		case tracepb.Span_SPAN_KIND_SERVER:
			server = span
		case tracepb.Span_SPAN_KIND_PRODUCER:
			producer = span
		case tracepb.Span_SPAN_KIND_CONSUMER:
			consumer = span
		}
	}
	if len(spans) != 3 || server == nil || producer == nil || consumer == nil ||
		!bytes.Equal(server.GetTraceId(), producer.GetTraceId()) || !bytes.Equal(server.GetTraceId(), consumer.GetTraceId()) ||
		!bytes.Equal(server.GetSpanId(), producer.GetParentSpanId()) || !bytes.Equal(producer.GetSpanId(), consumer.GetParentSpanId()) ||
		consumer.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR {
		t.Fatal("Kafka did not export a successful consumer span under the producer")
	}
	for _, private := range []string{topic, "private-user-key", "private-message-value", "private-handler-error", "private-actor", "private-code"} {
		if bytes.Contains(wire, []byte(private)) {
			t.Fatal("trace payload contains data from the HTTP or Kafka message")
		}
	}
}
