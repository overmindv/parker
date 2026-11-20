package parker

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	maxOTLPRequest  = 1 << 20
	maxOTLPResponse = 64 << 10
)

var (
	errTraceExport  = errors.New("trace batch export failed")
	errTracePartial = errors.New("collector rejected some spans")
)

// OTLP Client не читает параметры транспорта из окружения и не пересылает экспорт по редиректу.
type otlpClient struct {
	endpoint string
	client   *http.Client
}

func newOTLPClient(endpoint *url.URL) *otlpClient {
	transport := &http.Transport{
		DialContext:            (&net.Dialer{Timeout: traceExportTimeout}).DialContext,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2:      true,
		MaxResponseHeaderBytes: 16 << 10, MaxConnsPerHost: 2, MaxIdleConns: 2, IdleConnTimeout: 30 * time.Second,
	}

	return &otlpClient{
		endpoint: endpoint.String(),
		client: &http.Client{Transport: transport, Timeout: traceExportTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

func (*otlpClient) Start(context.Context) error { return nil }

func (c *otlpClient) Stop(context.Context) error {
	c.client.CloseIdleConnections()

	return nil
}

func (c *otlpClient) UploadTraces(ctx context.Context, spans []*tracepb.ResourceSpans) error {
	message := &collectorpb.ExportTraceServiceRequest{ResourceSpans: spans}
	if proto.Size(message) > maxOTLPRequest {
		return errTraceExport
	}
	body, err := proto.Marshal(message)
	if err != nil {
		return errTraceExport
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return errTraceExport
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := c.client.Do(req)
	if err != nil {
		return errTraceExport
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return errTraceExport
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, maxOTLPResponse+1))
	if err != nil || len(body) > maxOTLPResponse {
		return errTraceExport
	}
	var reply collectorpb.ExportTraceServiceResponse
	if err := proto.Unmarshal(body, &reply); err != nil {
		return errTraceExport
	}
	if partial := reply.GetPartialSuccess(); partial.GetRejectedSpans() > 0 || partial.GetErrorMessage() != "" {
		return errTracePartial
	}

	return nil
}
