package observability

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
	MaxOTLPRequest  = 1 << 20
	MaxOTLPResponse = 64 << 10
)

var (
	ErrTraceExport  = errors.New("trace batch export failed")
	ErrTracePartial = errors.New("collector rejected some spans")
)

// OTLP Client не читает параметры транспорта из окружения и не пересылает экспорт по редиректу.
type OTLPClient struct {
	endpoint string
	client   *http.Client
}

func NewOTLPClient(endpoint *url.URL) *OTLPClient {
	transport := &http.Transport{
		DialContext:            (&net.Dialer{Timeout: traceExportTimeout}).DialContext,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2:      true,
		MaxResponseHeaderBytes: 16 << 10, MaxConnsPerHost: 2, MaxIdleConns: 2, IdleConnTimeout: 30 * time.Second,
	}

	return &OTLPClient{
		endpoint: endpoint.String(),
		client: &http.Client{Transport: transport, Timeout: traceExportTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

func (*OTLPClient) Start(context.Context) error { return nil }

func (c *OTLPClient) Stop(context.Context) error {
	c.client.CloseIdleConnections()

	return nil
}

func (c *OTLPClient) UploadTraces(ctx context.Context, spans []*tracepb.ResourceSpans) error {
	message := &collectorpb.ExportTraceServiceRequest{ResourceSpans: spans}
	if proto.Size(message) > MaxOTLPRequest {
		return ErrTraceExport
	}
	body, err := proto.Marshal(message)
	if err != nil {
		return ErrTraceExport
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return ErrTraceExport
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := c.client.Do(req)
	if err != nil {
		return ErrTraceExport
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return ErrTraceExport
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, MaxOTLPResponse+1))
	if err != nil || len(body) > MaxOTLPResponse {
		return ErrTraceExport
	}
	var reply collectorpb.ExportTraceServiceResponse
	if err := proto.Unmarshal(body, &reply); err != nil {
		return ErrTraceExport
	}
	if partial := reply.GetPartialSuccess(); partial.GetRejectedSpans() > 0 || partial.GetErrorMessage() != "" {
		return ErrTracePartial
	}

	return nil
}
