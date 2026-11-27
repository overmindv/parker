package parker

import (
	"net/http"

	"github.com/overmindv/parker/internal/observability"
)

// TraceHTTPTransport передает traceparent во внутренние HTTP-вызовы.
// Для внешних адресов используйте обычный транспорт. nil означает http.DefaultTransport.
func (a *App) TraceHTTPTransport(base http.RoundTripper) http.RoundTripper {
	return observability.HTTPTransport(a.tracing, base)
}
