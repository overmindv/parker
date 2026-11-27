package parker

import (
	"net/http"

	"github.com/overmindv/parker/internal/httpkit"
)

// HTTPServer — тонкая обёртка над *http.ServeMux для регистрации бизнес-роутов.
type HTTPServer httpkit.Server

// Handle регистрирует обработчик по паттерну (поддерж: "GET /tasks", "POST /tasks/{id}").
func (s *HTTPServer) Handle(pattern string, handler http.Handler) {
	(*httpkit.Server)(s).Handle(pattern, handler)
}

// HandleFunc регистрирует функцию-обработчик по паттерну.
func (s *HTTPServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	(*httpkit.Server)(s).HandleFunc(pattern, handler)
}
