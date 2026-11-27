// Package httpkit содержит HTTP-обёртки и middleware parker.
package httpkit

import "net/http"

// Server — тонкая обёртка над *http.ServeMux для регистрации бизнес-роутов.
type Server struct{ mux *http.ServeMux }

// NewServer оборачивает mux.
func NewServer(mux *http.ServeMux) *Server {
	return &Server{mux: mux}
}

// Handle регистрирует обработчик по паттерну (поддерж: "GET /tasks", "POST /tasks/{id}").
func (s *Server) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

// HandleFunc регистрирует функцию-обработчик по паттерну.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.mux.HandleFunc(pattern, handler)
}
