package parker

import (
	"log/slog"

	"github.com/overmindv/parker/internal/logging"
)

// NewLogger собирает структурный JSON-логгер с заданным уровнем.
func NewLogger(level string) (*slog.Logger, error) {
	return logging.New(level)
}
