package parker

import (
	"fmt"
	"log/slog"
	"os"
)

// NewLogger собирает структурный JSON-логгер с заданным уровнем.
func NewLogger(level string) (*slog.Logger, error) {
	slogLevel, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel})
	return slog.New(handler), nil
}

// parseLevel преобразует строковый уровень в slog.Level.
func parseLevel(value string) (slog.Level, error) {
	switch value {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("недопустимый уровень лога %q: ожидается debug, info, warn или error", value)
	}
}
