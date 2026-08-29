package parker

import (
	"context"
	"log/slog"
	"testing"
)

func TestNewLogger_ValidLevels(t *testing.T) {
	for _, lvl := range []string{"debug", "info", "warn", "error"} {
		if _, err := NewLogger(lvl); err != nil {
			t.Fatalf("уровень %q не должен падать: %v", lvl, err)
		}
	}
}

func TestNewLogger_InvalidLevel(t *testing.T) {
	if _, err := NewLogger("verbose"); err == nil {
		t.Fatal("ожидалась ошибка для некорректного уровня")
	}
}

func TestNewLogger_IsSlog(t *testing.T) {
	logger, err := NewLogger("debug")
	if err != nil {
		t.Fatal(err)
	}
	if logger == nil {
		t.Fatal("логгер не должен быть nil")
	}
	if !logger.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("debug-уровень должен быть включён")
	}
}
