package parker

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/overmindv/parker/internal/lifecycle"
)

// Runnable — фоновый воркер, выполняемый Runner'ом. Должен завершаться по отмене ctx.
type Runnable func(ctx context.Context) error

// Runner управляет HTTP-сервером и фоновыми воркерами: запускает их, ожидает
// сигнала отмены либо fatal-ошибки, затем корректно завершает работу.
type Runner lifecycle.Runner

// NewRunner создаёт Runner.
func NewRunner(log *slog.Logger, shutdownTimeout time.Duration) *Runner {
	return (*Runner)(lifecycle.NewRunner(log, shutdownTimeout))
}

// AddRunnable регистрирует фоновый воркер.
func (r *Runner) AddRunnable(name string, fn Runnable) {
	(*lifecycle.Runner)(r).AddRunnable(name, lifecycle.Runnable(fn))
}

// Run запускает сервер и воркеры и блокирует до завершения.
func (r *Runner) Run(ctx context.Context, server *http.Server) error {
	return (*lifecycle.Runner)(r).Run(ctx, server)
}
