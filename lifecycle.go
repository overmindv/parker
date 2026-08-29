package parker

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Runnable — фоновый воркер, выполняемый Runner'ом. Должен завершаться по отмене ctx.
type Runnable func(ctx context.Context) error

type namedRunnable struct {
	name string
	fn   Runnable
}

// Runner управляет HTTP-сервером и фоновыми воркерами: запускает их, ожидает
// сигнала отмены (обычно signal.NotifyContext) либо fatal-ошибки, затем корректно
// завершает работу (graceful shutdown с таймаутом).
//
// Политика fatal-остановки (как в `tasks` runWorker/serve):
// если сервер или воркер завершился с ошибкой, пока контекст активен, — логируем,
// отменяем контекст (останавливая остальные воркеры) и возвращаем ошибку из Run.
type Runner struct {
	log             *slog.Logger
	shutdownTimeout time.Duration
	runnables       []namedRunnable
}

// NewRunner создаёт Runner.
func NewRunner(log *slog.Logger, shutdownTimeout time.Duration) *Runner {
	return &Runner{
		log:             log,
		shutdownTimeout: shutdownTimeout,
	}
}

// AddRunnable регистрирует фоновый воркер.
func (r *Runner) AddRunnable(name string, fn Runnable) {
	if fn != nil {
		r.runnables = append(r.runnables, namedRunnable{name: name, fn: fn})
	}
}

// Run запускает сервер и воркеры и блокирует до завершения.
// Возвращает ошибку при fatal-остановке; при graceful shutdown по сигналу — nil.
func (r *Runner) Run(ctx context.Context, server *http.Server) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fatal := make(chan error, 1)

	// HTTP-сервер.
	go func() {
		r.log.Info("http-сервер запущен", "address", server.Addr)
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
			r.log.Error("http-сервер завершился с ошибкой", "error", err)
			r.notifyFatal(fatal, err)
			cancel()
		}
	}()

	// Фоновые воркеры.
	var wg sync.WaitGroup
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		for _, rn := range r.runnables {
			rn := rn
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := rn.fn(ctx); err != nil && ctx.Err() == nil {
					r.log.Error(rn.name+" завершился с ошибкой", "error", err)
					r.notifyFatal(fatal, err)
					cancel()
				}
			}()
		}
		wg.Wait()
	}()

	<-ctx.Done()

	r.log.Info("graceful shutdown: останавливаю http-сервер и воркеры",
		"shutdown_timeout", r.shutdownTimeout)

	shutdownCtx, scancel := context.WithTimeout(context.Background(), r.shutdownTimeout)
	defer scancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		r.log.Error("не удалось корректно остановить http-сервер", "error", err)
	}

	select {
	case <-workersDone:
	case <-shutdownCtx.Done():
		r.log.Warn("таймаут завершения фоновых воркеров")
	}

	select {
	case err := <-fatal:
		return err
	default:
		return nil
	}
}

// notifyFatal кладёт ошибку в канал fatal без блокировки (не более одной).
func (r *Runner) notifyFatal(ch chan error, err error) {
	select {
	case ch <- err:
	default:
	}
}
