package parker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunner_GracefulShutdownOnCancel(t *testing.T) {
	runner := NewRunner(discardLogger(), time.Second)

	started := make(chan struct{})
	stopped := make(chan struct{})
	runner.AddRunnable("worker", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil
	})

	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx, server) }()

	<-started
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ожидался nil при graceful shutdown, получили %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Runner.Run не завершился по отмене контекста")
	}

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("воркер не завершился после отмены контекста")
	}
}

func TestRunner_FatalRunnableStopsApp(t *testing.T) {
	runner := NewRunner(discardLogger(), time.Second)
	runner.AddRunnable("bad", func(context.Context) error { return errors.New("boom") })

	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}
	ctx := context.Background()

	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx, server) }()

	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("ожидалась fatal-ошибка")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Runner.Run не завершился по fatal-ошибке воркера")
	}
}
