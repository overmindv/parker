// Package app собирает инфраструктуру и управляет жизненным циклом сервиса.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof включается явно и обслуживается отдельным внутренним listener
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/parker/internal/cli"
	"github.com/overmindv/parker/internal/config"
	"github.com/overmindv/parker/internal/health"
	"github.com/overmindv/parker/internal/httpkit"
	"github.com/overmindv/parker/internal/lifecycle"
	"github.com/overmindv/parker/internal/logging"
	"github.com/overmindv/parker/internal/observability"
	"github.com/overmindv/parker/internal/requestid"
)

const serveCommand = "serve"

// App — единая точка входа прикладного сервиса: всё инфраструктурное уже собрано.
// Сервис в вызываемом run(*App) регистрирует бизнес-роуты, health-чекки и воркеры.
type App struct {
	opts      config.Options
	log       *slog.Logger
	health    *health.Registry
	mux       *http.ServeMux
	metrics   *observability.Metrics
	tracing   *observability.Runtime
	server    *http.Server
	runnables []lifecycle.NamedRunnable

	pgOnce sync.Once
	pgPool *pgxpool.Pool
	pgErr  error

	closers []func() // ресурсы, созданные через App (pg/kafka), закрываются в Close
}

// MainArgs запускает приложение с заданным списком аргументов.
// Подкоманды: serve (по умолчанию), migrate, init, version.
func MainArgs(args []string, version string, run func(*App) error, configure func(config.Options) config.Options) int {
	cmd := serveCommand
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		cmd = args[0]
	}

	switch cmd {
	case serveCommand:
		return runServe(run, configure)
	case "version":
		_, _ = fmt.Fprintln(os.Stdout, "parker", version)

		return 0
	case "migrate":
		return cli.Migrate(args[1:])
	case "init":
		return cli.Init(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "parker: неизвестная команда %q\n", cmd)
		fmt.Fprintln(os.Stderr, "доступные команды: serve, migrate, init, version")

		return 2
	}
}

// runServe загружает конфиг, собирает App, вызывает run(app) и запускает Runner.
func runServe(run func(*App) error, configure func(config.Options) config.Options) int {
	options, err := config.Load()
	if err != nil {
		logFatal(err)

		return 1
	}
	options = configure(options)

	logger, err := logging.New(options.LogLevel)
	if err != nil {
		logFatal(err)

		return 1
	}

	app, err := New(options, logger)
	if err != nil {
		logFatal(err)

		return 1
	}
	defer app.Close()
	tracing := app.tracing

	if run != nil {
		if err := run(app); err != nil {
			if tracing != nil {
				tracing.Shutdown(context.Background())
			}
			logger.Error("не удалось инициализировать сервис", "error", err)

			return 1
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runner := lifecycle.NewRunner(logger, options.ShutdownTimeout)
	if tracing != nil {
		runner.SetAfterShutdown(tracing.Shutdown)
	}
	for _, rn := range app.runnables {
		runner.AddRunnable(rn.Name, rn.Fn)
	}

	logger.Info(options.ServiceName+" запущен",
		"address", options.HTTPAddress, "environment", options.Environment)

	if err := runner.Run(ctx, app.server); err != nil {
		logger.Error(options.ServiceName+" завершился с ошибкой", "error", err)

		return 1
	}

	return 0
}

// AdaptRun связывает пользовательский callback с публичным типом приложения.
func AdaptRun[T any](run func(*T) error, wrap func(*App) *T) func(*App) error {
	if run == nil {
		return nil
	}

	return func(a *App) error { return run(wrap(a)) }
}

// New создаёт контекст приложения и HTTP-сервер с настроенными middleware.
func New(opts config.Options, logger *slog.Logger) (*App, error) {
	tracing, err := observability.NewRuntime(opts.Tracing.Endpoint, opts.Tracing.SampleRatio, opts.ServiceName, opts.Environment, logger)
	if err != nil {
		return nil, err
	}
	a := &App{opts: opts, log: logger, health: health.NewRegistry(), tracing: tracing}
	a.initServer()

	return a, nil
}

// trackCloser регистрирует функцию очистки ресурса на завершение приложения.
func (a *App) trackCloser(closeFn func()) {
	a.closers = append(a.closers, closeFn)
}

// Close закрывает все ресурсы, созданные через App.
func (a *App) Close() {
	for _, fn := range a.closers {
		fn()
	}
}

// Config возвращает конфигурацию parker.
func (a *App) Config() config.Options { return a.opts }

// Logger возвращает структурный логгер сервиса.
func (a *App) Logger() *slog.Logger { return a.log }

// LoggerFor возвращает логгер с полями трассировки из контекста запроса
// (request_id/trace_id/span_id). Используется бизнес-логикой для связных логов.
func (a *App) LoggerFor(ctx context.Context) *slog.Logger {
	var attrs []any
	if id := requestid.From(ctx); id != "" {
		attrs = append(attrs, "request_id", id)
	}
	if id := observability.TraceIDFrom(ctx); id != "" {
		attrs = append(attrs, "trace_id", id)
	}
	if id := observability.SpanIDFrom(ctx); id != "" {
		attrs = append(attrs, "span_id", id)
	}

	return a.log.With(attrs...)
}

// HTTP возвращает роутер для регистрации бизнес-роутов
// (встроенные /health и /ready уже зарегистрированы).
func (a *App) HTTP() *httpkit.Server { return httpkit.NewServer(a.mux) }

// AddHealthCheck регистрирует проверку готовности (агрегируется в /ready).
func (a *App) AddHealthCheck(name string, check health.Check) {
	a.health.Add(name, check)
}

// AddRunnable регистрирует фоновый воркер с graceful shutdown.
func (a *App) AddRunnable(name string, fn lifecycle.Runnable) {
	if fn != nil {
		a.runnables = append(a.runnables, lifecycle.NamedRunnable{Name: name, Fn: fn})
	}
}

// initServer создаёт http.ServeMux со встроенными эндпоинтами, оборачивает его
// middleware-цепочкой (recover → access-log → request-id → metrics) и собирает
// http.Server с таймаутами из Options.
func (a *App) initServer() {
	a.mux = http.NewServeMux()
	a.mux.HandleFunc("GET /health", a.handleHealth)
	a.mux.HandleFunc("GET /ready", a.handleReady)
	if a.opts.MetricsEnabled {
		a.metrics = observability.NewMetrics(true)
		a.mux.Handle("GET /metrics", a.metrics.Handler())
	}
	if a.opts.PprofEnabled {
		a.registerPprof()
	}

	// Цепочка "снаружи внутрь":
	// recover → request-id → trace → access-log → metrics → mux.
	handler := http.Handler(a.mux)
	if a.metrics != nil {
		handler = observability.HTTPMiddleware(a.metrics, a.opts.ServiceName)(handler)
	}
	handler = httpkit.AccessLogMiddleware(a.log)(handler)
	handler = a.tracing.ServerMiddleware(handler)
	handler = httpkit.RequestIDMiddleware(handler)
	handler = httpkit.RecoverMiddleware(a.log)(handler)

	a.server = &http.Server{
		Addr:              a.opts.HTTPAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       a.opts.ReadTimeout,
		WriteTimeout:      a.opts.WriteTimeout,
		IdleTimeout:       60 * time.Second,
	}
}

// registerPprof проксирует стандартные pprof-эндпоинты (net/http/pprof регистрирует
// их на DefaultServeMux) в наш mux.
func (a *App) registerPprof() {
	for _, p := range []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
		"/debug/pprof/trace",
	} {
		a.mux.Handle(p, http.DefaultServeMux)
	}
}

func (a *App) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (a *App) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := a.health.Ready(r.Context()); err != nil {
		if a.metrics != nil {
			a.metrics.SetReady(a.opts.ServiceName, false)
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)

		return
	}
	if a.metrics != nil {
		a.metrics.SetReady(a.opts.ServiceName, true)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// logFatal печатает ошибку в stderr.
func logFatal(err error) {
	fmt.Fprintf(os.Stderr, "parker: %v\n", err)
}

// Metrics возвращает регистр приложения для общих инфраструктурных workers.
// При выключенных метриках возвращает nil.
func (a *App) Metrics() *observability.Metrics { return a.metrics }

func (a *App) Handler() http.Handler           { return a.server.Handler }
func (a *App) Tracing() *observability.Runtime { return a.tracing }
