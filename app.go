package parker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // регистрирует pprof-хендлеры на http.DefaultServeMux
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Version — версия parker, печатается командой `version`.
const Version = "0.1.0"

// App — единая точка входа прикладного сервиса: всё инфраструктурное уже собрано.
// Сервис в вызываемом run(*App) регистрирует бизнес-роуты, health-чекки и воркеры.
type App struct {
	opts      Options
	log       *slog.Logger
	health    *HealthRegistry
	mux       *http.ServeMux
	metrics   *Metrics
	server    *http.Server
	runnables []namedRunnable

	pgOnce sync.Once
	pgPool *pgxpool.Pool
	pgErr  error

	closers []func() // ресурсы, созданные через App (pg/kafka), закрываются в closeAll
}

// Main запускает приложение из os.Args. Эквивалент MainArgs(run, os.Args[1:], opts...).
func Main(run func(*App) error, opts ...Option) int {
	return MainArgs(run, os.Args[1:], opts...)
}

// MainArgs запускает приложение с заданным списком аргументов.
// Подкоманды: serve (по умолчанию), migrate, init, version.
func MainArgs(run func(*App) error, args []string, opts ...Option) int {
	cmd := "serve"
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		cmd = args[0]
	}

	switch cmd {
	case "serve":
		// runServe не использует args (инфраструктурная часть уже собрана в Options),
		// поэтому передаём nil — исключаем panic на args[1:] при пустом списке аргументов.
		return runServe(run, nil, opts...)
	case "version":
		_, _ = fmt.Fprintln(os.Stdout, "parker", Version)
		return 0
	case "migrate":
		return runMigrateCLI(args[1:])
	case "init":
		return runInitCLI(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "parker: неизвестная команда %q\n", cmd)
		fmt.Fprintln(os.Stderr, "доступные команды: serve, migrate, init, version")
		return 2
	}
}

// runServe загружает конфиг, собирает App, вызывает run(app) и запускает Runner.
func runServe(run func(*App) error, _ []string, opts ...Option) int {
	options, err := Load()
	if err != nil {
		logFatal(err)
		return 1
	}
	options = options.Apply(opts)

	logger, err := NewLogger(options.LogLevel)
	if err != nil {
		logFatal(err)
		return 1
	}

	app := &App{opts: options, log: logger, health: NewHealthRegistry()}
	app.initServer()
	defer app.closeAll()

	if run != nil {
		if err := run(app); err != nil {
			logger.Error("не удалось инициализировать сервис", "error", err)
			return 1
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runner := NewRunner(logger, options.ShutdownTimeout)
	for _, rn := range app.runnables {
		runner.AddRunnable(rn.name, rn.fn)
	}

	logger.Info(options.ServiceName+" запущен",
		"address", options.HTTPAddress, "environment", options.Environment)

	if err := runner.Run(ctx, app.server); err != nil {
		logger.Error(options.ServiceName+" завершился с ошибкой", "error", err)
		return 1
	}
	return 0
}

// trackCloser регистрирует функцию очистки ресурса на завершение приложения.
func (a *App) trackCloser(closeFn func()) {
	a.closers = append(a.closers, closeFn)
}

// closeAll закрывает все ресурсы, созданные через App.
func (a *App) closeAll() {
	for _, fn := range a.closers {
		fn()
	}
}

// Config возвращает конфигурацию parker.
func (a *App) Config() Options { return a.opts }

// Logger возвращает структурный логгер сервиса.
func (a *App) Logger() *slog.Logger { return a.log }

// LoggerFor возвращает логгер с полями трассировки из контекста запроса
// (request_id/trace_id/span_id). Используется бизнес-логикой для связных логов.
func (a *App) LoggerFor(ctx context.Context) *slog.Logger {
	var attrs []any
	if id := RequestIDFrom(ctx); id != "" {
		attrs = append(attrs, "request_id", id)
	}
	if id := TraceIDFrom(ctx); id != "" {
		attrs = append(attrs, "trace_id", id)
	}
	if id := SpanIDFrom(ctx); id != "" {
		attrs = append(attrs, "span_id", id)
	}
	return a.log.With(attrs...)
}

// HTTP возвращает роутер для регистрации бизнес-роутов
// (встроенные /health и /ready уже зарегистрированы).
func (a *App) HTTP() *HTTPServer { return &HTTPServer{mux: a.mux} }

// AddHealthCheck регистрирует проверку готовности (агрегируется в /ready).
func (a *App) AddHealthCheck(name string, check HealthCheck) {
	a.health.Add(name, check)
}

// AddRunnable регистрирует фоновый воркер с graceful shutdown.
func (a *App) AddRunnable(name string, fn Runnable) {
	if fn != nil {
		a.runnables = append(a.runnables, namedRunnable{name: name, fn: fn})
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
		a.metrics = NewMetrics(true)
		a.mux.Handle("GET /metrics", a.metrics.Handler())
	}
	if a.opts.PprofEnabled {
		a.registerPprof()
	}

	// Цепочка "снаружи внутрь":
	// recover → request-id → trace → access-log → metrics → mux.
	handler := http.Handler(a.mux)
	if a.metrics != nil {
		handler = a.metrics.middleware(a.opts.ServiceName)(handler)
	}
	handler = accessLogMiddleware(a.log)(handler)
	handler = traceMiddleware(handler)
	handler = requestIDMiddleware(handler)
	handler = recoverMiddleware(a.log)(handler)

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

// HTTPServer — тонкая обёртка над *http.ServeMux для регистрации бизнес-роутов.
type HTTPServer struct{ mux *http.ServeMux }

// Handle регистрирует обработчик по паттерну (поддерж: "GET /tasks", "POST /tasks/{id}").
func (s *HTTPServer) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

// HandleFunc регистрирует функцию-обработчик по паттерну.
func (s *HTTPServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.mux.HandleFunc(pattern, handler)
}

// logFatal печатает ошибку в stderr.
func logFatal(err error) {
	fmt.Fprintf(os.Stderr, "parker: %v\n", err)
}
