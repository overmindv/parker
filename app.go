package parker

import (
	"context"
	"log/slog"
	"os"

	"github.com/overmindv/parker/internal/app"
	"github.com/overmindv/parker/internal/config"
	"github.com/overmindv/parker/internal/lifecycle"
)

// Version — версия parker, печатается командой `version`.
const Version = "0.3.1"

// App — единая точка входа прикладного сервиса: всё инфраструктурное уже собрано.
// Сервис в вызываемом run(*App) регистрирует бизнес-роуты, health-чекки и воркеры.
type App app.App

// Main запускает приложение из os.Args. Эквивалент MainArgs(run, os.Args[1:], opts...).
func Main(run func(*App) error, opts ...Option) int {
	return MainArgs(run, os.Args[1:], opts...)
}

// MainArgs запускает приложение с заданным списком аргументов.
// Подкоманды: serve (по умолчанию), migrate, init, version.
func MainArgs(run func(*App) error, args []string, opts ...Option) int {
	return app.MainArgs(args, Version, app.AdaptRun(run, func(a *app.App) *App { return (*App)(a) }),
		func(o config.Options) config.Options { return publicOptions(o).Apply(opts).settings() })
}

// Config возвращает конфигурацию parker.
func (a *App) Config() Options { return publicOptions((*app.App)(a).Config()) }

// Logger возвращает структурный логгер сервиса.
func (a *App) Logger() *slog.Logger { return (*app.App)(a).Logger() }

// LoggerFor возвращает логгер с полями трассировки из контекста запроса
// (request_id/trace_id/span_id). Используется бизнес-логикой для связных логов.
func (a *App) LoggerFor(ctx context.Context) *slog.Logger {
	return (*app.App)(a).LoggerFor(ctx)
}

// HTTP возвращает роутер для регистрации бизнес-роутов
// (встроенные /health и /ready уже зарегистрированы).
func (a *App) HTTP() *HTTPServer { return (*HTTPServer)((*app.App)(a).HTTP()) }

// AddHealthCheck регистрирует проверку готовности (агрегируется в /ready).
func (a *App) AddHealthCheck(name string, check HealthCheck) {
	(*app.App)(a).AddHealthCheck(name, check)
}

// AddRunnable регистрирует фоновый воркер с graceful shutdown.
func (a *App) AddRunnable(name string, fn Runnable) {
	(*app.App)(a).AddRunnable(name, lifecycle.Runnable(fn))
}

// Metrics возвращает регистр приложения для общих инфраструктурных workers.
// При выключенных метриках возвращает nil.
func (a *App) Metrics() *Metrics { return (*Metrics)((*app.App)(a).Metrics()) }
