package parker

import (
	"time"

	"github.com/overmindv/parker/internal/config"
	internalenv "github.com/overmindv/parker/internal/env"
)

// Load читает стандартные env-переменные parker в Options поверх DefaultOptions()
// и валидирует результат.
func Load() (Options, error) {
	settings, err := config.Load()

	return publicOptions(settings), err
}

// Validate проверяет обязательные и ограниченные настройки.
func (o Options) Validate() error { return o.settings().Validate() }

// Env возвращает очищенную переменную окружения или fallback.
func Env(key, fallback string) string {
	return internalenv.String(key, fallback)
}

// EnvDuration разбирает duration из окружения или возвращает fallback.
func EnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	return internalenv.Duration(key, fallback)
}

// EnvInt64 разбирает int64 из окружения или возвращает fallback.
func EnvInt64(key string, fallback int64) (int64, error) {
	return internalenv.Int64(key, fallback)
}

// EnvBool разбирает boolean из окружения (true/false/1/0/yes/no) или возвращает fallback.
func EnvBool(key string, fallback bool) bool {
	return internalenv.Bool(key, fallback)
}

// EnvList разбирает непустой comma-separated список без дубликатов.
func EnvList(key, fallback string) []string {
	return internalenv.List(key, fallback)
}
