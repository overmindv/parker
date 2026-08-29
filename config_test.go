package parker

import (
	"strings"
	"testing"
	"time"
)

func TestEnv_EmptyUsesFallback(t *testing.T) {
	t.Setenv("PARKER_TEST_ENV_1", "")
	if got := Env("PARKER_TEST_ENV_1", "fallback"); got != "fallback" {
		t.Fatalf("ожидался fallback, получили %q", got)
	}

	t.Setenv("PARKER_TEST_ENV_2", "  value  ")
	if got := Env("PARKER_TEST_ENV_2", "fallback"); got != "value" {
		t.Fatalf("ожидалось value, получили %q", got)
	}
}

func TestEnvList_DedupAndTrim(t *testing.T) {
	t.Setenv("PARKER_TEST_LIST", " a,b ,a,,c ")
	got := EnvList("PARKER_TEST_LIST", "x")
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("ожидалось %v, получили %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("элемент %d: ожидалось %q, получили %q", i, want[i], got[i])
		}
	}
}

func TestEnvDuration(t *testing.T) {
	t.Setenv("PARKER_TEST_DUR", "5s")
	got, err := EnvDuration("PARKER_TEST_DUR", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != 5*time.Second {
		t.Fatalf("ожидалось 5s, получили %v", got)
	}

	t.Setenv("PARKER_TEST_DUR", "notaduration")
	if _, err := EnvDuration("PARKER_TEST_DUR", time.Second); err == nil {
		t.Fatal("ожидалась ошибка для некорректного duration")
	}
}

func TestEnvInt64(t *testing.T) {
	t.Setenv("PARKER_TEST_INT", "42")
	got, err := EnvInt64("PARKER_TEST_INT", 0)
	if err != nil || got != 42 {
		t.Fatalf("ожидалось 42, получили %d err=%v", got, err)
	}

	t.Setenv("PARKER_TEST_INT", "abc")
	if _, err := EnvInt64("PARKER_TEST_INT", 0); err == nil {
		t.Fatal("ожидалась ошибка для некорректного int")
	}
}

func TestLoad_Defaults(t *testing.T) {
	// Чистое окружение parker-переменных не требуется: Load применяет DEFAULT_VALUE.
	// Сбрасываем привязанные env-переменные, чтобы не зависеть от окружения машины.
	for _, k := range []string{"SERVICE_NAME", "HTTP_ADDR", "ENV", "LOG_LEVEL",
		"READ_TIMEOUT", "WRITE_TIMEOUT", "SHUTDOWN_TIMEOUT", "DATABASE_URL", "MIGRATIONS_DIR",
		"KAFKA_BOOTSTRAP_SERVERS"} {
		t.Setenv(k, "")
	}

	opts, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if opts.ServiceName != "parker" {
		t.Fatalf("ожидался дефолтный SERVICE_NAME=parker, получили %q", opts.ServiceName)
	}
	if opts.HTTPAddress != ":8080" {
		t.Fatalf("ожидался дефолтный HTTP_ADDR=:8080, получили %q", opts.HTTPAddress)
	}
	if opts.ReadTimeout != 10*time.Second {
		t.Fatalf("ожидался дефолтный READ_TIMEOUT, получили %v", opts.ReadTimeout)
	}
}

func TestLoad_InvalidLogLevel(t *testing.T) {
	t.Setenv("LOG_LEVEL", "bogus")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "LOG_LEVEL") {
		t.Fatalf("ожидалась ошибка про LOG_LEVEL, получили %v", err)
	}
}

func TestLoad_InvalidTimeout(t *testing.T) {
	t.Setenv("READ_TIMEOUT", "0s")
	if _, err := Load(); err == nil {
		t.Fatal("ожидалась ошибка про READ_TIMEOUT")
	}
}

func TestValidate_InvalidDatabaseURL(t *testing.T) {
	opts := DefaultOptions()
	opts.DatabaseURL = "mysql://user:pass@localhost/db"
	if err := opts.Validate(); err == nil {
		t.Fatal("ожидалась ошибка про DATABASE_URL")
	}
}
