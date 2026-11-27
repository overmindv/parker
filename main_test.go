package parker

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// captureOutput перехватывает вывод в os.Stdout или os.Stderr во время вызова fn.
func captureOutput(stream **os.File, fn func()) string {
	old := *stream
	r, w, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	*stream = w
	defer func() { *stream = old }()

	fn()
	_ = w.Close()

	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	_ = r.Close()

	return string(buf[:n])
}

//nolint:paralleltest // captureOutput временно заменяет process-wide os.Stdout
func TestMainArgs_Version(t *testing.T) {
	out := captureOutput(&os.Stdout, func() {
		if code := MainArgs(nil, []string{"version"}); code != 0 {
			t.Fatalf("version: ожидался 0, получили %d", code)
		}
	})
	if !strings.Contains(out, Version) {
		t.Fatalf("version должна печатать версию, получили %q", out)
	}
}

//nolint:paralleltest // captureOutput временно заменяет process-wide os.Stderr
func TestMainArgs_UnknownCommand(t *testing.T) {
	out := captureOutput(&os.Stderr, func() {
		if code := MainArgs(nil, []string{"frobnicate"}); code != 2 {
			t.Fatalf("неизвестная команда: ожидался 2, получили %d", code)
		}
	})
	if !strings.Contains(out, "frobnicate") {
		t.Fatalf("stderr должен содержать имя команды, получили %q", out)
	}
}

func TestMainArgsPreservesPublicOptions(t *testing.T) {
	t.Parallel()

	want := Options{
		ServiceName: "facade", HTTPAddress: ":0", Environment: "test", LogLevel: "warn",
		ReadTimeout: time.Second, WriteTimeout: 2 * time.Second, ShutdownTimeout: 3 * time.Second,
		DatabaseURL: "postgres://localhost/demo", MigrationsDir: "custom/migrations",
		KafkaSecurity: KafkaSecurity{TLS: true, ServerName: "broker"},
		KafkaBrokers:  []string{"broker:9092"}, MetricsEnabled: true, PprofEnabled: true,
		Tracing: TraceOptions{Endpoint: "http://localhost:4318", SampleRatio: 0.7},
	}
	called := false
	code := MainArgs(func(a *App) error {
		called = true
		if !reflect.DeepEqual(a.Config(), want) {
			t.Fatal("App.Config() потерял поля Options")
		}

		return errors.New("проверка инициализации завершена")
	}, nil, func(o *Options) { *o = want; o.ServiceName = "initial" }, nil, WithAppName(want.ServiceName))
	if !called || code != 1 {
		t.Fatal("ошибка пользовательского callback должна прерывать запуск")
	}
}

func TestMainArgsServeWithNilCallback(t *testing.T) {
	t.Parallel()

	code := MainArgs(nil, []string{"serve"}, nil, func(o *Options) { o.HTTPAddress = "invalid-address" })
	if code != 1 {
		t.Fatalf("ошибка запуска HTTP-сервера должна вернуть код 1, получили %d", code)
	}
}
