package parker

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestHealthRegistry_ReadyAllOk(t *testing.T) {
	h := NewHealthRegistry()
	h.Add("a", HealthCheckFunc(func(context.Context) error { return nil }))
	h.Add("b", HealthCheckFunc(func(context.Context) error { return nil }))
	if !h.Enabled() {
		t.Fatal("реестр должен быть enabled после Add")
	}
	if err := h.Ready(context.Background()); err != nil {
		t.Fatalf("ожидался nil, получили %v", err)
	}
}

func TestHealthRegistry_ReadyFail(t *testing.T) {
	h := NewHealthRegistry()
	h.Add("db", HealthCheckFunc(func(context.Context) error { return errors.New("connection refused") }))
	err := h.Ready(context.Background())
	if err == nil {
		t.Fatal("ожидалась ошибка")
	}
	if got := err.Error(); !strings.Contains(got, "db") {
		t.Fatalf("ошибка должна содержать имя чека, получили %q", got)
	}
}

func TestHealthRegistry_EmptyReadyOk(t *testing.T) {
	h := NewHealthRegistry()
	if h.Enabled() {
		t.Fatal("пустой реестр не enabled")
	}
	if err := h.Ready(context.Background()); err != nil {
		t.Fatalf("пустой реестр всегда ready: %v", err)
	}
}
