package parker

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestHealthPublicTypesRemainInParkerPackage(t *testing.T) {
	t.Parallel()

	for name, typ := range map[string]reflect.Type{
		"HealthCheck":     reflect.TypeOf((*HealthCheck)(nil)).Elem(),
		"HealthCheckFunc": reflect.TypeOf(HealthCheckFunc(nil)),
		"HealthRegistry":  reflect.TypeOf(HealthRegistry{}),
	} {
		if typ.Name() != name || typ.PkgPath() != "github.com/overmindv/parker" {
			t.Fatalf("%s потерял identity публичного parker-типа: name=%q pkg=%q", name, typ.Name(), typ.PkgPath())
		}
	}
}

func TestHealthRegistryZeroValueIsReady(t *testing.T) {
	t.Parallel()

	var registry HealthRegistry
	if registry.Enabled() {
		t.Fatal("нулевой реестр не должен быть enabled")
	}
	if err := registry.Ready(context.Background()); err != nil {
		t.Fatalf("нулевой реестр должен быть ready: %v", err)
	}
}

func TestHealthRegistry_ReadyAllOk(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	h := NewHealthRegistry()
	if h.Enabled() {
		t.Fatal("пустой реестр не enabled")
	}
	if err := h.Ready(context.Background()); err != nil {
		t.Fatalf("пустой реестр всегда ready: %v", err)
	}
}
