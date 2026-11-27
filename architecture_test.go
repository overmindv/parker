package parker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// rootFacadeFiles — разрешённый состав корневого пакета: только публичный фасад
// и типы контракта. Реализация живёт в internal/<домен>.
var rootFacadeFiles = map[string]bool{
	"app.go":            true,
	"config.go":         true,
	"health.go":         true,
	"http.go":           true,
	"init.go":           true,
	"kafka.go":          true,
	"kafka_security.go": true,
	"lifecycle.go":      true,
	"log.go":            true,
	"metrics.go":        true,
	"middleware.go":     true,
	"migrate.go":        true,
	"options.go":        true,
	"outbox.go":         true,
	"postgres.go":       true,
	"trace_http.go":     true,
	"tracing.go":        true,
}

var internalDomains = []string{
	"cli",
	"env",
	"health",
	"httpkit",
	"kafka",
	"lifecycle",
	"logging",
	"migrate",
	"observability",
	"outbox",
	"postgres",
	"requestid",
	"servicegen",
}

func TestInfrastructureImplementationLivesInInternalPackages(t *testing.T) {
	t.Parallel()

	for _, name := range internalDomains {
		path := filepath.Join("internal", name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("инфраструктурный пакет %s не найден: %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("%s должен быть директорией", path)
		}
		files, err := filepath.Glob(filepath.Join(path, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("пакет %s должен содержать Go-файлы", path)
		}
	}
}

func TestRootContainsOnlyPublicFacade(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if !rootFacadeFiles[name] {
			t.Errorf("файл %s не входит в публичный фасад: реализацию добавляйте в internal/<домен>", name)
		}
	}
}
