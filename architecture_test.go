package parker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
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
	"app",
	"config",
	"convert",
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

func TestFacadeHasNoControlFlow(t *testing.T) {
	t.Parallel()

	files := token.NewFileSet()
	for name := range rootFacadeFiles {
		file, err := parser.ParseFile(files, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.TypeSpec:
				if n.Name.IsExported() && n.Assign.IsValid() {
					t.Errorf("%s: публичный тип %s должен быть определён в корне", files.Position(n.Pos()), n.Name)
				}
			case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt,
				*ast.SelectStmt, *ast.GoStmt, *ast.DeferStmt, *ast.SendStmt, *ast.BranchStmt:
				t.Errorf("%s: управляющая логика должна находиться в internal", files.Position(node.Pos()))
			}

			return true
		})
	}
}

// Внешний код использует именованные публичные типы в литералах, интерфейсах и рефлексии.
func TestFacadeRetainsPublicTypeIdentities(t *testing.T) {
	t.Parallel()

	const module = "github.com/overmindv/parker"
	for _, typ := range []reflect.Type{
		reflect.TypeFor[App](), reflect.TypeFor[HTTPServer](), reflect.TypeFor[Handler](),
		reflect.TypeFor[HealthCheck](), reflect.TypeFor[HealthCheckFunc](), reflect.TypeFor[HealthRegistry](),
		reflect.TypeFor[KafkaSecurity](), reflect.TypeFor[Metrics](), reflect.TypeFor[Option](),
		reflect.TypeFor[Options](), reflect.TypeFor[Outbox](), reflect.TypeFor[OutboxDispatcher](),
		reflect.TypeFor[OutboxRecord](), reflect.TypeFor[PgOutbox](), reflect.TypeFor[Producer](),
		reflect.TypeFor[RecordPosition](), reflect.TypeFor[Runnable](), reflect.TypeFor[Runner](),
		reflect.TypeFor[Subscriber](), reflect.TypeFor[TraceOptions](),
	} {
		if typ.PkgPath() != module {
			t.Errorf("тип %s объявлен в %s", typ.Name(), typ.PkgPath())
		}
	}
	options := reflect.TypeFor[Options]()
	for name, want := range map[string]reflect.Type{
		"KafkaSecurity": reflect.TypeFor[KafkaSecurity](),
		"Tracing":       reflect.TypeFor[TraceOptions](),
	} {
		if field, ok := options.FieldByName(name); !ok || field.Type != want {
			t.Errorf("Options.%s должен иметь тип %s", name, want)
		}
	}
}

func TestRunnerKeepsPublishedMethodSet(t *testing.T) {
	t.Parallel()

	runner := reflect.TypeFor[*Runner]()
	if runner.NumMethod() != 2 || runner.Method(0).Name != "AddRunnable" || runner.Method(1).Name != "Run" {
		t.Error("публичные методы Runner должны соответствовать v0.3.0")
	}
}
