package parker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateServiceName(t *testing.T) {
	valid := []string{"tasks", "user-service", "a", "api-gateway", "svc2x"}
	for _, name := range valid {
		if err := ValidateServiceName(name); err != nil {
			t.Errorf("имя %q должно быть валидным: %v", name, err)
		}
	}
	invalid := []string{"", "Tasks", "my service", "my_service", "-bad", "averylongnameoverfortycharacterslongxxxxxx", "1x!"}
	for _, name := range invalid {
		if err := ValidateServiceName(name); err == nil {
			t.Errorf("имя %q должно быть отклонено", name)
		}
	}
}

func TestGenerateService_WithPG(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateService(dir, "demo", true, false); err != nil {
		t.Fatal(err)
	}

	expectFiles := []string{
		"go.mod",
		filepath.Join("cmd", "demo", "main.go"),
		"Makefile",
		"Dockerfile",
		".env.example",
		".gitignore",
		filepath.Join(".github", "workflows", "ci.yml"),
		"README.md",
		filepath.Join("migrations", "0001_init.sql"),
		filepath.Join("internal", "demo", "domain", "README.md"),
		filepath.Join("internal", "demo", "usecase", "README.md"),
		filepath.Join("internal", "demo", "repository", "README.md"),
		filepath.Join("internal", "demo", "transport", "README.md"),
	}
	for _, f := range expectFiles {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("ожидался файл %s: %v", f, err)
		}
	}

	gomod, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	gomodStr := string(gomod)
	if !strings.Contains(gomodStr, "module github.com/overmindv/demo") {
		t.Errorf("go.mod должен содержать module demo:\n%s", gomodStr)
	}
	if !strings.Contains(gomodStr, "replace github.com/overmindv/parker => ../parker") {
		t.Errorf("go.mod должен содержать replace => ../parker:\n%s", gomodStr)
	}

	main, err := os.ReadFile(filepath.Join(dir, "cmd", "demo", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	mainStr := string(main)
	if !strings.Contains(mainStr, `parker.Main(run, parker.WithAppName("demo"))`) {
		t.Errorf("main.go должен вызывать parker.Main с WithAppName:\n%s", mainStr)
	}
	if !strings.Contains(mainStr, "app.Postgres()") {
		t.Errorf("main.go (с --pg) должен использовать app.Postgres():\n%s", mainStr)
	}
}

func TestGenerateService_NoPG(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateService(dir, "lite", false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "migrations")); !os.IsNotExist(err) {
		t.Fatal("без --pg не должно быть migrations/")
	}
	main, err := os.ReadFile(filepath.Join(dir, "cmd", "lite", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(main), "app.Postgres()") {
		t.Error("main.go без --pg не должен использовать app.Postgres()")
	}
}

func TestGenerateService_InvalidName(t *testing.T) {
	if err := GenerateService(t.TempDir(), "My Service", true, false); err == nil {
		t.Fatal("ожидалась ошибка при недопустимом имени")
	}
}

func TestComposeBlockContainsService(t *testing.T) {
	var sb strings.Builder
	PrintComposeBlock(&sb, "demo", true, true)
	out := sb.String()
	for _, want := range []string{"demo-postgres", "demo-migrate", "  demo:", "KAFKA_BOOTSTRAP_SERVERS"} {
		if !strings.Contains(out, want) {
			t.Errorf("compose-блок должен содержать %q", want)
		}
	}
}
