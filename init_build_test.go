//go:build component

package parker

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestGenerateService_Builds проверяет, что сгенерированный сервис собирается
// против соседнего ../parker (запускается в CI, где есть сеть для go mod download).
func TestGenerateService_Builds(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	temp := t.TempDir()

	// Создаём layout, где ../parker указывает на реальный parker этого репозитория.
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	parkerDir, err := filepath.Abs(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	outRoot := filepath.Join(temp, "root")
	svcDir := filepath.Join(outRoot, "embedded-demo")
	if err := os.MkdirAll(outRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(parkerDir, filepath.Join(outRoot, "parker")); err != nil {
		t.Fatal(err)
	}
	if err := GenerateService(svcDir, "embedded-demo", false, false); err != nil {
		t.Fatal(err)
	}

	run := func(name string, args ...string) {
		t.Helper()
		cmd := exec.Command(name, args...)
		cmd.Dir = svcDir
		cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, out)
		}
	}

	run("go", "mod", "tidy")
	run("go", "build", "./...")
	run("go", "vet", "./...")
}
