package parker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInfrastructureImplementationLivesInInternalPackages(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"env", "health", "logging"} {
		path := filepath.Join("internal", name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("инфраструктурный пакет %s не найден: %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("%s должен быть директорией", path)
		}
	}
}
