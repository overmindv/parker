//go:build component

package parker

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestGeneratedServiceBuildsWithoutSibling(t *testing.T) {
	for _, pg := range []bool{false, true} {
		dir := t.TempDir()
		if err := GenerateService(dir, "isolated", pg, false); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"mod", "tidy"}, {"build", "./..."}, {"test", "./..."}} {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			cmd := exec.CommandContext(ctx, "go", args...)
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "GOWORK=off")
			output, err := cmd.CombinedOutput()
			cancel()
			if err != nil {
				t.Fatalf("isolated pg=%v go %v: %v\n%s", pg, args, err, output)
			}
		}
	}
}
