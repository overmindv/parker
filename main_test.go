package parker

import (
	"os"
	"strings"
	"testing"
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
