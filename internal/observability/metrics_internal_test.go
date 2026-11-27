package observability

import "testing"

const (
	healthPath = "/health"
	tasksPath  = "/tasks"
)

func TestNormalizeRoute(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		healthPath:      healthPath,
		tasksPath:       tasksPath,
		"/tasks/123":    "/tasks/{id}",
		"/tasks/42/sub": "/tasks/{id}/sub",
		"/users/550e8400-e29b-41d4-a716-446655440000": "/users/{id}",
	}
	for in, want := range cases {
		if got := normalizeRoute(in); got != want {
			t.Errorf("normalizeRoute(%q) = %q, ожидалось %q", in, got, want)
		}
	}
}

func TestLooksLikeID(t *testing.T) {
	t.Parallel()

	if !looksLikeID("123") || !looksLikeID("550e8400-e29b-41d4-a716-446655440000") {
		t.Fatal("число и UUID должны распознаваться как id")
	}
	if looksLikeID("alpha") || looksLikeID("") {
		t.Fatal("строка/пустота не должны распознаваться как id")
	}
}
