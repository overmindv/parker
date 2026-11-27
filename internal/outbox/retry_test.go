package outbox

import (
	"testing"
	"time"
)

func TestOutboxBackoffUsesClockAndBoundedJitter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	for attempt := 1; attempt <= 100; attempt++ {
		low := retryAt(now, attempt, 0)
		high := retryAt(now, attempt, 1)
		if !low.After(now) || high.Before(low) || high.Sub(now) > 5*time.Minute {
			t.Fatalf("unbounded retry %d: %s %s", attempt, low, high)
		}
		if attempt > 1 && retryAt(now, attempt-1, 0).After(low) {
			t.Fatal("decreasing backoff")
		}
	}
}
