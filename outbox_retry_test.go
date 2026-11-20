package parker

import (
	"testing"
	"time"
)

func TestOutboxBackoffUsesClockAndBoundedJitter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	for attempt := 1; attempt <= 100; attempt++ {
		low := outboxRetryAt(now, attempt, 0)
		high := outboxRetryAt(now, attempt, 1)
		if !low.After(now) || high.Before(low) || high.Sub(now) > 5*time.Minute {
			t.Fatalf("unbounded retry %d: %s %s", attempt, low, high)
		}
		if attempt > 1 && outboxRetryAt(now, attempt-1, 0).After(low) {
			t.Fatal("decreasing backoff")
		}
	}
}

func TestOutboxMetricsExposeQueueAge(t *testing.T) {
	t.Parallel()

	m := NewMetrics(true)
	m.outboxAge.Set(123)
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "parker_outbox_oldest_age_seconds" {
			if f.Metric[0].GetGauge().GetValue() != 123 {
				t.Fatal("wrong age")
			}

			return
		}
	}
	t.Fatal("missing queue age metric")
}
