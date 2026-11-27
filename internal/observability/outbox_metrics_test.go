package observability

import "testing"

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
