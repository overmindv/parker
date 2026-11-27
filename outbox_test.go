package parker

import "testing"

func TestPgOutboxFetchPendingPreservesNil(t *testing.T) {
	t.Parallel()

	records, err := NewPgOutbox(nil).FetchPending(t.Context(), 0)
	if err != nil || records != nil {
		t.Fatalf("при limit=0 ожидается nil без ошибок, получено %#v, %v", records, err)
	}
}
