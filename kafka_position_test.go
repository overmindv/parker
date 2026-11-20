package parker

import (
	"context"
	"errors"
	"testing"
)

func TestSubscriberPreservesPositionOnRetry(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, ok := PositionFromContext(ctx); ok {
		t.Fatal("unexpected position outside subscriber")
	}

	client := &prefixClient{cancel: cancel}
	var got []RecordPosition
	subscriber := &Subscriber{client: client, handler: func(ctx context.Context, _ string, _ []byte) error {
		position, ok := PositionFromContext(ctx)
		if !ok {
			t.Error("missing record position")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("missing record deadline")
		}
		got = append(got, position)
		if len(got) == 1 {
			return errors.New("try again")
		}

		return nil
	}}

	if err := subscriber.Run(ctx); err != nil {
		t.Fatal(err)
	}
	want := []RecordPosition{{Topic: "t"}, {Topic: "t"}, {Topic: "t", Offset: 1}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatal(got)
	}
}
