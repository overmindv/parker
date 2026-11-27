package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type prefixClient struct {
	polls, allows int
	committed     []int64
	failCommit    bool
	cancel        context.CancelFunc
}

func (c *prefixClient) PollRecords(ctx context.Context, n int) kgo.Fetches {
	c.polls++
	if c.polls > 1 {
		c.cancel()

		return nil
	}

	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "t", Partitions: []kgo.FetchPartition{{Partition: 0, Records: []*kgo.Record{{Topic: "t", Offset: 0}, {Topic: "t", Offset: 1}}}}}}}}
}
func (c *prefixClient) CommitRecords(_ context.Context, r ...*kgo.Record) error {
	if c.failCommit {
		return errors.New("commit failed")
	}
	for _, v := range r {
		c.committed = append(c.committed, v.Offset)
	}

	return nil
}
func (c *prefixClient) AllowRebalance()            { c.allows++ }
func (c *prefixClient) Ping(context.Context) error { return nil }
func (c *prefixClient) Close()                     {}
func TestSubscriberFailedPrefixStopsBatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &prefixClient{cancel: cancel}
	calls := 0
	s := &Subscriber{client: c, log: slog.New(slog.NewTextHandler(io.Discard, nil)), handler: func(context.Context, string, []byte) error {
		calls++

		return errors.New("failed")
	}}
	if err := s.Run(ctx); err == nil {
		t.Fatal("expected exhausted retry")
	}
	if len(c.committed) != 0 || calls != 3 || c.allows != 1 {
		t.Fatalf("commits=%v calls=%d allows=%d", c.committed, calls, c.allows)
	}
}
func TestSubscriberRetriesCurrentBeforeNext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &prefixClient{cancel: cancel}
	calls := 0
	s := &Subscriber{client: c, log: slog.New(slog.NewTextHandler(io.Discard, nil)), handler: func(context.Context, string, []byte) error {
		calls++
		if calls == 1 {
			return errors.New("first")
		}

		return nil
	}}
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || len(c.committed) != 2 || c.committed[0] != 0 || c.committed[1] != 1 {
		t.Fatalf("commits=%v calls=%d", c.committed, calls)
	}
}
func TestSubscriberCommitFailureStopsBatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &prefixClient{cancel: cancel, failCommit: true}
	calls := 0
	s := &Subscriber{client: c, log: slog.New(slog.NewTextHandler(io.Discard, nil)), handler: func(context.Context, string, []byte) error {
		calls++

		return nil
	}}
	if err := s.Run(ctx); err == nil {
		t.Fatal("ignored commit failure")
	}
	if calls != 1 || c.allows != 1 {
		t.Fatalf("calls=%d allows=%d", calls, c.allows)
	}
}

func TestSubscriberBlocksRebalanceWhileProcessing(t *testing.T) {
	t.Parallel()

	s, err := NewSubscriber([]string{"localhost:1"}, "g", []string{"t"}, func(context.Context, string, []byte) error { return nil }, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.client.(*kgo.Client).OptValue(kgo.BlockRebalanceOnPoll) != true {
		t.Fatal("rebalance must be blocked during processing")
	}
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
