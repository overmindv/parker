//go:build component

package parker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Реальный broker: неуспешный consumer уходит, новый member получает тот же offset.
func TestKafkaFailedRecordSurvivesNewMember(t *testing.T) {
	broker := os.Getenv("PARKER_TEST_KAFKA")
	if broker == "" {
		t.Skip("PARKER_TEST_KAFKA required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	topic := fmt.Sprintf("parker-prefix-%d", time.Now().UnixNano())
	group := topic + "-group"
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for _, v := range []string{"first", "second"} {
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(v)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := NewSubscriber([]string{broker}, group, []string{topic}, func(context.Context, string, []byte) error { return errors.New("fail") }, log)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Run(ctx); err == nil {
		t.Fatal("expected failure")
	}
	first.Close()
	var received []string
	second, err := NewSubscriber([]string{broker}, group, []string{topic}, func(_ context.Context, _ string, v []byte) error {
		received = append(received, string(v))
		if len(received) == 2 {
			cancel()
		}
		return nil
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(received) != 2 || received[0] != "first" || received[1] != "second" {
		t.Fatalf("lost prefix: %v", received)
	}
}

func TestKafkaRebalanceWaitsForInFlightHandler(t *testing.T) {
	broker := os.Getenv("PARKER_TEST_KAFKA")
	if broker == "" {
		t.Skip("PARKER_TEST_KAFKA required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	topic := fmt.Sprintf("parker-rebalance-%d", time.Now().UnixNano())
	group := topic + "-g"
	p, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("first")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	first, err := NewSubscriber([]string{broker}, group, []string{topic}, func(c context.Context, _ string, _ []byte) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-c.Done():
			return c.Err()
		}
	}, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	done1 := make(chan error, 1)
	go func() { done1 <- first.Run(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first did not enter")
	}
	delivered := make(chan struct{}, 1)
	second, err := NewSubscriber([]string{broker}, group, []string{topic}, func(context.Context, string, []byte) error {
		select {
		case delivered <- struct{}{}:
		default:
		}
		return nil
	}, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	done2 := make(chan error, 1)
	go func() { done2 <- second.Run(ctx) }()
	// Второй member присоединяется, пока первый держит неподтверждённую запись.
	select {
	case <-delivered:
		t.Error("rebalance stole in-flight partition")
	case <-time.After(time.Second):
	}
	close(release)
	cancel()
	<-done1
	<-done2
	first.Close()
	second.Close()
}
