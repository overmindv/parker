//go:build component

package parker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestAuthenticatedKafka(t *testing.T) {

	broker := os.Getenv("PARKER_SECURE_KAFKA")
	if broker == "" {
		t.Skip("secure Kafka fixture required")
	}
	security := KafkaSecurity{Environment: "production", TLS: true, CAFile: os.Getenv("PARKER_SECURE_CA"), SASLMechanism: "SCRAM-SHA-256", Username: "client", Password: os.Getenv("PARKER_SECURE_PASSWORD")}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p, err := NewProducer([]string{broker}, security)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.Publish(ctx, "ovm184-secure", "k", []byte("probe")); err != nil {
		t.Fatal(err)
	}
	received := false
	s, err := NewSubscriber([]string{broker}, "ovm184-group", []string{"ovm184-secure"}, func(context.Context, string, []byte) error { received = true; return nil }, discardLog(), security)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	committed := false
	s.client = &commitObserver{subscriberClient: s.client, cancel: cancel, committed: &committed}
	if err = s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !received || !committed {
		t.Fatal("no authenticated delivery")
	}
	t.Run("restricted ACL", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := p.Publish(ctx, "ovm184-denied", "k", []byte("probe")); err == nil {
			t.Fatal("write outside ACL accepted")
		}
	})
	for _, kind := range []string{"password", "hostname", "trust"} {
		t.Run(kind, func(t *testing.T) {
			bad := security
			switch kind {
			case "password":
				bad.Password = "wrong"
			case "hostname":
				bad.ServerName = "not-localhost.invalid"
			case "trust":
				bad.CAFile = ""
			}
			client, err := NewProducer([]string{broker}, bad)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Ping(ctx); err == nil {
				t.Fatal("invalid security accepted")
			}
		})
	}
}

type commitObserver struct {
	subscriberClient
	cancel    context.CancelFunc
	committed *bool
}

func (c *commitObserver) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	err := c.subscriberClient.CommitRecords(ctx, records...)
	if err == nil {
		*c.committed = true
		c.cancel()
	}
	return err
}
