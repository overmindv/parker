package parker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKafkaSecurityRejectsUnsafeConfigurations(t *testing.T) {
	t.Parallel()

	cases := []KafkaSecurity{
		{Environment: "production"},
		{SASLMechanism: "PLAIN", Username: "u", Password: "sensitive"},
		{TLS: true, SASLMechanism: "unknown", Username: "u", Password: "sensitive"},
		{TLS: true, SASLMechanism: "SCRAM-SHA-256", Username: "u"},
		{TLS: true, Password: "sensitive"},
		{CAFile: "missing"},
		{TLS: true, CertFile: "missing"},
		{TLS: true, CAFile: "missing"},
	}
	for _, c := range cases {
		if _, err := kafkaSecurityOptions(c); err == nil {
			t.Fatalf("accepted invalid config: %v", c)
		} else if strings.Contains(err.Error(), "sensitive") {
			t.Fatal("secret in error")
		}
	}
}
func TestKafkaSecurityIsSharedAndRedacted(t *testing.T) {
	t.Parallel()

	security := KafkaSecurity{TLS: true, SASLMechanism: "SCRAM-SHA-512", Username: "private-user", Password: "sensitive"}
	if strings.Contains(fmt.Sprintf("%+v %#v", security, security), "sensitive") {
		t.Fatal("secret in formatted config")
	}
	encoded, err := json.Marshal(security)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "sensitive") {
		t.Fatal("secret in JSON config")
	}
	p, err := NewProducer([]string{"localhost:1"}, security)
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	s, err := NewSubscriber([]string{"localhost:1"}, "g", []string{"t"}, func(_ context.Context, _ string, _ []byte) error { return nil }, discardLog(), security)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}

//nolint:paralleltest // t.Setenv изменяет process-wide environment
func TestKafkaSecurityEnvFailsClosed(t *testing.T) {
	t.Setenv("KAFKA_TLS_ENABLED", "typo")
	if _, err := Load(); err == nil {
		t.Fatal("invalid TLS flag accepted")
	}
	t.Setenv("KAFKA_TLS_ENABLED", "true")
	t.Setenv("KAFKA_SASL_MECHANISM", "PLAIN")
	t.Setenv("KAFKA_SASL_USERNAME", "user")
	t.Setenv("KAFKA_SASL_PASSWORD", " secret ")
	o, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if o.KafkaSecurity.Password != " secret " {
		t.Fatal("password whitespace changed")
	}
}

func TestKafkaRejectsMalformedCertificatesAndAppDowngrade(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(file, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, security := range []KafkaSecurity{{TLS: true, CAFile: file}, {TLS: true, CertFile: file, KeyFile: file}} {
		if _, err := kafkaSecurityOptions(security); err == nil {
			t.Fatal("malformed certificate accepted")
		}
	}
	app := &App{opts: Options{Environment: "production", KafkaBrokers: []string{"localhost:1"}, KafkaSecurity: KafkaSecurity{Environment: "local"}}}
	if _, err := app.NewProducer(); err == nil {
		t.Fatal("app environment overridden")
	}
}
