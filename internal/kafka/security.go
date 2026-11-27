// Package kafka содержит producer/subscriber и настройки безопасности Kafka.
package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/overmindv/parker/internal/env"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	kafkaSecurityRedacted = "KafkaSecurity(redacted)"
	saslPlain             = "PLAIN"
	saslSCRAMSHA256       = "SCRAM-SHA-256"
	saslSCRAMSHA512       = "SCRAM-SHA-512"

	environmentLocal       = "local"
	environmentDevelopment = "development"
	environmentTest        = "test"
	environmentStaging     = "staging"
	environmentProduction  = "production"
)

// Security — общие настройки producer/subscriber. Нулевое значение сохраняет local plaintext.
// Production требует TLS; проверка CA и hostname никогда не отключается.
type Security struct {
	Environment   string
	TLS           bool
	CAFile        string
	CertFile      string
	KeyFile       string
	ServerName    string
	SASLMechanism string
	Username      string
	Password      string `json:"-"`
}

// String/LogValue исключают credentials из обычного форматирования и slog.
func (Security) GoString() string { return kafkaSecurityRedacted }

func (Security) String() string         { return kafkaSecurityRedacted }
func (s Security) LogValue() slog.Value { return slog.StringValue(s.String()) }

func (s Security) validate() error {
	switch s.Environment {
	case "", environmentLocal, environmentDevelopment, environmentTest, environmentStaging, environmentProduction:
	default:
		return fmt.Errorf("invalid Kafka environment")
	}
	if s.Environment == environmentProduction && !s.TLS {
		return fmt.Errorf("kafka TLS required in production")
	}
	if !s.TLS && (s.CAFile != "" || s.CertFile != "" || s.KeyFile != "" || s.ServerName != "") {
		return fmt.Errorf("kafka TLS options require TLS")
	}
	if (s.CertFile == "") != (s.KeyFile == "") {
		return fmt.Errorf("kafka client certificate and key must be paired")
	}
	switch s.SASLMechanism {
	case "":
		if s.Username != "" || s.Password != "" {
			return fmt.Errorf("kafka SASL mechanism required")
		}
	case saslPlain, saslSCRAMSHA256, saslSCRAMSHA512:
		if !s.TLS {
			return fmt.Errorf("kafka SASL requires TLS")
		}
		if s.Username == "" || s.Password == "" {
			return fmt.Errorf("kafka SASL credentials required")
		}
	default:
		return fmt.Errorf("unsupported Kafka SASL mechanism")
	}

	return nil
}

func (s Security) validateConfiguration() error {
	s.Environment = ""

	return s.validate()
}

// ValidateConfiguration проверяет форму настроек до фактического подключения.
func ValidateConfiguration(s Security) error {
	return s.validateConfiguration()
}

// SecurityOptions возвращает базовые kgo-опции (TLS/SASL) для Kafka-клиентов.
func SecurityOptions(settings ...Security) ([]kgo.Opt, error) {
	if len(settings) > 1 {
		return nil, fmt.Errorf("only one Kafka security configuration allowed")
	}
	var s Security
	if len(settings) == 0 {
		loaded, err := LoadSecurity(env.String("ENV", environmentLocal))
		if err != nil {
			return nil, err
		}
		s = loaded
	} else {
		s = settings[0]
	}
	runtimeEnvironment := env.String("ENV", environmentLocal)
	if runtimeEnvironment == environmentProduction || s.Environment == "" {
		s.Environment = runtimeEnvironment
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	var opts []kgo.Opt
	if s.TLS {
		config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: s.ServerName}
		if s.CAFile != "" {
			pem, err := os.ReadFile(s.CAFile)
			if err != nil {
				return nil, fmt.Errorf("cannot read Kafka CA certificate")
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("invalid Kafka CA certificate")
			}
			config.RootCAs = roots
		}
		if s.CertFile != "" {
			cert, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("invalid Kafka client certificate or key")
			}
			config.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, kgo.DialTLSConfig(config))
	}
	switch s.SASLMechanism {
	case saslPlain:
		opts = append(opts, kgo.SASL(plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism()))
	case saslSCRAMSHA256:
		opts = append(opts, kgo.SASL(scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism()))
	case saslSCRAMSHA512:
		opts = append(opts, kgo.SASL(scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism()))
	}

	return opts, nil
}

// LoadSecurity читает настройки Kafka из окружения.
func LoadSecurity(environment string) (Security, error) {
	s := Security{
		Environment:   environment,
		CAFile:        env.String("KAFKA_TLS_CA_FILE", ""),
		CertFile:      env.String("KAFKA_TLS_CERT_FILE", ""),
		KeyFile:       env.String("KAFKA_TLS_KEY_FILE", ""),
		ServerName:    env.String("KAFKA_TLS_SERVER_NAME", ""),
		SASLMechanism: strings.ToUpper(env.String("KAFKA_SASL_MECHANISM", "")),
		Username:      os.Getenv("KAFKA_SASL_USERNAME"),
		Password:      os.Getenv("KAFKA_SASL_PASSWORD"),
	}
	if value := strings.TrimSpace(os.Getenv("KAFKA_TLS_ENABLED")); value != "" {
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return s, fmt.Errorf("invalid KAFKA_TLS_ENABLED")
		}
		s.TLS = enabled
	}

	return s, s.validateConfiguration()
}
