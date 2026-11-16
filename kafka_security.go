package parker

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// KafkaSecurity — общие настройки producer/subscriber. Нулевое значение сохраняет local plaintext.
// Production требует TLS; проверка CA и hostname никогда не отключается.
type KafkaSecurity struct {
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
func (KafkaSecurity) GoString() string { return "KafkaSecurity(redacted)" }

func (KafkaSecurity) String() string         { return "KafkaSecurity(redacted)" }
func (s KafkaSecurity) LogValue() slog.Value { return slog.StringValue(s.String()) }

func (s KafkaSecurity) validate() error {
	switch s.Environment {
	case "", "local", "development", "test", "staging", "production":
	default:
		return fmt.Errorf("invalid Kafka environment")
	}
	if s.Environment == "production" && !s.TLS {
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
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
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

func kafkaSecurityOptions(settings ...KafkaSecurity) ([]kgo.Opt, error) {
	if len(settings) > 1 {
		return nil, fmt.Errorf("only one Kafka security configuration allowed")
	}
	var s KafkaSecurity
	if len(settings) == 1 {
		s = settings[0]
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
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism()))
	}
	return opts, nil
}

func loadKafkaSecurity(environment string) (KafkaSecurity, error) {
	s := KafkaSecurity{Environment: environment, CAFile: Env("KAFKA_TLS_CA_FILE", ""), CertFile: Env("KAFKA_TLS_CERT_FILE", ""), KeyFile: Env("KAFKA_TLS_KEY_FILE", ""), ServerName: Env("KAFKA_TLS_SERVER_NAME", ""), SASLMechanism: strings.ToUpper(Env("KAFKA_SASL_MECHANISM", "")), Username: os.Getenv("KAFKA_SASL_USERNAME"), Password: os.Getenv("KAFKA_SASL_PASSWORD")}
	if value := strings.TrimSpace(os.Getenv("KAFKA_TLS_ENABLED")); value != "" {
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return s, fmt.Errorf("invalid KAFKA_TLS_ENABLED")
		}
		s.TLS = enabled
	}
	return s, s.validate()
}
