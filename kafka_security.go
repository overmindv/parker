package parker

import (
	"log/slog"

	"github.com/overmindv/parker/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaSecurity — общие настройки producer/subscriber. Нулевое значение сохраняет local plaintext.
// Production требует TLS; проверка CA и hostname никогда не отключается.
type KafkaSecurity kafka.Security

// String/LogValue исключают credentials из обычного форматирования и slog.
func (KafkaSecurity) GoString() string { return kafka.Security{}.GoString() }

func (s KafkaSecurity) String() string       { return kafka.Security(s).String() }
func (s KafkaSecurity) LogValue() slog.Value { return kafka.Security(s).LogValue() }

func kafkaSecurityOptions(settings ...KafkaSecurity) ([]kgo.Opt, error) {
	return kafka.SecurityOptions(internalSecurity(settings)...)
}
