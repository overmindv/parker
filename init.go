package parker

import (
	"io"

	"github.com/overmindv/parker/internal/servicegen"
)

// ValidateServiceName проверяет имя сервиса.
func ValidateServiceName(name string) error {
	return servicegen.ValidateServiceName(name)
}

// GenerateService создаёт каркас сервиса name в каталоге dir из встроенных шаблонов.
func GenerateService(dir, name string, hasPG, hasKafka bool) error {
	return servicegen.GenerateService(dir, name, hasPG, hasKafka)
}

// PrintComposeBlock печатает готовый блок docker-compose для вставки в infra.
func PrintComposeBlock(w io.Writer, name string, hasPG, hasKafka bool) {
	servicegen.PrintComposeBlock(w, name, hasPG, hasKafka)
}

// PrintPostInitChecklist печатает чек-лист (Части 1–5) после создания сервиса.
func PrintPostInitChecklist(w io.Writer, name string) {
	servicegen.PrintPostInitChecklist(w, name)
}
