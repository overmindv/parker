package parker

import (
	"bytes"
	"embed"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
)

//go:embed template/service/*.tmpl
var serviceTemplates embed.FS

// serviceData — параметры подстановки в шаблоны каркаса сервиса.
type serviceData struct {
	ServiceName string
	GoModule    string
	HasPG       bool
	HasKafka    bool
}

var serviceNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)

// ValidateServiceName проверяет имя сервиса.
func ValidateServiceName(name string) error {
	if !serviceNameRe.MatchString(name) {
		return fmt.Errorf("недопустимое имя сервиса %q: строчные латинские буквы, цифры и '-', 1–40 символов, начинается с буквы или цифры", name)
	}
	return nil
}

// GenerateService создаёт каркас сервиса name в каталоге dir из встроенных шаблонов.
func GenerateService(dir, name string, hasPG, hasKafka bool) error {
	if err := ValidateServiceName(name); err != nil {
		return err
	}
	data := serviceData{
		ServiceName: name,
		GoModule:    "github.com/overmindv/" + name,
		HasPG:       hasPG,
		HasKafka:    hasKafka,
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	files := []struct {
		tmpl, out string
		cond      bool
	}{
		{"template/service/go.mod.tmpl", "go.mod", true},
		{"template/service/main.go.tmpl", filepath.Join("cmd", name, "main.go"), true},
		{"template/service/Makefile.tmpl", "Makefile", true},
		{"template/service/Dockerfile.tmpl", "Dockerfile", true},
		{"template/service/env.example.tmpl", ".env.example", true},
		{"template/service/gitignore.tmpl", ".gitignore", true},
		{"template/service/ci.yml.tmpl", filepath.Join(".github", "workflows", "ci.yml"), true},
		{"template/service/README.md.tmpl", "README.md", true},
		{"template/service/migrations_0001_init.sql.tmpl", filepath.Join("migrations", "0001_init.sql"), hasPG},
	}
	for _, f := range files {
		if !f.cond {
			continue
		}
		if err := renderServiceFile(data, f.tmpl, filepath.Join(dir, f.out)); err != nil {
			return fmt.Errorf("сгенерировать %s: %w", f.out, err)
		}
	}
	if err := generateInternalSkeleton(dir, name); err != nil {
		return err
	}
	return nil
}

func renderServiceFile(data serviceData, tmplPath, outPath string) error {
	raw, err := serviceTemplates.ReadFile(tmplPath)
	if err != nil {
		return err
	}
	tmpl, err := template.New(filepath.Base(tmplPath)).Parse(string(raw))
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(outPath, buf.Bytes(), 0o644)
}

// generateInternalSkeleton создаёт структуру каталогов бизнес-логики с подсказками.
func generateInternalSkeleton(dir, name string) error {
	base := filepath.Join(dir, "internal", name)
	places := []struct {
		rel, hint string
	}{
		{"domain", "Бизнес-типы и инварианты."},
		{"usecase", "Сценарии/оркестрация. Boundary транзакций — здесь."},
		{"repository", "Persistence-интерфейс."},
		{"transport", "HTTP-обработчики; регистрируются на app.HTTP()."},
	}
	for _, p := range places {
		pdir := filepath.Join(base, p.rel)
		if err := os.MkdirAll(pdir, 0o755); err != nil {
			return err
		}
		readme := "# " + name + "/" + p.rel + "\n\n" + p.hint + "\n"
		if err := os.WriteFile(filepath.Join(pdir, "README.md"), []byte(readme), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// PrintComposeBlock печатает готовый блок docker-compose для вставки в infra.
func PrintComposeBlock(w io.Writer, name string, hasPG, hasKafka bool) {
	pf := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) } // игнорируем ошибку записи
	pf("# Вставьте в infra/docker-compose.yml и добавьте %s_POSTGRES_PASSWORD=__GENERATE__ в infra/.env.example\n", strings.ToUpper(name))
	pf("  %s-postgres:\n", name)
	pf("    image: postgres:17-alpine\n")
	pf("    environment: { POSTGRES_DB: %s, POSTGRES_USER: %s, POSTGRES_PASSWORD: \"${%s_POSTGRES_PASSWORD:?set in .env}\" }\n", name, name, strings.ToUpper(name))
	pf("    volumes: [ %s-postgres-data:/var/lib/postgresql/data ]\n", name)
	pf("    healthcheck: { test: [\"CMD-SHELL\",\"pg_isready -U %s -d %s\"], interval: 5s, timeout: 3s, retries: 20 }\n\n", name, name)

	pf("  %s-migrate:\n", name)
	pf("    build: { context: ../%s, args: { GOPROXY: \"${GOPROXY:-https://proxy.golang.org,direct}\" } }\n", name)
	pf("    entrypoint: [\"%s\", \"migrate\", \"--dir\", \"migrations\", \"up\"]\n", name)
	pf("    environment:\n      DATABASE_URL: \"postgres://%s:${%s_POSTGRES_PASSWORD}@%s-postgres:5432/%s?sslmode=disable\"\n\n",
		name, strings.ToUpper(name), name, name)

	pf("  %s:\n", name)
	pf("    build: { context: ../%s, args: { GOPROXY: \"${GOPROXY:-https://proxy.golang.org,direct}\" } }\n", name)
	pf("    environment:\n      SERVICE_NAME: %s\n      HTTP_ADDR: \":8080\"\n      ENV: local\n", name)
	pf("      DATABASE_URL: \"postgres://%s:${%s_POSTGRES_PASSWORD}@%s-postgres:5432/%s?sslmode=disable\"\n",
		name, strings.ToUpper(name), name, name)
	if hasKafka {
		pf("      KAFKA_BOOTSTRAP_SERVERS: \"kafka:9092\"\n")
	}
	pf("    depends_on:\n      %s-migrate: { condition: service_completed_successfully }\n", name)
	pf("    healthcheck:\n      test: [\"CMD-SHELL\",\"wget -qO- http://127.0.0.1:8080/ready >/dev/null\"]\n      interval: 5s; timeout: 3s; retries: 20; start_period: 5s\n    restart: unless-stopped\n")
}

// PrintPostInitChecklist печатает чек-лист (Части 1–5) после создания сервиса.
func PrintPostInitChecklist(w io.Writer, name string) {
	pf := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) } // игнорируем ошибку записи
	pf("\nСервис %q создан. Дальше (workflow Частей 1–5, см. parker/docs/plan/08_service_bootstrap.md):\n", name)
	pf("  Часть 2: вставьте compose-блок выше в infra/docker-compose.yml, добавьте переменные в .env.\n")
	pf("  Часть 2: go mod tidy && make dev (нужна PostgreSQL).\n")
	pf("  Часть 3: запишите, проверьте пайплайны GitHub Actions (lint/test/build/component).\n")
	pf("  Часть 4: проверьте /health /ready /metrics, логи и trace.\n")
	pf("  Часть 5: реализуйте бизнес-логику в internal/%s/.\n", name)
}
