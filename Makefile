LOCAL_BIN := $(CURDIR)/bin
GOLANGCI_LINT := $(LOCAL_BIN)/golangci-lint

# Автоматически подхватываем локальный .env, если есть.
ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: build test lint tidy tools bump-version

# build — проверяет, что весь паркер (включая CLI) компилируется.
build:
	go build ./...

# test — юнит-тесты с детектором гонок (без внешних зависимостей).
test:
	go test -race ./...

# lint — статический анализ (gofmt, goimports, staticcheck и др.).
lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run

# tidy — приводит go.mod/go.sum в соответствие с импортами.
tidy:
	go mod tidy

# tools — ставит вспомогательные инструменты в bin/ (один раз, gitignored).
$(GOLANGCI_LINT):
	GOBIN="$(LOCAL_BIN)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
