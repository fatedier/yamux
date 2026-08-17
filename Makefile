GO ?= go
GOFMT ?= gofmt
GOLANGCI_LINT ?= golangci-lint
GOLANGCI_LINT_EXPECTED_VERSION ?= 2.12.2

PACKAGES ?= ./...
GO_TEST_FLAGS ?=
GO_RACE_TEST_FLAGS ?=
GO_VET_FLAGS ?=
GO_BUILD_FLAGS ?=
GOLANGCI_LINT_FLAGS ?=

.DEFAULT_GOAL := all

.PHONY: all check ci fmt fmt-check test race vet build lint require-go require-gofmt require-golangci-lint

all: check

check: ci lint

ci: fmt-check test race vet build

require-go:
	@command -v "$(firstword $(GO))" >/dev/null 2>&1 || { \
		printf '%s\n' "error: required command '$(firstword $(GO))' not found; set GO to an available executable" >&2; \
		exit 127; \
	}

require-gofmt:
	@command -v "$(firstword $(GOFMT))" >/dev/null 2>&1 || { \
		printf '%s\n' "error: required command '$(firstword $(GOFMT))' not found; set GOFMT to an available executable" >&2; \
		exit 127; \
	}

require-golangci-lint:
	@command -v "$(firstword $(GOLANGCI_LINT))" >/dev/null 2>&1 || { \
		printf '%s\n' "error: required command '$(firstword $(GOLANGCI_LINT))' not found; set GOLANGCI_LINT to an available executable" >&2; \
		exit 127; \
	}
	@version_output="$$( $(GOLANGCI_LINT) version 2>&1 )"; \
	status=$$?; \
	if [ "$$status" -ne 0 ]; then \
		printf '%s\n' "error: failed to inspect golangci-lint version" >&2; \
		printf '%s\n' "$$version_output" >&2; \
		exit "$$status"; \
	fi; \
	lint_version="$$(printf '%s\n' "$$version_output" | sed -n 's/.* has version \([^[:space:]]*\).*/\1/p')"; \
	lint_version="$${lint_version#v}"; \
	if [ -z "$$lint_version" ]; then \
		printf '%s\n' "error: unable to determine golangci-lint version from: $$version_output" >&2; \
		exit 1; \
	fi; \
	if [ "$$lint_version" != "$(GOLANGCI_LINT_EXPECTED_VERSION)" ]; then \
		printf '%s\n' "error: golangci-lint version '$$lint_version' found; expected exactly '$(GOLANGCI_LINT_EXPECTED_VERSION)'" >&2; \
		exit 1; \
	fi

fmt: require-gofmt
	$(GOFMT) -w .

fmt-check: require-gofmt
	@diff="$$( $(GOFMT) -d . )" || exit $$?; \
	if [ -n "$$diff" ]; then \
		printf '%s\n' "$$diff"; \
		printf '%s\n' "error: Go files are not formatted; run 'make fmt'" >&2; \
		exit 1; \
	fi

test: require-go
	$(GO) test $(GO_TEST_FLAGS) $(PACKAGES)

race: require-go
	$(GO) test -race -short $(GO_RACE_TEST_FLAGS) $(PACKAGES)

vet: require-go
	$(GO) vet $(GO_VET_FLAGS) $(PACKAGES)

build: require-go
	$(GO) build $(GO_BUILD_FLAGS) $(PACKAGES)

lint: require-golangci-lint
	$(GOLANGCI_LINT) run $(GOLANGCI_LINT_FLAGS)
