## ─── Cryptomeria Historic — Makefile ───────────────────────────────

CARGO      := cargo
CARGO_AUDIT := cargo audit
TARPAULIN  := cargo tarpaulin

# Colors (disabled when stdout is not a TTY)
COLOR  := $(shell test -t 1 && printf '\033[32m' || printf '')
NC     := $(shell test -t 1 && printf '\033[0m'  || printf '')

# Default: show help
.DEFAULT_GOAL := help

## help        : Show this help message
.PHONY: help
help:
	@echo "$(COLOR)Cryptomeria Historic — available targets:$(NC)"
	@echo ""
	@grep -E '^## [a-z].* : ' $(MAKEFILE_LIST) | sed 's/^## //' | sed 's/ : /:/' | awk -F':' '{printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

## clean       : Remove build artifacts
.PHONY: clean
clean:
	$(CARGO) clean

## check        : Run fast type-checking (no codegen)
.PHONY: check
check:
	$(CARGO) check --all-targets

## build        : Build the crate in debug mode
.PHONY: build
build:
	$(CARGO) build

## build-release : Build the crate in release mode (optimized + LTO)
.PHONY: build-release
build-release:
	$(CARGO) build --release

## clippy       : Run Clippy lints with warnings as errors
.PHONY: clippy
clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

## fmt          : Format all source files
.PHONY: fmt
fmt:
	$(CARGO) fmt

## fmt-check    : Verify formatting without modifying files
.PHONY: fmt-check
fmt-check:
	$(CARGO) fmt -- --check

## test         : Run all unit tests
.PHONY: test
test:
	$(CARGO) test --lib

## test-integrations : Run integration tests (tests/ directory)
.PHONY: test-integrations
test-integrations:
	$(CARGO) test --test '*'

## audit        : Run cargo-audit to check for vulnerable dependencies
.PHONY: audit
audit:
	$(CARGO_AUDIT)

## coverage     : Run code coverage with cargo-tarpaulin (outputs Cobertura XML)
.PHONY: coverage
coverage:
	$(TARPAULIN) \
		--workspace \
		--out Xml \
		--output-dir coverage \
		-- --test-threads=4

## doc          : Build rustdoc documentation
.PHONY: doc
doc:
	$(CARGO) doc --no-deps --open

## all          : check, clippy, fmt, test (full CI-style run)
.PHONY: all
all: check clippy fmt-check test
