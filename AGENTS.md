# AGENTS.md — Cryptomeria Historic

This file describes conventions and workflows for AI agents working on
`cryptomeria-historic`. It supplements [CONTRIBUTING.md](CONTRIBUTING.md).

## Project snapshot

* **Crate:** `cryptomeria-historic` (library + binary `cryptomeria-historic`)
* **Language:** Rust 2024 edition
* **Purpose:** NNG PUB/SUB subscriber that forwards normalised LOB/trade market
  data to QuestDB with embedded schema migrations.

## Workspace layout

```
.
├── Cargo.toml              # crate manifest (lib + bin)
├── Makefile                # dev / CI helper targets
├── AGENTS.md               # this file
├── README.md               # user-facing documentation
├── CONTRIBUTING.md         # human contributor guidelines
├── src/
│   ├── lib.rs              # re-exports; crate-level doc
│   ├── main.rs             # CLI binary entry point
│   ├── forward.rs          # NNG wire-frame parsing (topic ␀ JSON)
│   ├── items.rs            # MarketDataItem / LobItem / TradeItem types
│   ├── subscriber.rs       # NNG SUB socket wrapper
│   ├── db/mod.rs           # QuestDB connection + persistence helpers
│   ├── db/migrations/      # embedded SQL migration files
│   ├── migrate.rs          # schema-versioned migration runner (HTTP REST)
│   └── logging.rs          # rasant-based process-wide logger
└── tests/                  # integration tests (currently empty)
```

## Module boundaries

| Module        | Responsibility                              |
|---------------|---------------------------------------------|
| `forward`     | Wire format: split / parse / frame messages |
| `items`       | Serde types for LOB and trade payloads      |
| `subscriber`  | NNG SUB socket lifecycle + topic filtering  |
| `db`          | QuestDB ILP persistence + config resolution |
| `migrate`     | Migration tracking via `schema_version`     |
| `logging`     | Global rasant logger + category helpers     |

## Conventions

### Tooling

* **Build:** `cargo build`
* **Check:** `cargo check --all-targets`
* **Lint:** `cargo clippy --all-targets --all-features -- -D warnings`
* **Format:** `cargo fmt` (verify with `cargo fmt -- --check`)
* **Test:** `cargo test --lib` (unit); `cargo test --test '*'` (integration)
* **Full CI gate:** `make all`

### Code style

* Edition 2024 idioms: `let-else`, `if let` chains, `matches!` macro.
* No comments in source code — express intent via names; document decisions in
  ADRs or commit messages.
* Use `anyhow::Result` at the application layer (CLI / persistence boundary).
* Internal modules may return `Result<T, String>` for migration HTTP errors.
* Prefer `clap::Parser` derive for CLI arguments (see `main.rs` `Cli` struct).
* Use `rasant` for logging, not `log`/`env_logger`. All logging goes through
  `logging.rs` category helpers (`info`, `warn`, `error`, `debug`).

### Naming

* Crate / lib: `cryptomeria_historic` (snake_case lib name)
* Binary: `cryptomeria-historic` (kebab-case bin name)
* Topic prefixes: `lob__` / `trade__` (double underscore)

### Paths

* Always relative — no absolute paths in code, config, or docs.

## Common tasks

### Add a new migration

1. Create `src/db/migrations/V{n}__{name}.sql`.
2. Register in `MIGRATIONS` array inside `src/db/mod.rs`.
3. Add a unit test case if the migration affects persistence logic.
4. Re-run `make all`.

### Add a new CLI flag

1. Add the field to the `Cli` struct in `src/main.rs` with a `#[arg(...)]`
   attribute and doc comment.
2. Consume it in `main()` before the receive loop.
3. Add or update a unit test in the `tests` module of `main.rs`.

### Add a unit test

* Place `#[cfg(test)] mod tests` at the bottom of the relevant module file.
* Use `serial_test` for tests that touch process-wide state.
* Keep tests hermetic — no network, no QuestDB, no NNG broker.
