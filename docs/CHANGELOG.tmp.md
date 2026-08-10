# Changelog

Date: 2026-08-10
Task: Replace custom rasant-based logger with log crate + env_logger

## Summary

The crate used a custom `rasant`-based logger (`logging.rs`) that was **not `Send`**,
making cross-thread usage fragile. Replaced it with the standard `log` crate facade
backed by `env_logger`, which is `Send`-compatible by design. All 19 call sites in
`main.rs`, `migrate.rs`, and `db/mod.rs` were migrated from
`logging::{info,warn,error,debug}(category, msg)` to
`log::{info,warn,error,debug}!("[category] msg")`, preserving the existing
categorisation. `logging.rs` was slimmed from 74 lines to a 12-line `init()` wrapper.

## Files modified

- `Cargo.toml` — replaced `rasant = 1.1` with `log = 0.4` and `env_logger = 0.11`
- `src/logging.rs` — rewritten as a thin `env_logger::init()` wrapper
- `src/main.rs` — migrated all `logging::*` calls to `log::*!` macros with category prefix
- `src/migrate.rs` — migrated `logging::info`/`error` calls to `log::info!`/`log::error!`; removed unused import
- `src/db/mod.rs` — migrated `logging::warn` calls to `log::warn!`; removed unused import
- `AGENTS.md` — updated module table, workspace layout, and conventions to reflect `log`/`env_logger`
- `docs/adr/Core_Architecture/ADR-002-20260810-replace-rasant-logger-with-log-crate-env_logger.md` — new ADR documenting the decision

## Test results

23 passed, 0 failed — `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`, and `make all` all green.
