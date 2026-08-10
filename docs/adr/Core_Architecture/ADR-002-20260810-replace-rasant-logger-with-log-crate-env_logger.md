# ADR-002: Replace rasant logger with log crate + env_logger for Send safety

- **Category**: Core Architecture
- **Status**: Accepted
- **Implemented**: N/A
- **Created**: 2026-08-10 15:00

## Context

The `cryptomeria-historic` crate uses `rasant` as a custom process-wide logger,
exposed through `logging.rs` category helpers (`info`, `warn`, `error`, `debug`).
Each helper takes a `category` string (e.g. `"forwarder"`, `"system"`, `"migrate"`,
`"ttl"`) and a message.

### Forces

* The `rasant::Logger` type is **not `Send`**, which complicates use across thread
  boundaries. The crate spawns a dedicated thread for the NNG receive loop, and the
  non-`Send` logger cannot be safely moved into or referenced from that thread.
* A logging approach that is `Send`-compatible by design is required to avoid
  future concurrency issues as the crate evolves.
* The `log` crate is the de facto standard facade for Rust logging; pairing it with
  `env_logger` provides a mature, `Send`-compatible backend with zero additional
  runtime complexity.

## Options Considered

### Option A: Keep rasant, wrap in `Arc<Mutex<>>`

Protect the `rasant::Logger` behind an `Arc<Mutex<>>` and pass clones into the
worker thread.

**Pros:**
- Minimal call-site churn.

**Cons:**
- Still requires manual synchronization; the `Logger` itself remains non-`Send`.
- Does not address the underlying design limitation.

### Option B: Adopt `log` crate facade + `env_logger` backend (accepted)

Replace `rasant` with the standard `log` / `env_logger` pair. The `log` macros
are `Send`-safe by design since they delegate to a globally registered backend.

**Pros:**
- `Send`-compatible by design — no manual synchronization needed.
- Industry-standard logging stack; `RUST_LOG` env var works out of the box.
- `env_logger::init()` is a one-liner; no global `Mutex` needed.
- Category is embedded in the format string (e.g. `log::info!("[forwarder] ...")`),
  preserving the existing categorisation.

**Cons:**
- Call sites change from `logging::info("cat", msg)` to
  `log::info!("[cat] msg")` — broader diff but mechanical.
- `env_logger` initializes its global backend at most once per process; calling
  `init()` twice panics. This matches the current single-init pattern.

### Option C: Adopt `tracing` + `tracing-subscriber`

Use the `tracing` crate as the facade and `tracing-subscriber` as the backend.

**Pros:**
- Structured, span-oriented logging with very low overhead.
- First-class async support.

**Cons:**
- Over-engineered for a simple PUB/SUB forwarder that does not need distributed
  tracing.
- Larger dependency tree and steeper learning curve for contributors.
- `log` / `env_logger` is sufficient and matches the existing crate style.

## Decision

Adopt **Option B** — replace `rasant` with `log = "0.4"` + `env_logger = "0.11"`.

* `src/logging.rs` is rewritten as a thin `init()` function delegating to
  `env_logger::Builder::from_env(...).default_filter_or("info")`.
* All call sites in `main.rs`, `migrate.rs`, and `db/mod.rs` are migrated from
  `logging::{info,warn,error,debug}("category", msg)` to
  `log::{info,warn,error,debug}!("[category] msg")`.
* `AGENTS.md` is updated to reflect the new convention.

## Consequences

### Positive

- **Send safety** — the `log` facade and `env_logger` backend are `Send` by
  design; no `Mutex<Logger>` workaround needed.
- **Standard tooling** — `RUST_LOG` controls verbosity without code changes.
- **Reduced custom code** — `logging.rs` shrinks from 74 lines to ~12 lines.

### Negative

- **Call-site churn** — every logging call site is touched, making the diff
  larger than a minimal drop-in replacement would have been.
- **Single initialization** — `env_logger::init()` panics if called a second
  time; this is acceptable since `init()` is called exactly once in `main()`.

## References

- [log crate](https://crates.io/crates/log)
- [env_logger crate](https://crates.io/crates/env_logger)
- ADR-001: Cryptomeria-historic provides QuestDB persistence for normalised market data
