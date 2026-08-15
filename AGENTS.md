# AGENTS.md — Cryptomeria Historic

Conventions and workflows for AI agents working on `cryptomeria-historic`.
Supplements [CONTRIBUTING.md](CONTRIBUTING.md) and [README.md](README.md).

Every section below captures something an agent would likely get wrong on first
visit without being told.

## Project snapshot

* **Crate:** `cryptomeria-historic` — library (`cryptomeria_historic`) + binary (`cryptomeria-historic`)
* **Language:** Rust 2024 edition
* **Purpose:** NNG PUB/SUB subscriber that forwards normalised LOB/trade market
  data to QuestDB (QWP/WebSocket) with embedded, schema-versioned SQL migrations.

## Dependencies worth knowing

* `cryptomeria-nng-client` — pulled from GitHub (`branch = "main"`), not vendored
  locally.
* `questdb-rs` v7 — **vendored at `vendor/questdb-rs/`** and consumed as a path
  dependency committed to the repo. Do NOT add `questdb` as an external crate;
  extend features in `Cargo.toml` against the vendored copy.
* `questdb-rs` connects over **QWP/WebSocket only** (`ws::addr=` / `wss://`).
  The ILP HTTP transport (`http::addr=`) is not enabled.
* `Cargo.lock` is **git-ignored** — every clean build re-resolves dependencies.

## Workspace layout

```
.
├── Cargo.toml              # crate manifest (lib + bin), edition 2024
├── build.rs                # scans src/db/migrations/ → generates MIGRATIONS const
├── Makefile                # dev / CI helper targets (see below)
├── src/
│   ├── lib.rs              # module declarations + re-exports
│   ├── main.rs             # CLI binary: parse → connect QuestDB → migrate → receive
│   ├── forward.rs          # wire-frame split + MarketDataItem deserialization
│   ├── items.rs            # MarketDataItem / LobItem / TradeItem / LobLevel
│   ├── topics.rs           # classify_topic / extract_topic_segment
│   ├── db/mod.rs           # QuestDB connect, persist, TTL, migration wiring
│   ├── db/migrations/      # V{n}__{name}.sql files (source of truth for schema)
│   ├── migrate.rs          # QuestDbMigrator: schema_version tracking + force-recreate
│   └── logging.rs          # env_logger init (default level: info)
└── tests/
    └── lob_persistence.rs  # integration test (testcontainers/QuestDB, requires Docker)
```

## Module boundaries

| Module        | Responsibility                                      |
|---------------|-----------------------------------------------------|
| `forward`     | Wire format: split `{topic}\0{payload}`, parse JSON to `MarketDataItem` |
| `items`       | Serde types for LOB and trade payloads              |
| `topics`      | Topic classification: `classify_topic`, `extract_topic_segment` |
| `db`          | QuestDB QWP/WebSocket persistence + config resolution |
| `migrate`     | Migration tracking via `schema_version` table       |
| `logging`     | env_logger initializer (`init()`)                  |

## Execution flow (the wiring)

1. `main()` parses CLI args (`clap::Parser` derive) and calls `logging::init()`.
2. QuestDB connection string resolved: CLI `--qdb-conf` > `QDB_CLIENT_CONF` env > hardcoded default (`ws::addr=localhost:9000;username=admin;password=quest;`).
3. **Startup order is fixed and must not be reordered** (`main.rs`):
   connect QuestDB → run migrations → apply TTL → create NNG subscriber → spawn consumer thread → enter receive loop.
4. `NngSubscriber::new(&nng_addrs).run(shutdown)` spawns one `tokio::task::spawn_blocking` per broker (each `Sub0::recv()` is blocking C FFI) and returns a single `mpsc` channel of `BrokerOutput` events.
5. The consumer is a **dedicated OS thread** (`std::thread::spawn`), not a `tokio::spawn` task: `questdb::BorrowedSender` is `!Send` (it carries `PhantomData<Rc<()>>`). This thread holds the sender and drains the channel.
6. On shutdown, `shutdown` is set `true`, per-broker tasks are joined (their `tx` clones drop), and the consumer exits on channel disconnect within ~500 ms.

## Conventions

### Tooling — exact commands

| Goal        | Command                                                    |
|-------------|------------------------------------------------------------|
| Check       | `cargo check --all-targets`                                |
| Lint        | `cargo clippy --all-targets --all-features -- -D warnings` |
| Format      | `cargo fmt` / verify: `cargo fmt -- --check`              |
| Unit tests  | `cargo test --lib`                                         |
| All         | `make all` (= check clippy fmt-check lib-tests)            |

`make all` runs **lib tests only** — it does **not** compile or run integration
tests. Integration tests (`make test-integrations` → `cargo test --test '*'`)
spin up a QuestDB testcontainers image and **require Docker**.

### Code style

* Edition 2024 idioms: `let-else`, `if let` chains, `matches!` macro.
* **No comments in source** — express intent via names; document decisions in
  ADRs or commit messages.
* `anyhow::Result` at the application layer (CLI / persistence boundary).
* Internal modules may return `Result<T, String>` for migration QWP errors.
* `clap::Parser` derive for CLI arguments (see `Cli` struct in `main.rs`).
* Log via `log::{info,warn,error,debug}!` with category embedded in the format
  string, e.g. `log::info!("[forwarder] ...")`. Initialize once via
  `logging::init()` in `main()`.

### Naming

* Crate / lib: `cryptomeria_historic` (snake_case) · Binary: `cryptomeria-historic` (kebab-case)
* Topic prefixes: `lob__` / `trade__` (double underscore, followed by instrument)

### Paths

* Always relative — no absolute paths in code, config, or docs.

## Traps an agent would otherwise miss

* **Adding a migration breaks a test.** `src/db/mod.rs` has
  `migrations_includes_all_files_from_disk` which asserts `MIGRATIONS.len() == 4`.
  Bump that count whenever a new V{n} file is added. (The build script's own
  `collect_migrations_finds_all_files` test is compiled but **not reachable** via
  `cargo test` — build-script tests are not collected by the standard harness.)
* **`--dry-run` skips QuestDB entirely.** No connection is ever attempted, so
  you won't see QuestDB errors in dry-run mode.
* **Crossed books are dropped.** An LOB event where `best_bid_price >
  best_ask_price` logs an error and persists nothing (no rows written).
* **`seq_id` is bounded to `i64::MAX`.** Trade seq_ids exceeding `i64::MAX` are
  mapped to `-1` in `persist_trade` (QuestDB `LONG` column).
* **Timestamps: exchange ms → event nanos.** `event_ts_nanos = (lob.ts as i64) *
  1_000_000`. `snapshot_id` is set equal to `event_ts_nanos`.
* **QuestDB conf string format matters.** `ws::addr=` not `http::addr=`. The
  default includes `username=admin;password=quest;`.
* **`testcontainers` integration tests are `#[serial]`.** Don't run them in
  parallel with each other; they share the process-level Docker daemon.

## Common tasks

### Add a new migration

1. Create `src/db/migrations/V{n}__{name}.sql` with `CREATE TABLE` (or `CREATE
   VIEW`) — `build.rs` auto-detects the target name and `is_view` flag.
2. `cargo build` triggers `build.rs` (it declares
   `cargo:rerun-if-changed=src/db/migrations`); the `MIGRATIONS` const is
   regenerated and `include!`-d into `src/db/mod.rs`. No manual registration.
3. Run `cargo test --lib` to verify the generated `MIGRATIONS` const tests pass.
4. Update `migrations_includes_all_files_from_disk` in `src/db/mod.rs`
   (asserts the expected `MIGRATIONS.len()`).
5. `make all`.

### Add a new CLI flag

1. Add a field to the `Cli` struct in `src/main.rs` with a `#[arg(...)]` attribute.
2. Consume it in `main()` before the receive loop (respect the startup order
   above).
3. Add or update a unit test in the `tests` module of `main.rs`.

### Add a unit test

* Place `#[cfg(test)] mod tests` at the bottom of the relevant module file.
* Use `serial_test` (`#[serial]`) for tests that touch process-wide state.
* Keep tests hermetic — no live network, no QuestDB, no NNG broker. Connectivity
  transitions are unit-tested via `connectivity_event` in the
  `cryptomeria-nng-client` crate; end-to-end behaviour is covered by integration
  tests with testcontainers.

### Add an integration test

* Place in `tests/` (each `.rs` file is a separate test binary).
* Use `testcontainers` (dev-dependency) to spin up QuestDB 10+ (`questdb/questdb:latest`).
* Set `DOCKER_HOST` if the Docker socket is not at the default path.
* For NNG-level behaviour, spin up in-process mock `nng::Protocol::Pub0` sockets
  bound to `tcp://127.0.0.1:0` (localhost ephemeral — no Docker, hermetic) and
  drive `NngSubscriber::run` against them. Mark these `#[serial]`.
* Mark container tests `#[serial]` to avoid resource contention.
