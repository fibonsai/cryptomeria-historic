# ADR-001: Cryptomeria-historic provides QuestDB persistence for normalised market data

- **Category**: Core Architecture
- **Status**: Accepted
- **Implemented**: N/A (foundational)
- **Created**: 2026-08-10 15:00

## Context

Cryptomeria is a Medium-Frequency Trading platform composed of several crates
that operate in a pipeline:

```
┌──────────────────────┐   ┌──────────────────┐   ┌──────────────────────┐
│ cryptomeria-marketdata │ → │ cryptomeria-historic │ → │ cryptomeria-backtest   │
│ (NNG PUB publisher)    │   │ (NNG SUB + QuestDB)  │   │ (QuestDB reader)       │
└──────────────────────┘   └──────────────────┘   └──────────────────────┘
```

The intermediate crate — `cryptomeria-historic` — is responsible for the
**historical data tier**: receiving normalised LOB and trade messages from the
market-data collector, persisting them durably into QuestDB, and keeping the
database schema versioned and migratable.

### Forces

* Market-data arrives as NNG PUB/SUB messages with a `{topic}\0{json-payload}`
  wire format. The subscriber must decode, classify, and store these in real
  time.
* QuestDB tables must evolve safely — new columns, new indexes, or new tables
  for additional instrument types should not break existing data.
* The crate must be operable both as a long-running daemon (production) and as
  a short-lived process (CI / backtesting ingest verification).
* Type definitions for the market-data payload must be authoritative in this
  crate so that downstream consumers (backtest, analytics) depend on a single
  source of truth.

## Options Considered

### Option A: Forward raw messages to a generic message queue (Kafka / NATS)

Push raw NNG messages into a durable queue and let consumers handle
persistence independently.

**Pros:**
- Decouples ingestion from storage.
- Allows multiple storage backends (Parquet, Postgres, etc.).

**Cons:**
- Adds operational complexity (queue management, consumer groups).
- QuestDB is already chosen as the canonical store; a queue becomes redundant.
- Does not solve the schema-evolution problem.

### Option B: Persist directly to QuestDB via ILP with embedded migrations

Subscribe to NNG, parse frames, and write rows via the QuestDB ILP line protocol.
Schema migrations are embedded as SQL files and applied on startup.

**Pros:**
- Zero external dependencies at runtime beyond QuestDB.
- Single source of truth for market-data types.
- Schema versioning is explicit and reproducible.
- CLI flags (`--dry-run`, `--test-timeout-secs`) make CI integration trivial.

**Cons:**
- Tied to QuestDB as the sole storage backend.
- Schema changes require a new crate release.

### Option C: Persist via QuestDB HTTP INSERT API

Use QuestDB's REST `/exec` or `/import` endpoints instead of the ILP `Sender`.

**Pros:**
- Works from environments without the ILP Rust crate.

**Cons:**
- ILP `Sender` is more performant (binary protocol, batching).
- The `questdb-rs` crate is already a dependency.

## Decision

Adopt **Option B** — `cryptomeria-historic` is the canonical QuestDB persistence
layer for the Cryptomeria pipeline. It:

1. Subscribes to an NNG PUB broker, receives framed `{topic}\0{payload}` messages,
   and classifies them by topic prefix (`lob__`, `trade__`).
2. Deserialises payloads into `MarketDataItem` (defined in `items.rs`) and
   persists them via the `questdb-rs` ILP `Sender` to the `trades` and
   `lob_levels` tables.
3. Runs embedded SQL migrations (`src/db/migrations/`) on startup via the
   QuestDB HTTP REST API, tracking applied versions in a `schema_version` table.
4. Exposes a CLI binary, `cryptomeria-historic`, with flags for configuration,
    dry-run mode, TTL overrides, and test auto-exit.

## Consequences

### Positive

- **Single source of truth** — `MarketDataItem`, `LobItem`, `TradeItem`, and the
  wire-format parsers live in the crate that owns QuestDB persistence.
- **Self-contained migrations** — SQL files are embedded at compile time and
  applied idempotently on startup.
- **CI-friendly** — `--dry-run` and `--test-timeout-secs` enable automated
  integration testing without a long-running process.
- **Operational simplicity** — no message queue, no separate migration
  tooling; a single `make all` validates code, lints, and runs tests.

### Negative

- **QuestDB lock-in** — switching storage backends requires code changes.
- **Migration coupling** — schema changes are tied to crate releases; rolling
  back a migration requires manual QuestDB intervention.

### Next steps

- Document the wire format in the README so `cryptomeria-marketdata` maintainers
  can stay in sync.
- Plan ADR-002 for the `schema_version` table design and migration file naming
  convention.
