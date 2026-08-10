# cryptomeria-historic

NNG PUB/SUB subscriber that forwards normalised LOB/trade market data to QuestDB with
embedded schema migrations.

## Overview

`cryptomeria-historic` connects to an NNG PUB broker (run by
[`cryptomeria-marketdata`](https://github.com/fibonsai/cryptomeria-marketdata)),
receives framed `{topic}\0{json-payload}` messages, deserialises them into
normalised `MarketDataItem` values, and writes LOB levels and trades into QuestDB
via the ILP line protocol.

### Architecture

```
cryptomeria-marketdata ──NNG PUB──► cryptomeria-historic ──ILP──► QuestDB
```

* **NNG subscriber thread** — blocking receive loop on a dedicated thread; filters
  by topic prefix (`lob__`, `trade__`) and parses wire frames.
* **QuestDB writer** — persists rows via `questdb-rs` ILP sender.
* **Migration runner** — executes embedded SQL migrations on startup via the
  QuestDB HTTP REST API, tracking applied versions in a `schema_version` table.

### Wire format

Each NNG message is `{topic}\0{payload}` where:

| Field      | Description                                            |
|------------|--------------------------------------------------------|
| `topic`    | UTF-8 string `{kind}__{instrument}` (e.g. `lob__btcusdt`) |
| `payload`  | JSON serialisation of a `MarketDataItem`               |
| separator  | NUL byte (`\0`)                                       |

### Supported topics

| Topic prefix  | Variant   | QuestDB table |
|---------------|-----------|---------------|
| `lob__*`      | `LobItem`  | `lob_levels`  |
| `trade__*`    | `TradeItem`| `trades`     |

## Quick start

### Prerequisites

* Rust 2024 edition toolchain
* QuestDB running and reachable (default: `http://localhost:9000`)
* NNG PUB broker (from `cryptomeria-marketdata`) running (default: `tcp://127.0.0.1:14242`)

### Build

```sh
cargo build
```

### Run

```sh
# Defaults: subscribe to tcp://127.0.0.1:14242, write to localhost:9000
cargo run

# Dry-run mode (receive and log, do not persist):
cargo run -- --dry-run

# Custom NNG and QuestDB addresses:
cargo run -- \
  --nng-addr tcp://127.0.0.1:14242 \
  --qdb-conf "http::addr=questdb:9000;username=admin;password=quest;"

# Auto-exit after 30 seconds (useful for CI):
cargo run -- --test-timeout-secs 30

# Apply a custom TTL on the destination tables:
cargo run -- --ttl-hours 24
```

### CLI options

| Flag                | Default                  | Description                                      |
|---------------------|--------------------------|--------------------------------------------------|
| `--nng-addr`        | `tcp://127.0.0.1:14242`  | NNG PUB broker address to subscribe to           |
| `--qdb-conf`        | env `QDB_CLIENT_CONF`    | QuestDB connection-conf string                   |
| `--ttl-hours`       | _none_                   | Override QuestDB table TTL (applied on startup)  |
| `--dry-run`         | _off_                    | Receive and log only; do not persist to QuestDB  |
| `--test-timeout-secs` | `0`                    | Exit after N seconds (`0` = run indefinitely)     |

## Development

### Make targets

| Target              | Description                                  |
|---------------------|----------------------------------------------|
| `make check`        | `cargo check --all-targets`                  |
| `make build`        | `cargo build`                                |
| `make build-release`| `cargo build --release`                      |
| `make clippy`       | `cargo clippy -- -D warnings`                |
| `make fmt`          | `cargo fmt`                                  |
| `make fmt-check`    | `cargo fmt -- --check`                       |
| `make test`         | `cargo test --lib`                           |
| `make test-integrations` | `cargo test --test '*'`                |
| `make all`          | check + clippy + fmt-check + test            |
| `make doc`          | `cargo doc --no-deps --open`                 |
| `make audit`        | `cargo audit`                                |
| `make coverage`     | `cargo-tarpaulin` (Cobertura XML output)     |
| `make clean`        | `cargo clean`                                |

### Coding standards

* Edition 2024, `cargo clippy -D warnings` clean.
* No comments in code — intent expressed via names; decisions in ADRs.
* Relative paths only.
* Secrets in `.env.local` only (never committed).
* See [CONTRIBUTING.md](CONTRIBUTING.md) for full guidelines.

## Schema

### `trades` (V1)

| Column       | Type   | Indexes           |
|--------------|--------|-------------------|
| `inst_id`    | SYMBOL | INDEX POSTING     |
| `exchange`   | SYMBOL | INDEX POSTING     |
| `trade_id`   | SYMBOL |                   |
| `seq_id`     | LONG   |                   |
| `px`         | DOUBLE |                   |
| `sz`         | DOUBLE |                   |
| `side`       | SYMBOL | INDEX POSTING     |
| `ts`         | TIMESTAMP | TIMESTAMP(ts), HOUR partition, 1h TTL |

### `lob_levels` (V2)

| Column       | Type   | Indexes           |
|--------------|--------|-------------------|
| `inst_id`    | SYMBOL | INDEX POSTING     |
| `exchange`   | SYMBOL | INDEX POSTING     |
| `ts`         | TIMESTAMP |               |
| `side`       | SYMBOL | INDEX POSTING     |
| `price`      | DOUBLE |                   |
| `size`       | DOUBLE |                   |
| `best_diff`  | DOUBLE |                   |

## Environment variables

| Variable          | Description                                      |
|-------------------|--------------------------------------------------|
| `QDB_CLIENT_CONF` | QuestDB connection-conf string (fallback for `--qdb-conf`) |
| `RUST_LOG`        | Log level (`info`, `debug`, `warn`, `error`; default: `info`) |

## License

Apache License 2.0 — see [LICENSE](LICENSE).
