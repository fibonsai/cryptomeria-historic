# ADR-009: Extract NNG subscriber client into a standalone `cryptomeria-nng-client` crate

| Field        | Value |
|--------------|-------|
| **Category** | Core Architecture |
| **Status**   | Accepted |
| **Implemented** | pending PR |
| **Created**  | 2026-08-14 23:46 |
| **Relates**  | ADR-001, ADR-007, ADR-008, Issue #20 |

## Context

The NNG PUB/SUB subscriber logic that lives in `cryptomeria-historic` — namely
`src/subscriber.rs` (per-broker `Sub0` sockets, `BrokerReader`, `NngSubscriber`,
`BrokerOutput`, `connectivity_event`, `classify_topic`) and `src/forward.rs`
(wire-frame parsing: `FRAME_SEPARATOR`, `split_frame`, `parse_frame`,
`frame_message`, `extract_topic_segment`) — is generic transport glue. It does
not depend on QuestDB, market-data schemas beyond the JSON payload, or any
`cryptomeria-historic`-specific concern.

The wire format (`{topic}\0{json}`) and topic prefixes (`lob__`/`trade__`) are
defined by `cryptomeria-marketdata`; the subscriber is the consumer side of that
contract. Several downstream consumers (e.g. a future low-latency relay, other
historical processors) may need the same NNG fan-out + connectivity-attribution
behaviour without inheriting QuestDB, migrations, or the `cryptomeria-historic`
dependency tree.

Keeping the NNG client inline also bloats `cryptomeria-historic`'s public API:
`BrokerOutput`, `NngSubscriber`, and the wire helpers leak into its lib surface
solely so `src/main.rs` can use them. A dedicated crate gives a single,
stable, reusable entry point.

## Options Considered

### Option 1: Keep NNG code inline in `cryptomeria-historic`

- **Pros:** Zero refactor now; no new crate to publish or version.
- **Cons:** Locks the NNG subscriber behind the QuestDB-forwarding crate;
  other consumers must either depend on `cryptomeria-historic` (and inherit
  QuestDB, migrations, `questdb-rs`) or copy the code. Coupling grows worse as
  the crate accumulates more persistence concerns.

### Option 2: Extract into `cryptomeria-nng-client`, keep `items` in `cryptomeria-historic`

The new crate owns NNG sockets + wire framing; `cryptomeria-historic` keeps
`items.rs` (the `MarketDataItem` serde types) and re-parses JSON.

- **Pros:** Clean ownership boundary; minimal API surface in the new crate
  (no serde payload types).
- **Cons:** `forward::parse_frame` currently produces `MarketDataItem` directly,
  so either the crate re-declares a payload type or the parse boundary moves to
  the consumer. Forces a `MarketDataItem` trait or generic, adding complexity
  to the most common path. The wire format and payload are tightly coupled
  (the separator split is meaningless without the payload type), so a hard
  boundary is artificial here.

### Option 3: Extract into `cryptomeria-nng-client` including `items` (chosen)

Move both `subscriber.rs` and `forward.rs` **and** `items.rs` into the new
crate. The new crate publishes `BrokerOutput`, `NngSubscriber`, `BrokerReader`,
`connectivity_event`, `classify_topic`, the wire helpers, and the
`MarketDataItem`/`LobItem`/`TradeItem` serde types as a single coherent
"NNG market-data subscriber" API. `cryptomeria-historic` becomes an application
crate that depends on `cryptomeria-nng-client` for transport + framing and on
QuestDB for persistence.

- **Pros:**
  - Single coherent API: the wire format, topic classification, and payload
    types live where they are produced.
  - `cryptomeria-historic` sheds its NNG and serde payload types; it only owns
    the QuestDB persistence path (`db/`, `migrate.rs`) and the CLI (`main.rs`).
  - Other consumers get the exact same framing + type-parsing + per-broker
    connectivity logic with no QuestDB dependency.
  - `forward::parse_frame` still yields `MarketDataItem` directly — no
    generics or trait objects needed.
- **Cons:**
  - `items.rs` now belongs to the client crate, so any code that previously
    referenced `cryptomeria_historic::items::MarketDataItem` must switch to
    `cryptomeria_nng_client::items::...`. This is contained to `main.rs` and
    the test files, which are both in-tree.
  - A new crate to version; but it lives in the same workspace/repo so
    path dependency makes the transition painless.

## Decision

Adopt **Option 3**: extract `cryptomeria-nng-client` as a library crate at the
repository root (sibling to the `cryptomeria-historic` crate) containing:

- `src/lib.rs` — re-exports.
- `src/subscriber.rs` — `NngSubscriber`, `BrokerReader`, `BrokerOutput`,
  `ConnEvent`, `connectivity_event`, `classify_topic`, topic-prefix constants.
- `src/forward.rs` — `FRAME_SEPARATOR`, `split_frame`, `parse_frame`,
  `frame_message`, `extract_topic_segment`.
- `src/items.rs` — `MarketDataItem`, `LobItem`, `LobLevel`, `TradeItem`.
- `tests/subscriber_broker_test.rs` — the in-process mock PUB0 integration tests
  (using the `nng` crate directly), relocated wholesale.
- `Cargo.toml` — `nng = "1"`, `serde`, `serde_json`, `tokio` (for
  `spawn_blocking`), `log`. No QuestDB dependency.

`cryptomeria-historic` then:
- Removes `src/forward.rs`, `src/subscriber.rs`, `src/items.rs`, and the
  relocated test file.
- Replaces `nng` and the inline NNG/wire/framing code in `Cargo.toml` with a
  path dependency on `cryptomeria-nng-client`.
- Updates `src/main.rs` to import `cryptomeria_nng_client::{forward, items,
  subscriber, BrokerOutput, NngSubscriber}` and to re-export the client crate's
  public types from `src/lib.rs` (so existing `cryptomeria_historic::items`
  callers keep working via a blanket `pub use`).
- Keeps the `tests/` integration test using `nng` directly for mock PUBs,
  importing framing helpers from `cryptomeria_nng_client::forward`.

The workspace is **flat** (two independent `Cargo.toml`s at sibling paths), not
a Cargo workspace, because the two crates have different edition/feature
profiles and the new crate is also intended for standalone publication.

### Concurrency model (unchanged)

`nng::Socket::recv()` is blocking C FFI and stays inside
`tokio::task::spawn_blocking`. The QuestDB writer consumer remains a
`std::thread::spawn` in `cryptomeria-historic` because `questdb::BorrowedSender`
is `!Send`; broker tasks push `BrokerOutput` onto a `std::sync::mpsc` channel
consumed by that thread. The new crate owns the broker side (sockets,
`BrokerReader::run`, the channel sender); the application crate owns the
consumer side (the dedicated thread + QuestDB sink).

### Down/up attribution & debounce (unchanged)

`connectivity_event` and the `DOWN_THRESHOLD_TICKS = 2` (~1 s) debounce travel
intact into the new crate. The regression covered by issue #20 and the
`down_secondary_broker_logged_while_others_flow` test are preserved verbatim.

## Consequences

- `cryptomeria-historic` no longer ships NNG, forward, or items modules; its
  lib surface narrows to `QuestDb`, `db::`, `migrate`, `logging`.
- New crate `cryptomeria-nng-client` is independently testable and reusable;
  its CI can run `cargo test` without any QuestDB container.
- The flat layout (no shared `[workspace]`) keeps each crate's `Cargo.lock`
  independent and allows independent versioning.
- Existing `cryptomeria_historic::items::*` re-exports are kept as a thin
  compatibility shim so external callers don't break immediately.

## References

- ADR-001: cryptomeria-historic provides QuestDB persistence
- ADR-007: Per-broker connectivity tracking via pipe_notify
- ADR-008: Switch to per-broker SUB sockets for reliable disconnect logging
- Issue #20: Log per-broker disconnects; timeout-only detection misses secondary brokers
- NNG pipe event API: <https://nanomsg.github.io/nng/man/v1.2.2/nng_pipe_notify.3.html>
