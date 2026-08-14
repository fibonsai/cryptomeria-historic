# ADR-010: Decouple domain-specific types and topic classification from `cryptomeria-nng-client`

| Field        | Value |
|--------------|-------|
| **Category** | Core Architecture |
| **Status**   | Proposed |
| **Implemented** | pending PR |
| **Created**  | 2026-08-14 23:50 |
| **Relates**  | ADR-009, Issue #23 |

## Context

ADR-009 chose Option 3: extract `cryptomeria-nng-client` as a library crate that
owns both the generic NNG transport layer **and** the domain-specific market-data
types (`MarketDataItem`, `LobItem`, `TradeItem`) and topic-classification helpers
(`classify_topic`, topic prefixes).

The goal was a "single coherent API" where the wire format, topic classification,
and payload types live where they are produced. However, this tightly couples the
NNG client crate to the `cryptomeria-marketdata` wire contract — specifically the
`{kind}__{instrument}` topic convention (`lob__`/`trade__`) and the JSON payload
schema of `MarketDataItem`.

### Forces

- **Reusability:** Other applications (e.g. a future low-latency relay, a
  research tool, a different historical processor) may want the same per-broker
  NNG SUB socket lifecycle and connectivity-attribution behaviour without
  inheriting the cryptomeria-specific market-data schema.
- **Topic format variance:** Not every NNG PUB/SUB deployment uses the
  `lob__`/`trade__` topic convention. A generic client should not impose a topic
  format on its consumers.
- **Payload variance:** The `parse_frame` function deserialises JSON into
  `MarketDataItem`, making the frame parser specific to one payload schema. Other
  consumers may want raw bytes or a different deserialisation target.
- **ADR-009's concession:** ADR-009 itself noted this as a downside: "Forces a
  `MarketDataItem` trait or generic, adding complexity to the most common path."

## Options Considered

### Option 1: Keep items and topic logic in `cryptomeria-nng-client` (status quo per ADR-009)

- **Pros:** Single coherent API; `parse_frame` still yields `MarketDataItem` directly — no generics or trait objects needed for the common path.
- **Cons:** Locks the client crate to the cryptomeria-marketdata wire contract. Other apps must either depend on `cryptomeria-historic` (and inherit QuestDB) or accept unwanted payload types. The topic-format coupling means `classify_topic` and the prefix constants can never serve a non-cryptomeria consumer.

### Option 2: Split by ownership — generic transport in client, domain types in historic (chosen)

Move `MarketDataItem`, `LobItem`, `LobLevel`, `TradeItem`, `classify_topic`,
`extract_topic_segment`, `parse_frame`, and topic-prefix constants into
`cryptomeria-historic`. The client crate keeps only:

- NNG lifecycle: `NngSubscriber`, `BrokerReader`, `BrokerOutput`, `ConnEvent`,
  `connectivity_event`
- Generic frame helpers: `split_frame`, `frame_message`, `FRAME_SEPARATOR`
- Generic transport: raw `nng::Message` delivery via `BrokerOutput::Message`

The consumer (`cryptomeria-historic`) calls `split_frame` to extract the topic
string and raw payload bytes from the `BrokerOutput::Message`, then applies its
own topic classification and JSON deserialisation.

- **Pros:**
  - `cryptomeria-nng-client` is reusable by any NNG PUB/SUB consumer, regardless of topic format or payload schema.
  - `MarketDataItem` and the `lob__`/`trade__` convention stay where they belong: in the crate that expects them.
  - No serde/serde_json dependency in the client crate.
  - The generic frame helpers (`split_frame`, `frame_message`) already operate on raw bytes — the split is clean.
- **Cons:**
  - `cryptomeria-historic` must do one extra `serde_json::from_slice` call per message (trivial; was implicit in `parse_frame` before).
  - The `parse_frame` convenience is lost from the client crate; callers must compose `split_frame` + their own deserialiser. This is acceptable given the gain in generality.

### Option 3: Trait-based generic payload (rejected)

Introduce a generic `parse_frame<T: DeserializeOwned>` in the client crate with a
`serde` bound.

- **Pros:** Still generic; type-safe.
- **Cons:** Forces a `serde` + `serde_json` dependency on the client crate for all consumers, even those that only want raw bytes. Adds compile-time complexity for a marginal ergonomic gain. The `split_frame` + caller-deserialises approach is simpler and dependency-free.

## Decision

Adopt **Option 2**: decouple domain-specific types and topic classification from
`cryptomeria-nng-client`. The client crate becomes a thin, generic NNG
PUB/SUB transport layer; `cryptomeria-historic` owns the wire-contract
interpretation.

### API surface of `cryptomeria-nng-client` (after)

Public API retains only generic, transport-level constructs:

- `BrokerReader` — one `Sub0` socket per broker, with per-broker pipe-count tracking
- `NngSubscriber` — multi-broker fan-out
- `BrokerOutput` — `Message(nng::Message)`, `Down { addr }`, `Up { addr }`
- `ConnEvent`, `connectivity_event` — debounce/state-transition logic
- `FRAME_SEPARATOR`, `split_frame`, `frame_message` — raw frame helpers

### API surface of `cryptomeria-historic` (after)

`cryptomeria-historic` owns:

- `items` module: `MarketDataItem`, `LobItem`, `LobLevel`, `TradeItem`
- `topics` module: `classify_topic`, `extract_topic_segment`, `LOB_TOPIC_PREFIX`, `TRADE_TOPIC_PREFIX`
- `forward` module: `parse_frame` (combines `split_frame` from nng-client + local `MarketDataItem` deserialisation)

`src/lib.rs` re-exports the generic nng-client types alongside the local domain
types, so existing callers of `cryptomeria_historic::*` keep working.

## Consequences

- **Positive:** `cryptomeria-nng-client` can be reused by apps with different topic formats and payload schemas. The crate’s `Cargo.toml` no longer needs `serde`/`serde_json`.
- **Positive:** The boundary between transport (raw NNG messages) and domain (cryptomeria-marketdata contracts) is now explicit and documented.
- **Negative:** `parse_frame` moves to the consumer crate; the nng-client no longer offers a one-step parse-to-domain-item convenience function. This is the cost of generality and is acceptable.
- **Negative:** Reverses the decision in ADR-009. ADR-009 is superseded by this ADR for the `items` and `classify_topic` placement; ADR-009 remains valid for the per-broker connectivity model (ADR-007/008) and the flat workspace layout.

## References

- ADR-009: Extract NNG subscriber client into a standalone `cryptomeria-nng-client` crate
- ADR-001: cryptomeria-historic provides QuestDB persistence
- Issue #20: Log per-broker disconnects; timeout-only detection misses secondary brokers
- Issue #23: This task
