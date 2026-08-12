# ADR-004: Store LOB snapshots with top-of-book fields for fast queries

- **Category**: Persistence & Storage
- **Status**: Accepted
- **Implemented**: [PR #TBD](https://github.com/fibonsai/cryptomeria-historic/pull/PR_NUMBER)
- **Created**: 2026-08-12 12:30

## Context

`cryptomeria-historic` persists LOB (limit-order-book) level data into QuestDB.
Consumers (e.g., `cryptomeria-backtest`) frequently need the top-of-book — the
best bid and best ask prices/sizes — which currently requires scanning and
aggregating all rows in `lob_levels` for a given instrument and timestamp.

Additionally, the wire payload from `cryptomeria-marketdata` does not guarantee
that bid/ask arrays arrive pre-sorted by price. The existing `persist_lob`
function used `lob.bids.first()` / `lob.asks.first()` as best prices without
sorting first, which could yield incorrect best-price values when arrays were
unordered.

Finally, the existing `lob_levels` table has no column linking individual price
levels to a specific LOB snapshot (event), making it impossible to retrieve all
levels belonging to the same book snapshot atomically.

## Options Considered

### Option A: Add `best_bid`/`best_ask` columns to `lob_levels`

**Pros**: No new table; minimal schema change.

**Cons**: Redundant data on every level row; top-of-book queries still require
scanning multiple rows.

### Option B: New `lob_snapshots` table linking to `lob_levels` via `snapshot_id`

**Pros**: Clean separation of snapshot-level metadata and individual price
levels; one join (or no join) to get top-of-book; minimal data duplication.

**Cons**: Requires a new migration and DDL change; callers must be aware of two
tables.

### Option C: Store the entire LOB as JSON in a single row

**Pros**: Zero schema change to `lob_levels`; everything in one row.

**Cons**: Loses columnar query efficiency; QuestDB's strength is columnar
analytics; JSON querying is slower.

## Decision

Adopt **Option B**: create a `lob_snapshots` table and link each `lob_levels`
row to its parent snapshot via `snapshot_id`.

Key design choices:

1. `snapshot_id` is derived from the event timestamp in nanoseconds
   (`ts * 1_000_000`), guaranteeing uniqueness per LOB event within the same
   instrument.
2. `lob_snapshots` stores `best_bid_price`, `best_bid_size`, `best_ask_price`,
   `best_ask_size` so top-of-book queries require no join with `lob_levels`.
3. `lob_levels` stores `level INT` (0 = best price) so sorted-order queries
   are trivial without `ORDER BY price`.
4. Bids are sorted descending by price (best/highest first) and asks ascending
   by price (best/lowest first) before persistence, ensuring `level 0` is
   always the most competitive price.
5. Events where `best_bid_price > best_ask_price` (crossed book) are treated as
   invalid: an error is logged and **no rows** are persisted.
6. `lob_snapshots` uses WAL mode with hourly partitioning and 25-hour TTL,
   matching the higher retention expectation for snapshot metadata.

## Consequences

- New table: `lob_snapshots` (V3 migration).
- `lob_levels` gains `snapshot_id LONG` and `level INT` columns (V2 force-recreate).
- `persist_lob` now sorts the incoming arrays before persisting — the wire
  contract is no longer relied upon.
- Crossed-book events are silently dropped (after logging), preventing data
  corruption in analytics.
- Existing callers of `persist_lob` see no API change; the function signature
  is unchanged.
