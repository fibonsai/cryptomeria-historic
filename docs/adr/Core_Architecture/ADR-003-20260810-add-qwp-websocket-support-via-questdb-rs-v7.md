# ADR-003: Migrate to QuestDB WebSocket Protocol (QWP) via questdb-rs v7

- **Category**: Core Architecture
- **Status**: Accepted
- **Implemented**: 2026-08-10
- **Created**: 2026-08-10 16:00

## Context

The crate persisted market data to QuestDB via ILP (`Sender::from_conf` with
`questdb-rs` v6) and ran schema migrations / TTL via `reqwest` HTTP REST calls.
QuestDB 10 introduced QWP (QuestDB WebSocket Protocol), available in
`questdb-rs` v7 through `QuestDb::connect("ws::addr=localhost:9000;")`.

The v7 client provides a unified `QuestDb` pool that exposes both ingestion
(`QuestDb::borrow_sender` → `BorrowedSender`) and query
(`QuestDb::borrow_reader` → `BorrowedReader`) over a single WebSocket
connection. The v7 config parser (`questdb::db::conf::parse`) only accepts
`ws::addr=` and explicitly rejects `http::addr=`, so `QuestDb` is
WebSocket-only by design. ILP sender APIs (`Sender::from_conf` with
`http://`) still exist in v7 as a lower-level transport but have no associated
query/read path.

The user requested removing the `reqwest` HTTP fallback and consolidating on
a clean single-transport design.

## Options Considered

### Option A: Keep ILP ingestion, switch only migrations/TTL to QWP

Use `Sender::from_conf("http::addr=...")` for ingest and a separate
`QuestDb::connect("ws::addr=...")` for migrations/TTL queries.

**Cons:**
- Two concurrent connections per process (ILP + WebSocket) — doubles connection
  overhead and failure surface.
- `DEFAULT_QDB_CONF` must use `ws::addr=` anyway for migrations, so users
  running with `http::addr=` get two different config strings or must run a
  dual-listener QuestDB.
- The v7 `QuestDb` pool's `sync-sender` feature already includes QWP/WS ingest,
  making the ILP `Sender` redundant.

### Option B: Drop ILP, use QuestDb (QWP/WebSocket) for everything

**Adopted.** Use `QuestDb::connect("ws::addr=...")` as the single transport for
ingest, migrations, and TTL. Removes `reqwest` and `urlencoding` entirely.

**Pros:**
- Zero HTTP/REST dependency; the entire crate speaks one binary protocol.
- `QuestDb` is `Send + Sync` and wraps an internal pool — `Arc<QuestDb>` shares
  across threads, `BorrowedSender` / `BorrowedReader` are borrowed on the
  calling thread.
- Migrations and TTL reuse the same `BorrowedReader::execute` cursor API — no
  JSON REST parsing, no URL encoding.
- `sync-sender` (which in v7 includes `sync-sender-qwp-ws`) and `sync-reader`
  (which includes `sync-reader-qwp-ws`) are the two features that gate the
  synchronous, blocking sender and reader APIs respectively — both are
  on by default. We enable them explicitly for clarity.

**Cons:**
- ILP transport (`http::addr=`) is no longer supported by this crate. Users
  who need ILP must use the lower-level `Sender::from_conf` API directly or
  wrap the crate differently. QuestDB 10+ with QWP enabled is required.

## Decision

**Option B** — migrate to `questdb-rs` v7 with `QuestDb::connect` (QWP/WebSocket
only). Default config string changes from `http::addr=localhost:9000` to
`ws::addr=localhost:9000`. The `reqwest` and `urlencoding` dependencies are
removed; migrations and TTL execute via `BorrowedReader::execute` with cursor
iteration (`ColumnView::Int`, `ColumnView::Varchar`/`Symbol`).

## Consequences

### Positive

- **Single transport** — one WebSocket connection handles ingest and queries.
- **No HTTP fallback** — the codebase is smaller and has fewer failure modes.
- **Cursor-based query results** — typed column access replaces ad-hoc JSON
  parsing, giving compile-time confidence on column types.
- **`BorrowedSender` is `!Send`** — forces the borrow to live on the single
  receive thread, which is the existing architecture.

### Negative

- **QuestDB 10+ required** — QWP/WebSocket was introduced in QuestDB 10.
- **ILP transport dropped** — users configured with `http::addr=` must switch to
  `ws::addr=`.
- **v7 not yet on crates.io** — at the time of writing, v7.0.0 is only on the
  `main` branch of `questdb/c-questdb-client`; a path/git dependency is used
  until it is published.

### sync-sender / sync-reader features

`questdb-rs` v7 splits its synchronous (blocking) API behind two cargo features:

| Feature | Enables | Used for |
|---|---|---|
| `sync-sender` | `BorrowedSender`, `QuestDb::borrow_sender`, `Sender` | data ingestion (`flush_buffer`) |
| `sync-reader` | `BorrowedReader`, `Reader::execute`, `Cursor` | migrations, TTL, schema queries |

Both are on by default (since v7's `default` includes them), but are enabled
explicitly in `Cargo.toml` for clarity and to satisfy clippy's
`--all-features` CI gate.
