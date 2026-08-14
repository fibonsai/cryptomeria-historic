# ADR-006: Idempotent view migrations and `--drop-first` escape hatch

- **Category**: Persistence & Storage
- **Status**: Accepted
- **Implemented**: [PR #TBD](https://github.com/fibonsai/cryptomeria-historic/pull/PR_NUMBER)
- **Created**: 2026-08-14 09:00

## Context

The V4 migration (`create_view_lob`) creates a `lob` view over `lob_snapshots` and
`lob_levels`. Two problems were identified:

1. **Non-idempotent view creation**: `CREATE VIEW 'lob' AS (...)` fails with "view already
   exists" if the view is present in QuestDB but not recorded in `schema_version` (e.g.,
   when a previous run partially succeeded, or when an operator manually creates the view).

2. **Force-recreate drops only tables**: When the migration SQL hash changes (detected via
   the `sql_hash` column in `schema_version`), `run_migrations` issues
   `DROP TABLE IF EXISTS {table_name}`. This fails for views — there is no way to
   distinguish a view migration from a table migration in the `Migration` struct.

3. **No operator escape hatch**: Operators have no way to force a clean schema reset when
   views or tables exist outside of `schema_version` tracking (e.g., stale state from a
   crashed migration, manual schema changes, or a corrupted `schema_version` table).

## Options Considered

### Option A: Prepend `DROP VIEW IF EXISTS` to V4 SQL only

**Pros:**
- Minimal change; fixes the immediate failure.

**Cons:**
- Does not address the force-recreate path, which still issues `DROP TABLE` for views.
- Does not provide an operator escape hatch.

### Option B: Track `is_view` in `Migration` and branch DROP statements

**Pros:**
- Both the idempotent SQL and the force-recreate path emit the correct `DROP VIEW` vs
  `DROP TABLE`.
- `build.rs` auto-detects view vs table from the SQL content, so no manual registration
  is needed.
- Clean separation of concerns.

**Cons:**
- Changes the `Migration` struct (breaking the auto-generated code contract), requiring
  updates to `build.rs`, `migrate.rs`, and all test sites.

### Option C: Always use `DROP TABLE IF EXISTS` unconditionally

**Pros:**
- No schema-level changes.

**Cons:**
- QuestDB does not support `DROP TABLE` for views — this would raise an error.
- Not viable.

## Decision

Adopt **Option B** with the following additions:

1. **Idempotent V4 SQL**: Prepend `DROP VIEW IF EXISTS 'lob';` to
   `src/db/migrations/V4__create_view_lob.sql` so that the CREATE VIEW always succeeds
   regardless of prior state.

2. **`is_view` field on `Migration`**: Add `is_view: bool` to the `Migration` struct in
   `src/migrate.rs`. The `build.rs` script detects whether each migration creates a view
   (via a shared `extract_table_and_view` helper that returns `(name, is_view)`) and
   populates the field automatically in the generated `MIGRATIONS` const.

3. **Correct DROP in force-recreate**: In `run_migrations`, branch on `is_view` to emit
   `DROP VIEW IF EXISTS` (for views) or `DROP TABLE IF EXISTS` (for tables) when a SQL hash
   mismatch triggers force-recreate.

4. **`--drop-first` CLI flag**: Add a `--drop-first` boolean flag to the `Cli` struct in
   `src/main.rs`. When set, `run_migrations` iterates all migrations in reverse version
   order, issues the appropriate `DROP VIEW IF EXISTS` / `DROP TABLE IF EXISTS` for each
   target, clears the `schema_version` table, and then applies all migrations from scratch.
   This is the escape hatch for operators dealing with stale or corrupted schema state.

## Consequences

- **Positive**: V4 migration is now idempotent — it can run safely against a database that
  already has the `lob` view.
- **Positive**: Force-recreate now works correctly for view migrations (SQL hash changes
  are handled gracefully).
- **Positive**: `--drop-first` gives operators a deterministic way to recover from schema
  drift without manually connecting to QuestDB.
- **Negative**: The `Migration` struct has a new field (`is_view`), which is a breaking
  change to the auto-generated code contract. Any hand-written `Migration` construction
  sites (e.g., in tests) must be updated.
- **Known limitation**: `extract_table_and_view` relies on string matching for
  `CREATE TABLE` / `CREATE VIEW`. Complex SQL with comments or inline DDL in unusual
  formats could confuse the parser. This is acceptable for the current small, controlled
  migration set.
