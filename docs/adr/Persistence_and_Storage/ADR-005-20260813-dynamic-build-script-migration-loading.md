# ADR-005: Load QuestDB migrations dynamically via build script

- **Category**: Persistence & Storage
- **Status**: Accepted
- **Implemented**: [PR #TBD](https://github.com/fibonsai/cryptomeria-historic/pull/PR_NUMBER)
- **Created**: 2026-08-13 15:33

## Context

`cryptomeria-historic` embeds SQL schema migrations as files under
`src/db/migrations/`, named with the convention `V{n}__{name}.sql`. Each
migration is registered in a hardcoded `const MIGRATIONS: &[Migration]` array
in `src/db/mod.rs` via `include_str!`.

This creates a two-place maintenance burden: adding a migration requires
creating the SQL file **and** manually editing the array. When the two drift
out of sync, the result is either a **compile error** (missing `include_str!`
target) or a **silent omission** (a file on disk that is never applied).

Concrete example from the repository history:

1. `V3__lob_snapshots.sql` was renamed to `V3__create_lob_snapshots.sql`, but
   the `include_str!` path in the hardcoded array was not updated — the crate
   **failed to compile** on `main`.
2. `V4__create_view_lob.sql` was added to disk but **never registered** in the
   array — it was silently skipped at every startup.

## Options Considered

### Option A: Runtime filesystem scan

Scan `src/db/migrations/` at process startup and build the `MIGRATIONS` list
dynamically.

**Pros:**
- Simplest code; no build script needed.
- Files are always in sync (read at startup, not compiled in).

**Cons:**
- The binary must ship with the `src/db/migrations/` directory at runtime,
  which is not how Rust binaries are typically deployed.
- `include_str!` is no longer used; SQL files are read with `std::fs` at
  runtime, losing compile-time embedding.
- Adds a runtime dependency on the local filesystem layout.

### Option B: `include_dir!` crate

Embed the entire `src/db/migrations/` directory as a virtual filesystem at
compile time.

**Pros:**
- Zero runtime filesystem dependency.
- Can iterate over embedded files at startup.

**Cons:**
- Adds an external build dependency (`include_dir` or similar).
- Requires restructuring how migrations are loaded (iterating a virtual FS
  instead of a const slice).
- Overkill for a small, fixed set of migration files.

### Option C: Build script (`build.rs`) scans at compile time

A `build.rs` script scans `src/db/migrations/` at compile time, parses each
`V{n}__{name>.sql` filename to extract the version and name, reads each SQL
file to extract the table/view name, and generates a `migrations.rs` file in
`$OUT_DIR` that is `include!`-d into `src/db/mod.rs`.

**Pros:**
- Zero runtime filesystem dependency (SQL is still embedded via `include_str!`).
- Zero new dependencies (uses only `std::fs` and string parsing).
- New migration files are picked up automatically on the next rebuild — no
  manual array update.
- The `Migration` struct stays `&'static str` (no struct changes needed).
- `cargo:rerun-if-changed` ensures correct incremental builds.

**Cons:**
- Adds a `build.rs` file (build-time complexity).
- Errors in migration file naming are caught at compile time (which is the
  desired behavior, but slightly less flexible than runtime).

## Decision

Adopt **Option C** — a `build.rs` build script that scans the migrations
directory at compile time and generates the `MIGRATIONS` const.

Key implementation details:

1. The build script scans `src/db/migrations/` for files matching
   `V{n}__{name>.sql`, parses the version (`i32`) and name (`String`) from
   the filename, and extracts the table/view name from the SQL content.
2. Generated code is written to `$OUT_DIR/migrations.rs` and pulled into
   `src/db/mod.rs` via `include!(concat!(env!("OUT_DIR"), "/migrations.rs"))`.
3. `cargo:rerun-if-changed=src/db/migrations/` is emitted so Cargo rebuilds
   when migration files change.
4. The `Migration` struct in `src/migrate.rs` is unchanged — all fields
   remain `&'static str` because `include_str!` and string literals are both
   `'static`.

## Consequences

- **Positive**: No more two-place updates. Adding a migration is now: create
  the SQL file and rebuild.
- **Positive**: The V4 view migration is now automatically registered and
  will be applied on first run.
- **Positive**: The V3 rename desync is resolved — the build script reads the
  current filename, not a stale hardcoded path.
- **Negative**: Build errors for malformed filenames or missing SQL files
  surface at compile time rather than runtime (acceptable trade-off for
  correctness).
- **Known limitation**: The force-recreate path in `migrate.rs` uses
  `DROP TABLE IF EXISTS {table_name}`, which does not work for views. This
  applies to the V4 `CREATE VIEW 'lob'` migration — on a SQL hash change, the
  DROP would fail. This is a pre-existing limitation and out of scope for this
  ADR; V4 applies cleanly via the INSERT path on first run.

## References

- [ADR-004: Store LOB snapshots with top-of-book fields](ADR-004-20260812-lob-snapshots-for-top-of-book-queries.md)
