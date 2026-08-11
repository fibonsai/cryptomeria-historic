//! Lightweight schema-versioned migration runner for QuestDB.
//!
//! Tracks applied migrations in a `schema_version` table and applies embedded
//! SQL files in version order via `BorrowedReader::execute` over QWP/WebSocket.
//!
//! When an embedded migration's SQL content has changed (detected via a hash
//! stored alongside the version record), the migration is **force-recreated**:
//! the associated QuestDB table is dropped and the migration SQL re-run.  This
//! lets schema changes be made by editing existing `V{n}` files in place
//! rather than adding new migration versions.

use chrono::Utc;
use questdb::QuestDb;
use questdb::egress::ColumnView;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const SCHEMA_VERSION_DDL: &str = "CREATE TABLE IF NOT EXISTS schema_version (version INT, name STRING, sql_hash STRING, applied_on STRING)";

const ADD_SQL_HASH_COLUMN: &str = "ALTER TABLE schema_version ADD COLUMN sql_hash STRING";

/// A single database migration.
pub struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub table_name: &'static str,
    pub sql: &'static str,
}

/// A migration that has already been applied.
struct AppliedMigration {
    version: i32,
    name: String,
    sql_hash: Option<String>,
}

/// Compute a short hexadecimal hash of the migration SQL for change detection.
fn sql_hash(sql: &str) -> String {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

/// Runs SQL migrations against QuestDB over QWP/WebSocket.
pub struct QuestDbMigrator<'a> {
    db: &'a QuestDb,
}

impl<'a> QuestDbMigrator<'a> {
    pub fn new(db: &'a QuestDb) -> Self {
        QuestDbMigrator { db }
    }

    fn run_sql(&self, sql: &str) -> Result<(), String> {
        let mut reader = self
            .db
            .borrow_reader()
            .map_err(|e| format!("borrow reader error: {e}"))?;
        let mut cursor = reader
            .execute(sql)
            .map_err(|e| format!("execute error: {e}"))?;
        while cursor
            .next_batch()
            .map_err(|e| format!("cursor error: {e}"))?
            .is_some()
        {}
        Ok(())
    }

    fn list_applied(&self) -> Result<Vec<AppliedMigration>, String> {
        self.run_sql(SCHEMA_VERSION_DDL)?;
        let mut reader = self
            .db
            .borrow_reader()
            .map_err(|e| format!("borrow reader error: {e}"))?;
        let mut cursor = reader
            .execute("SELECT version, name, sql_hash FROM schema_version ORDER BY version ASC")
            .map_err(|e| format!("execute error: {e}"))?;

        let mut applied = Vec::new();
        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| format!("next_batch error: {e}"))?
        {
            if batch.row_count() == 0 {
                continue;
            }
            let version_col = batch.column(0).map_err(|e| format!("column error: {e}"))?;
            let name_col = batch.column(1).map_err(|e| format!("column error: {e}"))?;
            let hash_col = batch.column(2).map_err(|e| format!("column error: {e}"))?;

            let version = match &version_col {
                ColumnView::Int(col) => col.value(0),
                _ => 0,
            };
            let name = match &name_col {
                ColumnView::Varchar(col) => col.value(0).unwrap_or("").to_string(),
                ColumnView::Symbol(col) => col.resolve(0).unwrap_or("").to_string(),
                _ => String::new(),
            };
            let sql_hash = match &hash_col {
                ColumnView::Varchar(col) => col.value(0).map(|v| v.to_string()),
                ColumnView::Symbol(col) => col.resolve(0).map(|v| v.to_string()),
                _ => None,
            };
            applied.push(AppliedMigration {
                version,
                name,
                sql_hash,
            });
        }
        Ok(applied)
    }

    /// Run all migrations from `migrations` that have not yet been applied.
    ///
    /// If an already-applied migration's SQL hash differs from the embedded
    /// hash, the table is dropped and the migration re-run (force-recreate).
    pub async fn run_migrations(&self, migrations: &[Migration]) -> Result<(), String> {
        self.run_sql(SCHEMA_VERSION_DDL)?;
        let _ = self.run_sql(ADD_SQL_HASH_COLUMN);
        let applied = self.list_applied()?;
        let applied_map: HashMap<i32, &AppliedMigration> =
            applied.iter().map(|m| (m.version, m)).collect();

        for migration in migrations {
            let version = migration.version;
            let hash = sql_hash(migration.sql);
            if let Some(existing) = applied_map.get(&version) {
                if existing.sql_hash.as_deref() == Some(&hash) {
                    if existing.name != migration.name {
                        log::warn!(
                            "[migrate] V{}: name divergence — embedded '{}' != applied '{}'",
                            version,
                            migration.name,
                            existing.name
                        );
                    }
                    continue;
                }
                log::warn!(
                    "[migrate] V{}__{} SQL changed (hash mismatch), force-recreating table {}",
                    version,
                    migration.name,
                    migration.table_name
                );
                let drop_sql = format!("DROP TABLE IF EXISTS {}", migration.table_name);
                self.run_sql(&drop_sql).map_err(|e| {
                    format!(
                        "Failed to drop table for V{}__{}: {e}",
                        version, migration.name
                    )
                })?;
                self.run_sql(migration.sql).map_err(|e| {
                    format!("Migration V{}__{} failed: {e}", version, migration.name)
                })?;
                let update_sql = format!(
                    "UPDATE schema_version SET name = '{}', sql_hash = '{}', applied_on = '{}' WHERE version = {}",
                    migration.name.replace('\'', "''"),
                    hash,
                    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    version
                );
                self.run_sql(&update_sql).map_err(|e| {
                    format!("Failed to update V{}__{}: {e}", version, migration.name)
                })?;
                log::info!("[migrate] V{}__{} force-recreated", version, migration.name);
            } else {
                log::info!("[migrate] Applying V{}__{}...", version, migration.name);
                self.run_sql(migration.sql).map_err(|e| {
                    format!("Migration V{}__{} failed: {e}", version, migration.name)
                })?;
                let insert_sql = format!(
                    "INSERT INTO schema_version (version, name, sql_hash, applied_on) VALUES ({}, '{}', '{}', '{}')",
                    version,
                    migration.name.replace('\'', "''"),
                    hash,
                    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
                );
                self.run_sql(&insert_sql).map_err(|e| {
                    format!("Failed to record V{}__{}: {e}", version, migration.name)
                })?;
                log::info!("[migrate] V{}__{} applied", version, migration.name);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_struct_holds_version_name_table_sql() {
        let m = Migration {
            version: 1,
            name: "test_migration",
            table_name: "test_table",
            sql: "SELECT 1",
        };
        assert_eq!(m.version, 1);
        assert_eq!(m.name, "test_migration");
        assert_eq!(m.table_name, "test_table");
    }

    #[test]
    fn sql_hash_is_deterministic_and_stable() {
        let h1 = sql_hash("SELECT 1");
        let h2 = sql_hash("SELECT 1");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16);
    }

    #[test]
    fn sql_hash_differs_for_different_sql() {
        assert_ne!(sql_hash("SELECT 1"), sql_hash("SELECT 2"));
        assert_ne!(
            sql_hash("CREATE TABLE foo (a INT)"),
            sql_hash("CREATE TABLE foo (a LONG)")
        );
    }
}
