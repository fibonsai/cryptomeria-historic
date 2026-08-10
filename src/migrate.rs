//! Lightweight schema-versioned migration runner for QuestDB.
//!
//! Tracks applied migrations in a `schema_version` table and applies embedded
//! SQL files in version order via `BorrowedReader::execute` over QWP/WebSocket.

use chrono::Utc;
use questdb::QuestDb;
use questdb::egress::ColumnView;
use std::collections::HashMap;

const SCHEMA_VERSION_DDL: &str =
    "CREATE TABLE IF NOT EXISTS schema_version (version INT, name STRING, applied_on STRING)";

/// A single database migration.
pub struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub sql: &'static str,
}

/// A migration that has already been applied.
struct AppliedMigration {
    version: i32,
    name: String,
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
            .execute("SELECT version, name FROM schema_version ORDER BY version ASC")
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

            let version = match &version_col {
                ColumnView::Int(col) => col.value(0),
                _ => 0,
            };
            let name = match &name_col {
                ColumnView::Varchar(col) => col.value(0).unwrap_or("").to_string(),
                ColumnView::Symbol(col) => col.resolve(0).unwrap_or("").to_string(),
                _ => String::new(),
            };
            applied.push(AppliedMigration { version, name });
        }
        Ok(applied)
    }

    /// Run all migrations from `migrations` that have not yet been applied.
    pub async fn run_migrations(&self, migrations: &[Migration]) -> Result<(), String> {
        self.run_sql(SCHEMA_VERSION_DDL)?;
        let applied = self.list_applied()?;
        let applied_map: HashMap<i32, &AppliedMigration> =
            applied.iter().map(|m| (m.version, m)).collect();

        for migration in migrations {
            let version = migration.version;
            if let Some(existing) = applied_map.get(&version) {
                if existing.name != migration.name {
                    log::error!(
                        "[migrate] Divergent migration V{}: embedded name '{}' != applied name '{}'",
                        version,
                        migration.name,
                        existing.name
                    );
                }
                continue;
            }
            log::info!("[migrate] Applying V{}__{}...", version, migration.name);
            self.run_sql(migration.sql)
                .map_err(|e| format!("Migration V{}__{} failed: {e}", version, migration.name))?;
            let insert_sql = format!(
                "INSERT INTO schema_version (version, name, applied_on) VALUES ({}, '{}', '{}')",
                version,
                migration.name.replace('\'', "''"),
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
            );
            self.run_sql(&insert_sql)
                .map_err(|e| format!("Failed to record V{}__{}: {e}", version, migration.name))?;
            log::info!("[migrate] V{}__{} applied", version, migration.name);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_struct_holds_version_name_sql() {
        let m = Migration {
            version: 1,
            name: "test_migration",
            sql: "SELECT 1",
        };
        assert_eq!(m.version, 1);
        assert_eq!(m.name, "test_migration");
    }
}
