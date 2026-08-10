//! Lightweight schema-versioned migration runner for QuestDB.
//!
//! Tracks applied migrations in a `schema_version` table and applies embedded
//! SQL files in version order via QuestDB's HTTP REST endpoint.

use crate::logging;
use chrono::Utc;
use reqwest::Client;
use std::collections::HashMap;
use std::time::Duration;

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

/// Runs SQL migrations against QuestDB over HTTP.
pub struct QuestDbMigrator {
    client: Client,
    http_addr: String,
}

impl QuestDbMigrator {
    pub fn new(http_addr: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client should build");
        QuestDbMigrator {
            client,
            http_addr: http_addr.to_string(),
        }
    }

    async fn execute_sql(&self, sql: &str) -> Result<(), String> {
        let url = format!(
            "http://{}/exec?query={}",
            self.http_addr,
            urlencoding::encode(sql)
        );
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !response.status().is_success() {
            let text = response.text().await.map_err(|e| e.to_string())?;
            return Err(format!("QuestDB SQL error: {}", text));
        }
        Ok(())
    }

    async fn query_json(&self, sql: &str) -> Result<serde_json::Value, String> {
        let url = format!(
            "http://{}/exec?query={}",
            self.http_addr,
            urlencoding::encode(sql)
        );
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !response.status().is_success() {
            let text = response.text().await.map_err(|e| e.to_string())?;
            return Err(format!("QuestDB SQL error: {}", text));
        }
        let body = response.text().await.map_err(|e| e.to_string())?;
        serde_json::from_str(&body).map_err(|e| format!("JSON parse error: {e}"))
    }

    async fn ensure_schema_version_table(&self) -> Result<(), String> {
        self.execute_sql(SCHEMA_VERSION_DDL).await
    }

    async fn list_applied(&self) -> Result<Vec<AppliedMigration>, String> {
        if self.execute_sql(SCHEMA_VERSION_DDL).await.is_err() {
            return Ok(Vec::new());
        }
        let json = self
            .query_json("SELECT version, name FROM schema_version ORDER BY version ASC")
            .await?;
        let dataset = json["dataset"].as_array().cloned().unwrap_or_default();
        let mut applied = Vec::with_capacity(dataset.len());
        for row in &dataset {
            let parts = row.as_array().cloned().unwrap_or_default();
            if parts.len() < 2 {
                continue;
            }
            let version = parts[0].as_i64().unwrap_or(0) as i32;
            let name = parts[1].as_str().unwrap_or("").to_string();
            applied.push(AppliedMigration { version, name });
        }
        Ok(applied)
    }

    /// Run all migrations from `migrations` that have not yet been applied.
    pub async fn run_migrations(&self, migrations: &[Migration]) -> Result<(), String> {
        self.ensure_schema_version_table().await?;
        let applied = self.list_applied().await?;
        let applied_map: HashMap<i32, &AppliedMigration> =
            applied.iter().map(|m| (m.version, m)).collect();

        for migration in migrations {
            let version = migration.version;
            if let Some(existing) = applied_map.get(&version) {
                if existing.name != migration.name {
                    logging::error(
                        "migrate",
                        &format!(
                            "Divergent migration V{}: embedded name '{}' != applied name '{}'",
                            version, migration.name, existing.name
                        ),
                    );
                }
                continue;
            }
            logging::info(
                "migrate",
                &format!("Applying V{}__{}...", version, migration.name),
            );
            self.execute_sql(migration.sql)
                .await
                .map_err(|e| format!("Migration V{}__{} failed: {e}", version, migration.name))?;
            let insert_sql = format!(
                "INSERT INTO schema_version (version, name, applied_on) VALUES ({}, '{}', '{}')",
                version,
                migration.name.replace('\'', "''"),
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ")
            );
            self.execute_sql(&insert_sql)
                .await
                .map_err(|e| format!("Failed to record V{}__{}: {e}", version, migration.name))?;
            logging::info(
                "migrate",
                &format!("V{}__{} applied", version, migration.name),
            );
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
