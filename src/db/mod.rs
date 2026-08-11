//! QuestDB connection, persistence, and migration wiring.
//!
//! Schema mirrors the normalised `MarketDataItem` published by
//! `cryptomeria-marketdata` over NNG — every column maps to a field that
//! actually exists on the wire payload.

use crate::items::{LobItem, TradeItem};
use crate::migrate::{Migration, QuestDbMigrator};
use anyhow::Result;
use questdb::BorrowedSender;
pub use questdb::QuestDb;
use questdb::ingress::{Buffer, TimestampNanos};
use std::time::{SystemTime, UNIX_EPOCH};

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "create_trades",
        table_name: "trades",
        sql: include_str!("migrations/V1__create_trades.sql"),
    },
    Migration {
        version: 2,
        name: "create_lob_levels",
        table_name: "lob_levels",
        sql: include_str!("migrations/V2__create_lob_levels.sql"),
    },
];

/// Default QuestDB connection-conf string (QDB_CLIENT_CONF format).
pub const DEFAULT_QDB_CONF: &str = "ws::addr=localhost:9000;username=admin;password=quest;";

/// Resolve the QuestDB configuration string.
/// Priority: CLI arg > `QDB_CLIENT_CONF` env var > hardcoded default.
pub fn resolve_questdb_conf(cli_conf: Option<&str>) -> String {
    if let Some(conf) = cli_conf {
        return conf.to_string();
    }
    if let Ok(env_conf) = std::env::var("QDB_CLIENT_CONF") {
        return env_conf;
    }
    DEFAULT_QDB_CONF.to_string()
}

/// Create a QuestDB [`QuestDb`] pool from a `QDB_CLIENT_CONF` formatted string.
pub async fn connect(conf_str: &str) -> Result<QuestDb> {
    let conf = if conf_str.is_empty() {
        DEFAULT_QDB_CONF
    } else {
        conf_str
    };
    Ok(QuestDb::connect(conf)?)
}

/// Run embedded SQL migrations against QuestDB via QWP/WebSocket.
pub async fn run_migrations(db: &QuestDb) -> Result<()> {
    let migrator = QuestDbMigrator::new(db);
    migrator
        .run_migrations(MIGRATIONS)
        .await
        .map_err(|e| anyhow::anyhow!("migration error: {e}"))?;
    Ok(())
}

/// Set QuestDB TTL for `trades` and `lob_levels`.
pub async fn apply_ttl(ttl_hours: u64, db: &QuestDb) -> Result<()> {
    if ttl_hours == 0 {
        return Ok(());
    }

    for table in &["lob_levels", "trades"] {
        let sql = format!("ALTER TABLE {} SET TTL {} HOURS", table, ttl_hours);
        let mut reader = db
            .borrow_reader()
            .map_err(|e| anyhow::anyhow!("failed to borrow reader: {e}"))?;
        let mut cursor = reader
            .execute(&sql)
            .map_err(|e| anyhow::anyhow!("TTL query failed for {table}: {e}"))?;
        while cursor.next_batch()?.is_some() {}
        log::info!("[ttl] table {table}: TTL set to {ttl_hours} hours");
    }
    Ok(())
}

/// Persist a single trade row to QuestDB.
pub fn persist_trade(sender: &mut BorrowedSender, inst_id: &str, trade: &TradeItem) -> Result<()> {
    let mut buffer = sender.new_buffer();
    let event_ts_nanos = (trade.ts as i64) * 1_000_000;
    let insert_ts_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow::anyhow!("clock error: {e}"))?
        .as_nanos() as i64;
    let latency = insert_ts_nanos - event_ts_nanos;
    let trade_id = trade.trade_id.as_deref().unwrap_or("");
    let seq_id = trade
        .seq_id
        .filter(|&v| v <= i64::MAX as u64)
        .map(|v| v as i64)
        .unwrap_or(-1);

    buffer
        .table("trades")?
        .symbol("inst_id", inst_id)?
        .symbol("exchange", &trade.exchange)?
        .symbol("trade_id", trade_id)?
        .symbol("side", &trade.side)?
        .column_f64("px", trade.price)?
        .column_f64("sz", trade.size)?
        .column_i64("seq_id", seq_id)?
        .column_i64("latency", latency)?
        .at(TimestampNanos::new(event_ts_nanos))?;
    sender.flush_buffer(&mut buffer)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_lob_level(
    buffer: &mut Buffer,
    inst_id: &str,
    exchange: &str,
    timestamp_nanos: i64,
    latency: i64,
    side: &str,
    price: f64,
    size: f64,
    best_diff: f64,
) -> Result<()> {
    buffer
        .table("lob_levels")?
        .symbol("inst_id", inst_id)?
        .symbol("exchange", exchange)?
        .symbol("side", side)?
        .column_f64("price", price)?
        .column_f64("size", size)?
        .column_f64("best_diff", best_diff)?
        .column_i64("latency", latency)?
        .at(TimestampNanos::new(timestamp_nanos))?;
    Ok(())
}

/// Compute the best-level difference for a price level.
/// For bids: `best_bid - price` (how far below the best bid).
/// For asks: `price - best_ask` (how far above the best ask).
pub fn compute_best_diff(side: &str, best: Option<f64>, price: f64) -> f64 {
    match (side, best) {
        ("bid", Some(bb)) => bb - price,
        ("ask", Some(ba)) => price - ba,
        _ => 0.0,
    }
}

/// Persist every level of an LOB item to QuestDB.
///
/// `best_bid` is the top bid price; `best_ask` the top ask price.
/// For bid levels `best_diff = best_bid - price`; for ask levels
/// `best_diff = price - best_ask`.
pub fn persist_lob(sender: &mut BorrowedSender, inst_id: &str, lob: &LobItem) -> Result<()> {
    let best_bid = lob.bids.first().map(|l| l.price);
    let best_ask = lob.asks.first().map(|l| l.price);
    let event_ts_nanos = (lob.ts as i64) * 1_000_000;
    let insert_ts_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow::anyhow!("clock error: {e}"))?
        .as_nanos() as i64;
    let latency = insert_ts_nanos - event_ts_nanos;
    let mut buffer = sender.new_buffer();

    for level in &lob.bids {
        let best_diff = compute_best_diff("bid", best_bid, level.price);
        write_lob_level(
            &mut buffer,
            inst_id,
            &lob.exchange,
            event_ts_nanos,
            latency,
            "bid",
            level.price,
            level.size,
            best_diff,
        )?;
    }
    for level in &lob.asks {
        let best_diff = compute_best_diff("ask", best_ask, level.price);
        write_lob_level(
            &mut buffer,
            inst_id,
            &lob.exchange,
            event_ts_nanos,
            latency,
            "ask",
            level.price,
            level.size,
            best_diff,
        )?;
    }
    sender.flush_buffer(&mut buffer)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_questdb_conf_cli_overrides_env() {
        let result = resolve_questdb_conf(Some("ws::addr=custom:9009;"));
        assert_eq!(result, "ws::addr=custom:9009;");
    }

    #[test]
    fn compute_best_diff_for_bid_and_ask() {
        let best_bid = Some(100.0);
        let best_ask = Some(101.0);

        assert!((compute_best_diff("bid", best_bid, 100.0) - 0.0).abs() < 1e-9);
        assert!((compute_best_diff("bid", best_bid, 99.0) - 1.0).abs() < 1e-9);

        assert!((compute_best_diff("ask", best_ask, 101.0) - 0.0).abs() < 1e-9);
        assert!((compute_best_diff("ask", best_ask, 102.0) - 1.0).abs() < 1e-9);

        assert_eq!(compute_best_diff("bid", None, 98.0), 0.0);
        assert_eq!(compute_best_diff("ask", None, 103.0), 0.0);

        assert_eq!(compute_best_diff("neutral", best_bid, 100.0), 0.0);
    }
}
