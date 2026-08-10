//! QuestDB connection, persistence, and migration wiring.
//!
//! Schema mirrors the normalised `MarketDataItem` published by
//! `cryptomeria-marketdata` over NNG — every column maps to a field that
//! actually exists on the wire payload.

use crate::items::{LobItem, TradeItem};
use crate::logging;
use crate::migrate::{Migration, QuestDbMigrator};
use anyhow::Result;
use questdb::ingress::{Buffer, Sender, TimestampNanos};
use reqwest::Client;
use std::env;
use std::time::Duration;

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "create_trades",
        sql: include_str!("migrations/V1__create_trades.sql"),
    },
    Migration {
        version: 2,
        name: "create_lob_levels",
        sql: include_str!("migrations/V2__create_lob_levels.sql"),
    },
];

/// Default QuestDB connection-conf string (QDB_CLIENT_CONF format).
pub const DEFAULT_QDB_CONF: &str = "http::addr=localhost:9000;username=admin;password=quest;";

/// Resolve the QuestDB configuration string.
/// Priority: CLI arg > `QDB_CLIENT_CONF` env var > hardcoded default.
pub fn resolve_questdb_conf(cli_conf: Option<&str>) -> String {
    if let Some(conf) = cli_conf {
        return conf.to_string();
    }
    if let Ok(env_conf) = env::var("QDB_CLIENT_CONF") {
        return env_conf;
    }
    DEFAULT_QDB_CONF.to_string()
}

fn extract_http_addr(conf_str: &str) -> String {
    for part in conf_str.split(';') {
        if let Some(stripped) = part.strip_prefix("http::addr=") {
            return stripped.to_string();
        }
        if let Some(stripped) = part.strip_prefix("https::addr=") {
            return stripped.to_string();
        }
    }
    "localhost:9000".to_string()
}

/// Create a QuestDB [`Sender`] from a `QDB_CLIENT_CONF` formatted string.
pub async fn connect(conf_str: &str) -> Result<Sender> {
    let conf = if conf_str.is_empty() {
        DEFAULT_QDB_CONF
    } else {
        conf_str
    };
    Ok(Sender::from_conf(conf)?)
}

/// Run embedded SQL migrations against QuestDB via its HTTP REST API.
pub async fn run_migrations(conf_str: &str) -> Result<()> {
    let http_addr = extract_http_addr(conf_str);
    let migrator = QuestDbMigrator::new(&http_addr);
    migrator
        .run_migrations(MIGRATIONS)
        .await
        .map_err(|e| anyhow::anyhow!("migration error: {e}"))?;
    Ok(())
}

/// Set QuestDB TTL for `trades` and `lob_levels`.
pub async fn apply_ttl(ttl_hours: u64, questdb_conf: &str) -> Result<()> {
    if ttl_hours == 0 {
        return Ok(());
    }
    let http_addr = extract_http_addr(questdb_conf);
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;

    for table in &["lob_levels", "trades"] {
        let sql = format!("ALTER TABLE {} SET TTL {} HOURS", table, ttl_hours);
        let url = format!(
            "http://{}/exec?query={}",
            http_addr,
            urlencoding::encode(&sql)
        );
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                let text = resp.text().await.unwrap_or_default();
                logging::warn("ttl", &format!("table {table}: {text}"));
            }
            Err(e) => {
                logging::warn("ttl", &format!("table {table}: {e}"));
            }
        }
    }
    Ok(())
}

/// Persist a single trade row to QuestDB.
pub fn persist_trade(sender: &mut Sender, inst_id: &str, trade: &TradeItem) -> Result<()> {
    let mut buffer = sender.new_buffer();
    let timestamp_nanos = (trade.ts as i64) * 1_000_000;
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
        .at(TimestampNanos::new(timestamp_nanos))?;
    sender.flush(&mut buffer)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_lob_level(
    buffer: &mut Buffer,
    inst_id: &str,
    exchange: &str,
    timestamp_nanos: i64,
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
pub fn persist_lob(sender: &mut Sender, inst_id: &str, lob: &LobItem) -> Result<()> {
    let best_bid = lob.bids.first().map(|l| l.price);
    let best_ask = lob.asks.first().map(|l| l.price);
    let timestamp_nanos = (lob.ts as i64) * 1_000_000;
    let mut buffer = sender.new_buffer();

    for level in &lob.bids {
        let best_diff = compute_best_diff("bid", best_bid, level.price);
        write_lob_level(
            &mut buffer,
            inst_id,
            &lob.exchange,
            timestamp_nanos,
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
            timestamp_nanos,
            "ask",
            level.price,
            level.size,
            best_diff,
        )?;
    }
    sender.flush(&mut buffer)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_http_addr_from_conf() {
        assert_eq!(
            extract_http_addr("http::addr=localhost:9000;username=admin;password=quest;"),
            "localhost:9000"
        );
        assert_eq!(
            extract_http_addr("username=admin;password=quest;"),
            "localhost:9000"
        );
    }

    #[test]
    fn resolve_questdb_conf_cli_overrides_env() {
        let result = resolve_questdb_conf(Some("http::addr=custom:9009;"));
        assert_eq!(result, "http::addr=custom:9009;");
    }

    #[test]
    fn compute_best_diff_for_bid_and_ask() {
        // best_bid = 100.0, best_ask = 101.0
        let best_bid = Some(100.0);
        let best_ask = Some(101.0);

        // bid best_diff = best_bid - price
        assert!((compute_best_diff("bid", best_bid, 100.0) - 0.0).abs() < 1e-9);
        assert!((compute_best_diff("bid", best_bid, 99.0) - 1.0).abs() < 1e-9);

        // ask best_diff = price - best_ask
        assert!((compute_best_diff("ask", best_ask, 101.0) - 0.0).abs() < 1e-9);
        assert!((compute_best_diff("ask", best_ask, 102.0) - 1.0).abs() < 1e-9);

        // no best price → 0.0
        assert_eq!(compute_best_diff("bid", None, 98.0), 0.0);
        assert_eq!(compute_best_diff("ask", None, 103.0), 0.0);

        // unknown side → 0.0
        assert_eq!(compute_best_diff("neutral", best_bid, 100.0), 0.0);
    }

    #[test]
    fn extract_http_addr_extracts_from_https_conf() {
        assert_eq!(extract_http_addr("https::addr=secure:9000;"), "secure:9000");
    }
}
