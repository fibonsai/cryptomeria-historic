//! cryptomeria-historic — subscribe to an NNG PUB broker and forward market data to QuestDB.
//!
//! Connects to an external NNG PUB socket (run by `cryptomeria-marketdata`),
//! receives framed `{topic}\0{json-payload}` messages, deserialises them into
//! normalised `MarketDataItem` values, and writes LOB levels / trades into
//! QuestDB via QWP/WebSocket with embedded schema-versioned migrations.

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use cryptomeria_historic::QuestDb;
use cryptomeria_historic::db;
use cryptomeria_historic::forward;
use cryptomeria_historic::items::MarketDataItem;
use cryptomeria_historic::logging;
use cryptomeria_historic::subscriber;
use questdb::BorrowedSender;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

const RECV_TIMEOUT_WARN_INTERVAL_SECS: u64 = 5;

/// CLI options.
#[derive(Parser, Clone)]
#[command(
    version,
    about = "Forward LOB/trade data from an NNG PUB broker to QuestDB"
)]
struct Cli {
    /// NNG PUB broker addresses to subscribe to, comma-separated
    /// (e.g. `tcp://127.0.0.1:14242,tcp://10.0.0.1:14242`).
    #[arg(long, default_value = "tcp://127.0.0.1:14242")]
    nng_addrs: String,

    /// QuestDB connection-conf string (QDB_CLIENT_CONF format).
    /// Example: `ws::addr=localhost:9000;username=admin;password=quest;`
    #[arg(long)]
    qdb_conf: Option<String>,

    /// QuestDB table TTL in hours. Applied via `ALTER TABLE … SET TTL`.
    #[arg(long)]
    ttl_hours: Option<u64>,

    /// Do not connect to QuestDB; receive and log only.
    #[arg(long)]
    dry_run: bool,

    /// Drop all migration targets (tables and views) and clear schema_version
    /// before applying migrations, forcing a full re-apply from scratch.
    #[arg(long)]
    drop_first: bool,

    /// Exit automatically after this many seconds (0 = no timeout; for CI).
    #[arg(long, default_value_t = 0)]
    test_timeout_secs: u64,
}

/// Process a single NNG wire message: split frame, classify topic, persist.
fn process_message(
    bytes: &[u8],
    sender: Option<&mut BorrowedSender>,
    dry_run: bool,
) -> anyhow::Result<()> {
    let (topic, item) =
        forward::parse_frame(bytes).with_context(|| "failed to parse wire frame")?;
    let (kind, inst_id) =
        subscriber::classify_topic(&topic).ok_or_else(|| anyhow!("unrecognised topic: {topic}"))?;
    let log_level = if dry_run {
        log::Level::Info
    } else {
        log::Level::Debug
    };

    match (&item, kind) {
        (MarketDataItem::Lob(lob), "lob") => {
            let level_count = lob.bids.len() + lob.asks.len();
            if let Some(s) = sender {
                db::persist_lob(s, inst_id, lob).context("failed to persist lob levels")?;
            }
            if dry_run || log::log_enabled!(log::Level::Debug) {
                log::log!(
                    log_level,
                    "[forwarder] {topic}: lob {level_count} levels (ts={}) exchange={}",
                    lob.ts,
                    lob.exchange
                );
            }
        }
        (MarketDataItem::Trade(trade), "trade") => {
            if let Some(s) = sender {
                db::persist_trade(s, inst_id, trade).context("failed to persist trade")?;
            }
            if dry_run || log::log_enabled!(log::Level::Debug) {
                log::log!(
                    log_level,
                    "[forwarder] {topic}: trade px={} sz={} side={} (ts={}) exchange={}",
                    trade.price,
                    trade.size,
                    trade.side,
                    trade.ts,
                    trade.exchange
                );
            }
        }
        _ => {
            log::warn!("topic/item kind mismatch: {topic} → {}", item_kind(&item));
        }
    }
    Ok(())
}

fn item_kind(item: &MarketDataItem) -> &'static str {
    match item {
        MarketDataItem::Lob(_) => "lob",
        MarketDataItem::Trade(_) => "trade",
    }
}

/// Parse a comma-separated list of NNG broker addresses into a `Vec<String>`.
///
/// Whitespace around each address is trimmed; empty entries are dropped.
fn parse_nng_addrs(addrs: &str) -> Vec<String> {
    addrs
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// Blocking receive loop running on a dedicated thread.
fn receive_loop(
    sub_socket: subscriber::NngSubscriber,
    questdb: Option<Arc<QuestDb>>,
    shutdown: Arc<AtomicBool>,
    dry_run: bool,
) {
    let mut sender: Option<BorrowedSender> = questdb.as_ref().map(|db| {
        db.borrow_sender()
            .expect("failed to borrow sender from QuestDB pool")
    });

    let mut last_timeout_warn = std::time::Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            log::info!("shutdown requested, stopping receive loop");
            break;
        }

        match sub_socket.recv() {
            Ok(message) => {
                last_timeout_warn = std::time::Instant::now();
                if let Err(e) = process_message(message.as_slice(), sender.as_mut(), dry_run) {
                    log::error!("processing error: {e}");
                }
            }
            Err(nng::Error::TimedOut) => {
                if last_timeout_warn.elapsed().as_secs() >= RECV_TIMEOUT_WARN_INTERVAL_SECS {
                    log::warn!(
                        "no messages received in {}s — NNG broker may be down",
                        last_timeout_warn.elapsed().as_secs()
                    );
                    last_timeout_warn = std::time::Instant::now();
                }
                continue;
            }
            Err(e) => {
                log::warn!("NNG recv error: {e}");
                thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    logging::init();

    let qdb_conf = db::resolve_questdb_conf(cli.qdb_conf.as_deref());

    log::info!("[system] NNG brokers: {}", cli.nng_addrs);
    log::info!("[system] QuestDB conf: {}", qdb_conf);

    // Connect QuestDB pool (skip in dry-run).
    let questdb: Option<Arc<QuestDb>> = if cli.dry_run {
        log::info!("[system] dry-run mode: QuestDB not connected");
        None
    } else {
        let qdb = db::connect(&qdb_conf)
            .await
            .context("failed to connect to QuestDB")?;
        log::info!("[system] connected to QuestDB");
        Some(Arc::new(qdb))
    };

    // Run migrations before starting the receive loop.
    if let Some(ref qdb) = questdb {
        db::run_migrations(qdb, cli.drop_first)
            .await
            .context("migration failed")?;
    }

    // Optional TTL override.
    if let (Some(ttl), Some(qdb)) = (cli.ttl_hours, &questdb) {
        db::apply_ttl(ttl, qdb)
            .await
            .context("TTL application failed")?;
    }

    // Connect NNG subscriber to all requested brokers.
    let nng_addrs = parse_nng_addrs(&cli.nng_addrs);
    log::info!("[system] connecting to {} NNG broker(s)", nng_addrs.len());
    for addr in &nng_addrs {
        log::info!("[system]   NNG dial: {addr}");
    }
    let sub_socket = subscriber::NngSubscriber::new(&nng_addrs).with_context(|| {
        format!(
            "failed to connect NNG subscriber to: {}",
            nng_addrs.join(", ")
        )
    })?;
    log::info!("[system] subscribed to NNG PUB broker(s)");

    // Spawn the blocking receive loop.
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_loop = Arc::clone(&shutdown);
    let handle =
        thread::spawn(move || receive_loop(sub_socket, questdb, shutdown_loop, cli.dry_run));
    log::info!("[system] forwarder running (Ctrl+C to stop)");

    // Wait for shutdown signal.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            log::info!("[system] Ctrl+C received, shutting down");
        }
        _ = async {
            if cli.test_timeout_secs > 0 {
                tokio::time::sleep(Duration::from_secs(cli.test_timeout_secs)).await;
            } else {
                std::future::pending::<()>().await;
            }
        } => {
            log::info!("[system] test timeout reached, shutting down");
        }
    }

    shutdown.store(true, Ordering::SeqCst);

    // Drain the recv timeout window (~500 ms) so the loop notices shutdown.
    if handle.join().is_err() {
        log::warn!("[system] forwarder thread did not join cleanly");
    }

    log::info!("[system] bye");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_defaults() {
        let cli = Cli::try_parse_from(["cryptomeria-historic"]).unwrap();
        assert_eq!(cli.nng_addrs, "tcp://127.0.0.1:14242");
        assert!(!cli.dry_run);
        assert!(!cli.drop_first);
        assert_eq!(cli.ttl_hours, None);
        assert_eq!(cli.test_timeout_secs, 0);
    }

    #[test]
    fn cli_accepts_multiple_nng_addrs() {
        let cli = Cli::try_parse_from([
            "cryptomeria-historic",
            "--nng-addrs",
            "tcp://1.2.3.4:14242,tcp://5.6.7.8:14242",
        ])
        .unwrap();
        assert_eq!(cli.nng_addrs, "tcp://1.2.3.4:14242,tcp://5.6.7.8:14242");
    }

    #[test]
    fn parse_nng_addrs_single() {
        let result = parse_nng_addrs("tcp://127.0.0.1:14242");
        assert_eq!(result, vec!["tcp://127.0.0.1:14242".to_string()]);
    }

    #[test]
    fn parse_nng_addrs_multiple() {
        let result = parse_nng_addrs("tcp://1.2.3.4:14242,tcp://5.6.7.8:14242");
        assert_eq!(
            result,
            vec![
                "tcp://1.2.3.4:14242".to_string(),
                "tcp://5.6.7.8:14242".to_string(),
            ]
        );
    }

    #[test]
    fn parse_nng_addrs_trims_whitespace_and_drops_empty() {
        let result = parse_nng_addrs(" tcp://a:1 , , tcp://b:2 ");
        assert_eq!(
            result,
            vec!["tcp://a:1".to_string(), "tcp://b:2".to_string(),]
        );
    }

    #[test]
    fn cli_accepts_drop_first_flag() {
        let cli = Cli::try_parse_from(["cryptomeria-historic", "--drop-first"]).unwrap();
        assert!(cli.drop_first);
    }

    #[test]
    fn cli_accepts_ws_qdb_conf() {
        let cli = Cli::try_parse_from([
            "cryptomeria-historic",
            "--qdb-conf",
            "ws::addr=localhost:9000;username=admin;password=quest;",
        ])
        .unwrap();
        assert_eq!(
            cli.qdb_conf,
            Some("ws::addr=localhost:9000;username=admin;password=quest;".to_string())
        );
    }

    #[test]
    fn item_kind_classifies_variants() {
        let lob = MarketDataItem::Lob(cryptomeria_historic::items::LobItem {
            ts: 0,
            exchange: String::new(),
            bids: vec![],
            asks: vec![],
        });
        let trade = MarketDataItem::Trade(cryptomeria_historic::items::TradeItem {
            ts: 0,
            exchange: String::new(),
            price: 0.0,
            size: 0.0,
            side: String::new(),
            trade_id: None,
            seq_id: None,
        });
        assert_eq!(item_kind(&lob), "lob");
        assert_eq!(item_kind(&trade), "trade");
    }

    fn lob_frame() -> Vec<u8> {
        let json = r#"{"lob":{"ts":123,"exchange":"okx","bids":[{"p":100.0,"s":1.0}],"asks":[{"p":101.0,"s":2.0}]}}"#;
        forward::frame_message("lob__btcusdt", json.as_bytes())
    }

    fn trade_frame() -> Vec<u8> {
        let json =
            r#"{"trade":{"ts":456,"exchange":"kraken","price":100.0,"size":1.5,"side":"buy"}}"#;
        forward::frame_message("trade__btcusd", json.as_bytes())
    }

    #[test]
    fn process_message_succeeds_in_dry_run_with_no_sender() {
        process_message(&lob_frame(), None, true).unwrap();
        process_message(&trade_frame(), None, true).unwrap();
    }

    #[test]
    fn process_message_succeeds_not_dry_run_with_no_sender() {
        process_message(&lob_frame(), None, false).unwrap();
        process_message(&trade_frame(), None, false).unwrap();
    }
}
