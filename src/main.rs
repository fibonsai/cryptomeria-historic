//! cryptomeria-historic — subscribe to an NNG PUB broker and forward market data to QuestDB.
//!
//! Connects to an external NNG PUB socket (run by `cryptomeria-marketdata`),
//! receives framed `{topic}\0{json-payload}` messages, deserialises them into
//! normalised `MarketDataItem` values, and writes LOB levels / trades into
//! QuestDB via ILP with embedded schema-versioned migrations.

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use cryptomeria_historic::db;
use cryptomeria_historic::forward;
use cryptomeria_historic::items::MarketDataItem;
use cryptomeria_historic::logging;
use cryptomeria_historic::subscriber;
use questdb::ingress::Sender;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

/// CLI options.
#[derive(Parser, Clone)]
#[command(
    version,
    about = "Forward LOB/trade data from an NNG PUB broker to QuestDB"
)]
struct Cli {
    /// NNG PUB broker address to subscribe to (e.g. `tcp://127.0.0.1:14242`).
    #[arg(long, default_value = "tcp://127.0.0.1:14242")]
    nng_addr: String,

    /// QuestDB connection-conf string. Overrides the `QDB_CLIENT_CONF` env var.
    #[arg(long)]
    qdb_conf: Option<String>,

    /// QuestDB table TTL in hours. Applied via `ALTER TABLE … SET TTL`.
    #[arg(long)]
    ttl_hours: Option<u64>,

    /// Do not connect to QuestDB; receive and log only.
    #[arg(long)]
    dry_run: bool,

    /// Exit automatically after this many seconds (0 = no timeout; for CI).
    #[arg(long, default_value_t = 0)]
    test_timeout_secs: u64,
}

/// Process a single NNG wire message: split frame, classify topic, persist.
fn process_message(bytes: &[u8], sender: Option<&mut Sender>) -> anyhow::Result<()> {
    let (topic, item) =
        forward::parse_frame(bytes).with_context(|| "failed to parse wire frame")?;
    let (kind, inst_id) =
        subscriber::classify_topic(&topic).ok_or_else(|| anyhow!("unrecognised topic: {topic}"))?;

    match (&item, kind) {
        (MarketDataItem::Lob(lob), "lob") => {
            let level_count = lob.bids.len() + lob.asks.len();
            if let Some(s) = sender {
                db::persist_lob(s, inst_id, lob).context("failed to persist lob levels")?;
            }
            log::info!("{topic}: lob {level_count} levels (ts={})", lob.ts);
        }
        (MarketDataItem::Trade(trade), "trade") => {
            if let Some(s) = sender {
                db::persist_trade(s, inst_id, trade).context("failed to persist trade")?;
            }
            log::info!(
                "{topic}: trade px={} sz={} side={} (ts={})",
                trade.price,
                trade.size,
                trade.side,
                trade.ts
            );
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

/// Blocking receive loop running on a dedicated thread.
fn receive_loop(
    sub_socket: subscriber::NngSubscriber,
    mut sender: Option<Sender>,
    shutdown: Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            log::info!("shutdown requested, stopping receive loop");
            break;
        }

        match sub_socket.recv() {
            Ok(message) => {
                if let Err(e) = process_message(message.as_slice(), sender.as_mut()) {
                    log::error!("processing error: {e}");
                }
            }
            Err(nng::Error::TimedOut) => continue,
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

    log::info!("[system] NNG broker: {}", cli.nng_addr);
    log::info!("[system] QuestDB conf: {}", qdb_conf);

    // Run migrations before connecting the ILP sender.
    db::run_migrations(&qdb_conf)
        .await
        .context("migration failed")?;

    // Optional TTL override.
    if let Some(ttl) = cli.ttl_hours {
        db::apply_ttl(ttl, &qdb_conf)
            .await
            .context("TTL application failed")?;
    }

    // Connect QuestDB ILP sender (skip in dry-run).
    let sender: Option<Sender> = if cli.dry_run {
        log::info!("[system] dry-run mode: QuestDB sender not connected");
        None
    } else {
        let s = db::connect(&qdb_conf)
            .await
            .context("failed to connect to QuestDB")?;
        log::info!("[system] connected to QuestDB");
        Some(s)
    };

    // Connect NNG subscriber.
    let sub_socket = subscriber::NngSubscriber::new(&cli.nng_addr)
        .with_context(|| format!("failed to connect NNG subscriber to {}", cli.nng_addr))?;
    log::info!("[system] subscribed to NNG PUB broker");

    // Spawn the blocking receive loop.
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_loop = Arc::clone(&shutdown);
    let handle = thread::spawn(move || receive_loop(sub_socket, sender, shutdown_loop));
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
        assert_eq!(cli.nng_addr, "tcp://127.0.0.1:14242");
        assert!(!cli.dry_run);
        assert_eq!(cli.ttl_hours, None);
        assert_eq!(cli.test_timeout_secs, 0);
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
}
