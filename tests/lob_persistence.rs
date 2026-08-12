use cryptomeria_historic::db;
use cryptomeria_historic::items::{LobItem, LobLevel};
use questdb::egress::ColumnView;
use serial_test::serial;
use std::time::Duration;
use testcontainers::GenericImage;
use testcontainers::core::{ImageExt, IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;

const QUESTDB_IMAGE: &str = "questdb/questdb";

async fn connect_with_retry(conf: &str, max_retries: u64) -> cryptomeria_historic::QuestDb {
    for attempt in 0..max_retries {
        match db::connect(conf).await {
            Ok(qdb) => return qdb,
            Err(e) => {
                if attempt == max_retries - 1 {
                    panic!("failed to connect to QuestDB after {max_retries} retries: {e}");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    unreachable!()
}

fn start_questdb() -> (testcontainers::Container<GenericImage>, String) {
    let image = GenericImage::new(QUESTDB_IMAGE, "latest")
        .with_exposed_port(9000u16.tcp())
        .with_wait_for(WaitFor::message_on_stdout("questdb"))
        .with_env_var("QDB_ADMIN_PASSWORD", "quest")
        .with_env_var("QDB_PG_ENABLED", "false");

    let container = image.start().expect("failed to start QuestDB container");
    let host_port = container
        .get_host_port_ipv4(9000u16)
        .expect("failed to get host port for 9000");

    let conf = format!(
        "ws::addr=127.0.0.1:{};username=admin;password=quest;",
        host_port
    );

    (container, conf)
}

fn build_sample_lob() -> LobItem {
    LobItem {
        ts: 1786524350108,
        exchange: "okx".to_string(),
        // bids in ascending order — NOT the expected descending/best-first order.
        bids: vec![
            LobLevel {
                price: 63720.5,
                size: 0.15695879,
            },
            LobLevel {
                price: 63723.1,
                size: 0.13561901,
            },
            LobLevel {
                price: 63724.3,
                size: 0.03138991,
            },
        ],
        // asks already in ascending order (best/lowest first).
        asks: vec![
            LobLevel {
                price: 63724.4,
                size: 0.0026,
            },
            LobLevel {
                price: 63725.0,
                size: 0.34837142,
            },
            LobLevel {
                price: 63725.2,
                size: 0.34523342,
            },
        ],
    }
}

fn approx_eq(a: f64, b: f64, msg: &str) {
    assert!((a - b).abs() < 1e-6, "{msg}: expected {b}, got {a}");
}

#[test]
#[serial]
fn lob_persistence_round_trip() {
    let (_container, conf) = start_questdb();
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let qdb = connect_with_retry(&conf, 60).await;
        db::run_migrations(&qdb).await.expect("migrations failed");

        let lob = build_sample_lob();
        let snapshot_id_expected = (lob.ts as i64) * 1_000_000;

        {
            let mut sender = qdb.borrow_sender().expect("failed to borrow sender");
            db::persist_lob(&mut sender, "btcusdt", &lob).expect("persist_lob failed");
        }

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let mut reader = qdb.borrow_reader().expect("failed to borrow reader");

        // ── Query lob_snapshots ──
        {
            let mut cursor = reader
                .execute(
                    "SELECT snapshot_id, inst_id, exchange, sequence, \
                     best_bid_price, best_bid_size, best_ask_price, best_ask_size \
                     FROM lob_snapshots",
                )
                .expect("query failed");

            #[allow(clippy::type_complexity)]
            let mut snapshots: Vec<(i64, String, String, i64, f64, f64, f64, f64)> = Vec::new();
            while let Some(batch) = cursor.next_batch().expect("next_batch failed") {
                for row in 0..batch.row_count() {
                    let sid = match batch.column(0).expect("col 0") {
                        ColumnView::Long(c) => c.value(row),
                        _ => panic!("expected Long for snapshot_id"),
                    };
                    let inst = match batch.column(1).expect("col 1") {
                        ColumnView::Symbol(c) => c.resolve(row).unwrap_or("").to_string(),
                        ColumnView::Varchar(c) => c.value(row).unwrap_or("").to_string(),
                        _ => panic!("expected Symbol for inst_id"),
                    };
                    let exch = match batch.column(2).expect("col 2") {
                        ColumnView::Symbol(c) => c.resolve(row).unwrap_or("").to_string(),
                        ColumnView::Varchar(c) => c.value(row).unwrap_or("").to_string(),
                        _ => panic!("expected Symbol for exchange"),
                    };
                    let seq = match batch.column(3).expect("col 3") {
                        ColumnView::Long(c) => c.value(row),
                        _ => panic!("expected Long for sequence"),
                    };
                    let bbp = match batch.column(4).expect("col 4") {
                        ColumnView::Double(c) => c.value(row),
                        _ => panic!("expected Double for best_bid_price"),
                    };
                    let bbs = match batch.column(5).expect("col 5") {
                        ColumnView::Double(c) => c.value(row),
                        _ => panic!("expected Double for best_bid_size"),
                    };
                    let bap = match batch.column(6).expect("col 6") {
                        ColumnView::Double(c) => c.value(row),
                        _ => panic!("expected Double for best_ask_price"),
                    };
                    let bas = match batch.column(7).expect("col 7") {
                        ColumnView::Double(c) => c.value(row),
                        _ => panic!("expected Double for best_ask_size"),
                    };
                    snapshots.push((sid, inst, exch, seq, bbp, bbs, bap, bas));
                }
            }

            assert_eq!(snapshots.len(), 1, "expected exactly 1 snapshot row");
            let (sid, inst, exch, seq, bbp, bbs, bap, bas) = &snapshots[0];
            assert_eq!(*sid, snapshot_id_expected, "snapshot_id mismatch");
            assert_eq!(inst, "btcusdt", "inst_id mismatch");
            assert_eq!(exch, "okx", "exchange mismatch");
            assert_eq!(*seq, lob.ts as i64, "sequence mismatch");
            approx_eq(*bbp, 63724.3, "best_bid_price");
            approx_eq(*bbs, 0.03138991, "best_bid_size");
            approx_eq(*bap, 63724.4, "best_ask_price");
            approx_eq(*bas, 0.0026, "best_ask_size");
        }

        // ── Query lob_levels (ask first, then bid, ordered by level) ──
        let mut cursor = reader
            .execute(
                "SELECT level, side, price, size FROM lob_levels \
                 ORDER BY side ASC, level ASC",
            )
            .expect("query failed");

        let mut levels: Vec<(i32, String, f64, f64)> = Vec::new();
        while let Some(batch) = cursor.next_batch().expect("next_batch failed") {
            for row in 0..batch.row_count() {
                let level = match batch.column(0).expect("col 0") {
                    ColumnView::Int(c) => c.value(row),
                    _ => panic!("expected Int for level"),
                };
                let side = match batch.column(1).expect("col 1") {
                    ColumnView::Symbol(c) => c.resolve(row).unwrap_or("").to_string(),
                    ColumnView::Varchar(c) => c.value(row).unwrap_or("").to_string(),
                    _ => panic!("expected Symbol for side"),
                };
                let price = match batch.column(2).expect("col 2") {
                    ColumnView::Double(c) => c.value(row),
                    _ => panic!("expected Double for price"),
                };
                let size = match batch.column(3).expect("col 3") {
                    ColumnView::Double(c) => c.value(row),
                    _ => panic!("expected Double for size"),
                };
                levels.push((level, side, price, size));
            }
        }

        assert_eq!(levels.len(), 6, "expected 6 level rows (3 asks + 3 bids)");

        // Asks sorted ascending (level 0 = best/lowest ask)
        assert_eq!(levels[0].0, 0, "ask level index");
        assert_eq!(levels[0].1, "ask", "ask side");
        approx_eq(levels[0].2, 63724.4, "ask level 0 price");
        approx_eq(levels[0].3, 0.0026, "ask level 0 size");

        assert_eq!(levels[1].0, 1, "ask level index");
        assert_eq!(levels[1].1, "ask", "ask side");
        approx_eq(levels[1].2, 63725.0, "ask level 1 price");
        approx_eq(levels[1].3, 0.34837142, "ask level 1 size");

        assert_eq!(levels[2].0, 2, "ask level index");
        assert_eq!(levels[2].1, "ask", "ask side");
        approx_eq(levels[2].2, 63725.2, "ask level 2 price");
        approx_eq(levels[2].3, 0.34523342, "ask level 2 size");

        // Bids sorted descending (level 0 = best/highest bid)
        assert_eq!(levels[3].0, 0, "bid level index");
        assert_eq!(levels[3].1, "bid", "bid side");
        approx_eq(levels[3].2, 63724.3, "bid level 0 price");
        approx_eq(levels[3].3, 0.03138991, "bid level 0 size");

        assert_eq!(levels[4].0, 1, "bid level index");
        assert_eq!(levels[4].1, "bid", "bid side");
        approx_eq(levels[4].2, 63723.1, "bid level 1 price");
        approx_eq(levels[4].3, 0.13561901, "bid level 1 size");

        assert_eq!(levels[5].0, 2, "bid level index");
        assert_eq!(levels[5].1, "bid", "bid side");
        approx_eq(levels[5].2, 63720.5, "bid level 2 price");
        approx_eq(levels[5].3, 0.15695879, "bid level 2 size");
    });
}

#[test]
#[serial]
fn lob_persistence_drops_invalid_crossed_book() {
    let (_container, conf) = start_questdb();
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let qdb = connect_with_retry(&conf, 60).await;
        db::run_migrations(&qdb).await.expect("migrations failed");

        // best_bid (100.0) > best_ask (90.0) — invalid crossed book.
        let lob = LobItem {
            ts: 1000,
            exchange: "okx".to_string(),
            bids: vec![LobLevel {
                price: 100.0,
                size: 1.0,
            }],
            asks: vec![LobLevel {
                price: 90.0,
                size: 2.0,
            }],
        };

        let result = {
            let mut sender = qdb.borrow_sender().expect("failed to borrow sender");
            db::persist_lob(&mut sender, "btcusdt", &lob)
        };
        assert!(
            result.is_ok(),
            "persist_lob should return Ok even for invalid crossed book"
        );

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let mut reader = qdb.borrow_reader().expect("failed to borrow reader");

        // Verify no snapshots persisted
        {
            let mut cursor = reader
                .execute("SELECT snapshot_id FROM lob_snapshots")
                .expect("query failed");
            let mut snapshot_count = 0usize;
            while let Some(batch) = cursor.next_batch().expect("next_batch failed") {
                snapshot_count += batch.row_count();
            }
            assert_eq!(
                snapshot_count, 0,
                "lob_snapshots should be empty for invalid crossed book"
            );
        }

        // Verify no levels persisted
        let mut cursor = reader
            .execute("SELECT level FROM lob_levels")
            .expect("query failed");
        let mut level_count = 0usize;
        while let Some(batch) = cursor.next_batch().expect("next_batch failed") {
            level_count += batch.row_count();
        }
        assert_eq!(
            level_count, 0,
            "lob_levels should be empty for invalid crossed book"
        );
    });
}
