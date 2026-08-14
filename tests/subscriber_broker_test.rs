//! Integration test for per-broker disconnect attribution (issue #20).
//!
//! Spins up three in-process mock NNG `Pub0` servers on distinct localhost
//! ports (hermetic: no Docker, no external network) and a `NngSubscriber`
//! that dials all three. Each broker runs on its own `spawn_blocking` task and
//! reports `Down`/`Up` events on the shared channel.
//!
//! The regression: when broker #2 is dropped while brokers #1 and #3 keep
//! publishing, the subscriber must emit `BrokerOutput::Down { addr: #2 }` **and**
//! continue surfacing `BrokerOutput::Message` events from #1 / #3 — the case
//! #20 describes as previously un-logged.

use cryptomeria_historic::forward;
use cryptomeria_historic::subscriber::{BrokerOutput, NngSubscriber};
use nng::{Protocol, Socket};
use serial_test::serial;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const SETTLE: Duration = Duration::from_millis(300);
const POLL: Duration = Duration::from_millis(100);

/// Discover a free localhost port by briefly opening a std TCP listener and
/// releasing it. The chosen port is then handed to NNG for listening.
fn free_addr() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    format!("tcp://127.0.0.1:{port}")
}

/// Spawns a publisher thread that sends `label \0 payload` frames every ~50 ms
/// until `stop` is set, then closes (and drops) its socket so the subscriber's
/// dialer observes `RemovePost`.
///
/// The thread binds to `addr` (retrying briefly for port reuse on reconnect).
fn spawn_publisher(
    addr: String,
    label: &str,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    let label = label.to_string();
    std::thread::spawn(move || {
        let sock = loop {
            let s = Socket::new(Protocol::Pub0).unwrap();
            if s.listen(&addr).is_ok() {
                break s;
            }
            // port still in TIME_WAIT or raced — retry briefly
            std::thread::sleep(Duration::from_millis(20));
        };
        while !stop.load(Ordering::Relaxed) {
            let frame = forward::frame_message(&label, b"payload");
            let _ = sock.send(&frame[..]);
            std::thread::sleep(Duration::from_millis(50));
        }
        sock.close();
    })
}

/// Broker label (first frame segment) of a message, if `ev` is a `Message`.
fn source_of(ev: &BrokerOutput) -> Option<String> {
    match ev {
        BrokerOutput::Message(m) => forward::split_frame(m.as_slice()).map(|(t, _)| t),
        _ => None,
    }
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn down_secondary_broker_logged_while_others_flow() {
    let addr1 = free_addr();
    let addr2 = free_addr();
    let addr3 = free_addr();

    let stop1 = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::new(AtomicBool::new(false));
    let stop3 = Arc::new(AtomicBool::new(false));

    let h1 = spawn_publisher(addr1.clone(), "broker1", stop1.clone());
    let h2 = spawn_publisher(addr2.clone(), "broker2", stop2.clone());
    let h3 = spawn_publisher(addr3.clone(), "broker3", stop3.clone());

    let addrs = vec![addr1.clone(), addr2.clone(), addr3.clone()];
    let shutdown = Arc::new(AtomicBool::new(false));
    let (rx, handles) = match NngSubscriber::new(&addrs) {
        Ok(s) => s.run(shutdown.clone()),
        Err(e) => {
            stop1.store(true, Ordering::Relaxed);
            stop2.store(true, Ordering::Relaxed);
            stop3.store(true, Ordering::Relaxed);
            let _ = h1.join();
            let _ = h2.join();
            let _ = h3.join();
            panic!("subscriber failed: {e:?}");
        }
    };

    // 1) All three brokers up: expect messages from each, no Down events.
    std::thread::sleep(SETTLE);
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut seen_sources = std::collections::HashSet::new();
    let mut saw_down = false;
    while Instant::now() < deadline {
        if let Ok(ev) = rx.recv_timeout(POLL) {
            if matches!(ev, BrokerOutput::Down { .. }) {
                saw_down = true;
            }
            if let Some(t) = source_of(&ev) {
                seen_sources.insert(t);
            }
        }
        if seen_sources.len() == 3 && !saw_down {
            break;
        }
    }
    for want in ["broker1", "broker2", "broker3"] {
        assert!(
            seen_sources.contains(want),
            "expected messages from {want}; got: {seen_sources:?}"
        );
    }
    assert!(
        !saw_down,
        "no broker should be down while all publishers are alive"
    );

    // 2) Drop broker #2 only — the scenario #20 says was un-logged.
    stop2.store(true, Ordering::Relaxed);
    let _ = h2.join();
    std::thread::sleep(SETTLE);

    // Expect Down{addr2} while messages from broker1/broker3 keep flowing.
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut saw_down_addr2 = false;
    let mut saw_message_while_down = false;
    while Instant::now() < deadline {
        match rx.recv_timeout(POLL) {
            Ok(BrokerOutput::Down { addr }) if addr == addr2 => saw_down_addr2 = true,
            Ok(ev) => {
                if saw_down_addr2
                    && matches!(source_of(&ev).as_deref(), Some("broker1") | Some("broker3"))
                {
                    saw_message_while_down = true;
                }
                let _ = ev;
            }
            Err(_) => {}
        }
        if saw_down_addr2 && saw_message_while_down {
            break;
        }
    }
    assert!(
        saw_down_addr2,
        "expected a Down{{addr: addr2}} event after dropping broker #2"
    );
    assert!(
        saw_message_while_down,
        "expected messages from broker #1/#3 to keep flowing after broker #2 went down (issue #20)"
    );

    // 3) Reconnect broker #2 and expect Up + resumed messages.
    stop2.store(false, Ordering::Relaxed);
    let h2b = spawn_publisher(addr2.clone(), "broker2", stop2.clone());
    std::thread::sleep(SETTLE);

    let deadline = Instant::now() + Duration::from_secs(4);
    let mut saw_up_addr2 = false;
    let mut saw_broker2_again = false;
    while Instant::now() < deadline {
        match rx.recv_timeout(POLL) {
            Ok(BrokerOutput::Up { addr }) if addr == addr2 => saw_up_addr2 = true,
            Ok(BrokerOutput::Message(m)) => {
                if forward::split_frame(m.as_slice()).map(|(t, _)| t) == Some("broker2".to_string())
                {
                    saw_broker2_again = true;
                }
            }
            Ok(_) => {}
            Err(_) => {}
        }
        if saw_up_addr2 && saw_broker2_again {
            break;
        }
    }
    assert!(
        saw_up_addr2,
        "expected Up{{addr: addr2}} after reconnecting broker #2"
    );
    assert!(
        saw_broker2_again,
        "expected resumed messages from broker #2"
    );

    // Clean shutdown.
    shutdown.store(true, Ordering::SeqCst);
    stop1.store(true, Ordering::Relaxed);
    stop3.store(true, Ordering::Relaxed);
    stop2.store(true, Ordering::Relaxed);
    let _ = h1.join();
    let _ = h2b.join();
    let _ = h3.join();
    for h in handles {
        let _ = h.await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn down_all_brokers_emits_each_down_event() {
    let addr1 = free_addr();
    let addr2 = free_addr();

    let stop1 = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::new(AtomicBool::new(false));
    let h1 = spawn_publisher(addr1.clone(), "broker1", stop1.clone());
    let h2 = spawn_publisher(addr2.clone(), "broker2", stop2.clone());

    let addrs = vec![addr1.clone(), addr2.clone()];
    let shutdown = Arc::new(AtomicBool::new(false));
    let (rx, handles) = NngSubscriber::new(&addrs)
        .expect("subscriber")
        .run(shutdown.clone());

    std::thread::sleep(SETTLE * 2);

    // Drop both publishers; both should be reported as Down.
    stop1.store(true, Ordering::Relaxed);
    stop2.store(true, Ordering::Relaxed);
    let _ = h1.join();
    let _ = h2.join();

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut downs: Vec<String> = Vec::new();
    while Instant::now() < deadline && downs.len() < 2 {
        match rx.recv_timeout(POLL) {
            Ok(BrokerOutput::Down { addr }) => downs.push(addr),
            Ok(_) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    shutdown.store(true, Ordering::SeqCst);
    for h in handles {
        let _ = h.await;
    }

    assert_eq!(
        downs.len(),
        2,
        "expected both brokers reported down: {downs:?}"
    );
    assert!(downs.contains(&addr1));
    assert!(downs.contains(&addr2));
}
