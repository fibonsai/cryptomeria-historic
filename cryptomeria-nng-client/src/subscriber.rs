//! NNG SUB socket wrappers that receive framed market-data messages from one
//! or more PUB brokers.
//!
//! Each configured broker gets its own `Sub0` socket on its own
//! `spawn_blocking` task (see ADR-007 Option 3 / ADR-008). This isolates
//! connectivity: a `recv()` timeout or pipe removal on broker *B* is attributable
//! to *B* alone, so a down broker is logged even while sibling brokers keep
//! delivering data — the failure mode described in issue #20.

use nng::options::protocol::pubsub::Subscribe;
use nng::options::{Options, RecvTimeout, Url};
use nng::{Error, PipeEvent, Protocol, Socket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

const RECV_TIMEOUT_MS: u64 = 500;

/// Number of consecutive no-pipe `recv` timeouts before transitioning a broker
/// to `Down`. At 500 ms/timeout this gives a ~1 s debounce window, which avoids
/// false "down" logs during the initial SUB/PUB handshake (where the pipe count
/// is briefly 0) and transient reconnect blips — while still detecting a real
/// drop within ~1 s.
const DOWN_THRESHOLD_TICKS: u32 = 2;

/// Topic prefix for LOB messages.
pub const LOB_TOPIC_PREFIX: &str = "lob__";
/// Topic prefix for trade messages.
pub const TRADE_TOPIC_PREFIX: &str = "trade__";

/// Output of a single broker reader. Connectivity events (`Down`/`Up`) are
/// emitted once per state transition; `Message` is forwarded verbatim.
///
/// Using a structured enum (instead of bare `log::warn!` strings) keeps down /
/// up assertions deterministic in tests.
pub enum BrokerOutput {
    /// A message received from the broker.
    Message(nng::Message),
    /// The broker stopped producing data (pipe gone or sustained timeout) while
    /// it was previously considered up.
    Down { addr: String },
    /// The broker is reachable again after a `Down` transition.
    Up { addr: String },
}

/// Connectivity event emitted by [`connectivity_event`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnEvent {
    None,
    Down,
    Up,
}

/// Decide the connectivity event for a broker given its previous down state,
/// the current NNG pipe count, whether a message was received on this `recv()`
/// iteration, and the running streak of consecutive no-pipe timeouts.
///
/// `pipe_count` comes from the `pipe_notify` callback (number of live pipes for
/// this broker's dialer). Combining the recv outcome with the pipe count and a
/// debounce streak avoids false "down" logs during the initial SUB/PUB
/// handshake (pipe count briefly 0) and bursty feeds (connected but idle).
///
/// Returns `(new_down, new_streak, event)`.
pub fn connectivity_event(
    prev_down: bool,
    pipe_count: usize,
    recv_ok: bool,
    streak: u32,
) -> (bool, u32, Option<ConnEvent>) {
    if recv_ok {
        if prev_down {
            (false, 0, Some(ConnEvent::Up))
        } else {
            (prev_down, 0, None)
        }
    } else if pipe_count > 0 {
        // Connected but idle this tick: do not flap. Recover if we were down.
        if prev_down {
            (false, 0, Some(ConnEvent::Up))
        } else {
            (prev_down, 0, None)
        }
    } else {
        // No live pipe. Debt the debounce streak; only declare down once it
        // persists for `DOWN_THRESHOLD_TICKS` consecutive timeouts.
        let streak = streak + 1;
        if !prev_down && streak >= DOWN_THRESHOLD_TICKS {
            (true, streak, Some(ConnEvent::Down))
        } else {
            (prev_down, streak, None)
        }
    }
}

/// One subscriber socket bound to a single NNG PUB broker address.
pub struct BrokerReader {
    addr: String,
    socket: Socket,
    pipe_count: Arc<AtomicUsize>,
}

impl BrokerReader {
    /// Create a `BrokerReader` that dials `addr` asynchronously (non-blocking)
    /// and subscribes to all topics (empty prefix). A `pipe_notify` callback
    /// maintains a per-broker live-pipe count used to attribute connectivity.
    pub fn new(addr: String) -> Result<Self, Error> {
        let socket = Socket::new(Protocol::Sub0)?;
        socket.set_opt::<Subscribe>(Vec::<u8>::new())?;
        socket.set_opt::<RecvTimeout>(Some(Duration::from_millis(RECV_TIMEOUT_MS)))?;

        let pipe_count = Arc::new(AtomicUsize::new(0));
        let pipe_count_cb = Arc::clone(&pipe_count);
        let addr_cb = addr.clone();
        socket.pipe_notify(move |pipe, event| {
            let dialer = match pipe.dialer() {
                Some(d) => d,
                None => return,
            };
            let Ok(url) = dialer.get_opt::<Url>() else {
                return;
            };
            if url != addr_cb {
                return;
            }
            match event {
                PipeEvent::AddPost => {
                    pipe_count_cb.fetch_add(1, Ordering::SeqCst);
                }
                PipeEvent::RemovePost => {
                    pipe_count_cb.fetch_sub(1, Ordering::SeqCst);
                }
                _ => {}
            }
        })?;
        socket.dial_async(&addr)?;

        Ok(Self {
            addr,
            socket,
            pipe_count,
        })
    }

    /// Address this reader is dialing.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Close the underlying socket. Called by `NngSubscriber::drop` for brokers
    /// that were constructed but never `run`.
    pub(crate) fn close(&self) {
        self.socket.close();
    }

    /// Snapshot of the current live-pipe count (for diagnostics/tests).
    pub fn snapshot_count(&self) -> usize {
        self.pipe_count.load(Ordering::SeqCst)
    }

    /// Spawn a `spawn_blocking` task that receives from this broker's socket and
    /// forwards `BrokerOutput` events on `tx`. Connectivity state is tracked
    /// across iterations; `Down`/`Up` are emitted only on transitions, so a
    /// bursty-but-healthy broker never flaps.
    ///
    /// The task exits when `shutdown` is set or when `tx.send` fails (consumer
    /// dropped), closing the socket on exit. `tx` is a `std::sync::mpsc::Sender`
    /// (`Send` + `Sync`), so it can be moved into a `spawn_blocking` task.
    pub fn run(
        self,
        tx: std::sync::mpsc::Sender<BrokerOutput>,
        shutdown: Arc<AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        let BrokerReader {
            addr,
            socket,
            pipe_count,
        } = self;
        tokio::task::spawn_blocking(move || {
            let mut down = false;
            let mut streak = 0u32;
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                match socket.recv() {
                    Ok(msg) => {
                        let count = pipe_count.load(Ordering::SeqCst);
                        let (new_down, new_streak, ev) =
                            connectivity_event(down, count, true, streak);
                        down = new_down;
                        streak = new_streak;
                        if let Some(ConnEvent::Up) = ev
                            && tx.send(BrokerOutput::Up { addr: addr.clone() }).is_err()
                        {
                            break;
                        }
                        if tx.send(BrokerOutput::Message(msg)).is_err() {
                            break;
                        }
                    }
                    Err(Error::TimedOut) => {
                        let count = pipe_count.load(Ordering::SeqCst);
                        let (new_down, new_streak, ev) =
                            connectivity_event(down, count, false, streak);
                        down = new_down;
                        streak = new_streak;
                        if let Some(ConnEvent::Down) = ev
                            && tx.send(BrokerOutput::Down { addr: addr.clone() }).is_err()
                        {
                            break;
                        }
                        if let Some(ConnEvent::Up) = ev
                            && tx.send(BrokerOutput::Up { addr: addr.clone() }).is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        log::warn!("[subscriber] broker `{addr}` recv error: {e}");
                        std::thread::sleep(Duration::from_millis(500));
                    }
                }
            }
            socket.close();
        })
    }
}

/// A subscriber that fans out one `BrokerReader` per configured NNG PUB broker
/// address. Each broker runs on its own `spawn_blocking` task; the central
/// consumer receives `BrokerOutput` events on a single `mpsc` channel.
pub struct NngSubscriber {
    brokers: Vec<BrokerReader>,
}

impl NngSubscriber {
    /// Connect to one or more external NNG PUB brokers (e.g.
    /// `tcp://127.0.0.1:14242`). Each address gets its own `Sub0` socket dialed
    /// asynchronously, so construction does not fail when a broker is
    /// unreachable — reconnection is handled by NNG's internal dialer.
    pub fn new(addrs: &[String]) -> Result<Self, Error> {
        let brokers = addrs
            .iter()
            .map(|a| BrokerReader::new(a.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { brokers })
    }

    /// Spawn one `spawn_blocking` task per broker and return a single channel
    /// of `BrokerOutput` events plus the task handles. The channel closes once
    /// every broker task has exited (e.g. on `shutdown`).
    pub fn run(
        mut self,
        shutdown: Arc<AtomicBool>,
    ) -> (
        std::sync::mpsc::Receiver<BrokerOutput>,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        let brokers = std::mem::take(&mut self.brokers);
        let (tx, rx) = std::sync::mpsc::channel::<BrokerOutput>();
        let handles: Vec<_> = brokers
            .into_iter()
            .map(|b| b.run(tx.clone(), shutdown.clone()))
            .collect();
        drop(tx);
        (rx, handles)
    }

    /// Configured broker addresses, in order.
    pub fn addrs(&self) -> Vec<&str> {
        self.brokers.iter().map(|b| b.addr()).collect()
    }

    /// Per-broker live-pipe counts (for diagnostics/tests).
    pub fn pipe_counts(&self) -> Vec<(String, usize)> {
        self.brokers
            .iter()
            .map(|b| (b.addr().to_string(), b.snapshot_count()))
            .collect()
    }
}

impl Drop for NngSubscriber {
    fn drop(&mut self) {
        for b in &self.brokers {
            b.close();
        }
    }
}

/// Classify a topic into `("lob" | "trade", instrument)` or `None` when the
/// topic doesn't match either prefix.
pub fn classify_topic(topic: &str) -> Option<(&'static str, &str)> {
    if let Some(inst) = topic.strip_prefix(LOB_TOPIC_PREFIX)
        && !inst.is_empty()
    {
        return Some(("lob", inst));
    }
    if let Some(inst) = topic.strip_prefix(TRADE_TOPIC_PREFIX)
        && !inst.is_empty()
    {
        return Some(("trade", inst));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_lob_topic() {
        assert_eq!(classify_topic("lob__btcusdt"), Some(("lob", "btcusdt")));
    }

    #[test]
    fn classifies_trade_topic() {
        assert_eq!(classify_topic("trade__btcusd"), Some(("trade", "btcusd")));
    }

    #[test]
    fn returns_none_for_unknown_prefix() {
        assert_eq!(classify_topic("foo__bar"), None);
    }

    #[test]
    fn returns_none_for_empty_instrument() {
        assert_eq!(classify_topic("lob__"), None);
    }

    #[test]
    fn new_with_empty_addrs_creates_no_brokers() {
        let sub = NngSubscriber::new(&[]).unwrap();
        assert!(sub.brokers.is_empty());
    }

    #[test]
    fn new_with_single_addr_succeeds_without_broker() {
        let addr = "tcp://127.0.0.1:14242".to_string();
        let sub = NngSubscriber::new(&[addr]).unwrap();
        assert_eq!(sub.addrs(), vec!["tcp://127.0.0.1:14242"]);
    }

    #[test]
    fn new_with_single_addr_starts_with_zero_pipe_count() {
        let addr = "tcp://127.0.0.1:14242".to_string();
        let sub = NngSubscriber::new(&[addr]).unwrap();
        let counts = sub.pipe_counts();
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0].0, "tcp://127.0.0.1:14242");
        assert_eq!(counts[0].1, 0);
    }

    #[test]
    fn new_with_multiple_addrs_keeps_order() {
        let addrs = vec![
            "tcp://127.0.0.1:14243".to_string(),
            "tcp://127.0.0.1:14244".to_string(),
            "tcp://10.0.0.1:14242".to_string(),
        ];
        let sub = NngSubscriber::new(&addrs).unwrap();
        assert_eq!(sub.addrs().len(), 3);
    }

    // --- connectivity_event transition logic (hermetic, no network) ---

    #[test]
    fn event_healthy_message_no_change() {
        let (down, streak, ev) = connectivity_event(false, 1, true, 0);
        assert!(!down);
        assert_eq!(streak, 0);
        assert_eq!(ev, None);
    }

    #[test]
    fn event_connected_idle_no_flap() {
        let (down, streak, ev) = connectivity_event(false, 1, false, 0);
        assert!(!down);
        assert_eq!(streak, 0);
        assert_eq!(ev, None);
    }

    #[test]
    fn event_pipe_gone_first_timeout_scratches_streak_without_down() {
        let (down, streak, ev) = connectivity_event(false, 0, false, 0);
        assert!(!down);
        assert_eq!(streak, 1);
        assert_eq!(ev, None);
    }

    #[test]
    fn event_pipe_gone_second_timeout_emits_down() {
        let (down, streak, ev) = connectivity_event(false, 0, false, 1);
        assert!(down);
        assert_eq!(streak, 2);
        assert_eq!(ev, Some(ConnEvent::Down));
    }

    #[test]
    fn event_already_down_sustained_no_repeat() {
        let (down, _streak, ev) = connectivity_event(true, 0, false, 2);
        assert!(down);
        assert_eq!(ev, None);
    }

    #[test]
    fn event_recovery_via_pipe_reconnect() {
        let (down, _streak, ev) = connectivity_event(true, 1, false, 5);
        assert!(!down);
        assert_eq!(ev, Some(ConnEvent::Up));
    }

    #[test]
    fn event_recovery_via_message() {
        let (down, _streak, ev) = connectivity_event(true, 1, true, 0);
        assert!(!down);
        assert_eq!(ev, Some(ConnEvent::Up));
    }

    #[test]
    fn event_connected_idle_after_down_recovers() {
        let (down, _streak, ev) = connectivity_event(true, 1, false, 3);
        assert!(!down);
        assert_eq!(ev, Some(ConnEvent::Up));
    }
}
