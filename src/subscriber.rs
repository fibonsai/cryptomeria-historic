//! NNG SUB socket wrapper that receives framed market-data messages.
//!
//! Subscribes to all topics (empty prefix) and filters by `lob__` / `trade__`
//! in the processing loop, mirroring the approach used by
//! `cryptomeria-marketdata/src/subscriber.rs`.

use nng::options::protocol::pubsub::Subscribe;
use nng::options::{Options, RecvTimeout, Url};
use nng::{Error, PipeEvent, Protocol, Socket};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const RECV_TIMEOUT_MS: u64 = 500;

/// Topic prefix for LOB messages.
pub const LOB_TOPIC_PREFIX: &str = "lob__";
/// Topic prefix for trade messages.
pub const TRADE_TOPIC_PREFIX: &str = "trade__";

/// A connected NNG subscriber that may dial multiple PUB brokers.
pub struct NngSubscriber {
    socket: Socket,
    addrs: Vec<String>,
    pipe_counts: Arc<Mutex<HashMap<String, usize>>>,
}

impl NngSubscriber {
    /// Connect to one or more external NNG PUB brokers (e.g.
    /// `tcp://127.0.0.1:14242`) and subscribe to all topics.
    ///
    /// A single SUB socket dials every address in `addrs`, so messages from
    /// all brokers are multiplexed onto the same socket.  Each dial is
    /// non-blocking (`dial_async`) so construction does not fail when a broker
    /// is unreachable — reconnection is handled by NNG's internal dialer.
    pub fn new(addrs: &[String]) -> Result<Self, Error> {
        let socket = Socket::new(Protocol::Sub0)?;
        // Empty subscription = receive all messages; filtering happens in-process.
        socket.set_opt::<Subscribe>(Vec::<u8>::new())?;
        socket.set_opt::<RecvTimeout>(Some(Duration::from_millis(RECV_TIMEOUT_MS)))?;

        let mut initial_counts = HashMap::with_capacity(addrs.len());
        for addr in addrs {
            initial_counts.insert(addr.clone(), 0);
        }
        let pipe_counts: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(initial_counts));
        let pipe_counts_cb = Arc::clone(&pipe_counts);

        socket.pipe_notify(move |pipe, event| {
            let dialer = match pipe.dialer() {
                Some(d) => d,
                None => return,
            };
            let Ok(url) = dialer.get_opt::<Url>() else {
                return;
            };
            let mut counts = pipe_counts_cb.lock().unwrap();
            match event {
                PipeEvent::AddPost => {
                    *counts.entry(url).or_insert(0) += 1;
                }
                PipeEvent::RemovePost => {
                    if let Some(c) = counts.get_mut(&url) {
                        *c = c.saturating_sub(1);
                    }
                }
                _ => {}
            }
        })?;

        for addr in addrs {
            socket.dial_async(addr)?;
        }

        Ok(NngSubscriber {
            socket,
            addrs: addrs.to_vec(),
            pipe_counts,
        })
    }

    /// Block until a message arrives or the recv-timeout elapses.
    ///
    /// Returns `Err(Error::TimedOut)` when no message arrives within the
    /// configured timeout, allowing the caller to check a shutdown flag.
    pub fn recv(&self) -> Result<nng::Message, Error> {
        self.socket.recv()
    }

    /// Return the subset of configured broker addresses that currently have
    /// no active pipe (i.e. are not connected).
    ///
    /// Addresses are matched against the per-pipe counters maintained by the
    /// `pipe_notify` callback.  An address with a count of zero is considered
    /// down.
    pub fn down_addrs(&self) -> Vec<String> {
        let counts = self.pipe_counts.lock().unwrap();
        self.addrs
            .iter()
            .filter(|addr| counts.get(*addr).copied().unwrap_or(0) == 0)
            .cloned()
            .collect()
    }
}

impl Drop for NngSubscriber {
    fn drop(&mut self) {
        self.socket.close();
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
    fn new_with_empty_addrs_creates_socket_without_dialing() {
        let _sub = NngSubscriber::new(&[]).unwrap();
    }

    #[test]
    fn new_with_single_addr_succeeds_without_broker() {
        let addr = "tcp://127.0.0.1:14242".to_string();
        let _sub = NngSubscriber::new(&[addr]).unwrap();
    }

    #[test]
    fn down_addrs_returns_all_configured_when_none_connected() {
        let addrs = vec![
            "tcp://127.0.0.1:14243".to_string(),
            "tcp://127.0.0.1:14244".to_string(),
        ];
        let sub = NngSubscriber::new(&addrs).unwrap();
        let down = sub.down_addrs();
        assert_eq!(down.len(), addrs.len());
        for addr in &addrs {
            assert!(down.contains(addr));
        }
    }

    #[test]
    fn down_addrs_returns_empty_for_empty_addrs() {
        let sub = NngSubscriber::new(&[]).unwrap();
        assert!(sub.down_addrs().is_empty());
    }
}
