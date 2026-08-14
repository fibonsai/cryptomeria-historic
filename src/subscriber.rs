//! NNG SUB socket wrapper that receives framed market-data messages.
//!
//! Subscribes to all topics (empty prefix) and filters by `lob__` / `trade__`
//! in the processing loop, mirroring the approach used by
//! `cryptomeria-marketdata/src/subscriber.rs`.

use nng::options::protocol::pubsub::Subscribe;
use nng::options::{Options, RecvTimeout};
use nng::{Error, Protocol, Socket};
use std::time::Duration;

const RECV_TIMEOUT_MS: u64 = 500;

/// Topic prefix for LOB messages.
pub const LOB_TOPIC_PREFIX: &str = "lob__";
/// Topic prefix for trade messages.
pub const TRADE_TOPIC_PREFIX: &str = "trade__";

/// A connected NNG subscriber that may dial multiple PUB brokers.
pub struct NngSubscriber {
    socket: Socket,
}

impl NngSubscriber {
    /// Connect to one or more external NNG PUB brokers (e.g.
    /// `tcp://127.0.0.1:14242`) and subscribe to all topics.
    ///
    /// A single SUB socket dials every address in `addrs`, so messages from
    /// all brokers are multiplexed onto the same socket.
    pub fn new(addrs: &[String]) -> Result<Self, Error> {
        let socket = Socket::new(Protocol::Sub0)?;
        // Empty subscription = receive all messages; filtering happens in-process.
        socket.set_opt::<Subscribe>(Vec::<u8>::new())?;
        socket.set_opt::<RecvTimeout>(Some(Duration::from_millis(RECV_TIMEOUT_MS)))?;
        for addr in addrs {
            socket.dial(addr)?;
        }
        Ok(NngSubscriber { socket })
    }

    /// Block until a message arrives or the recv-timeout elapses.
    ///
    /// Returns `Err(Error::TimedOut)` when no message arrives within the
    /// configured timeout, allowing the caller to check a shutdown flag.
    pub fn recv(&self) -> Result<nng::Message, Error> {
        self.socket.recv()
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
    fn new_with_single_addr_compiles() {
        let addr = "tcp://127.0.0.1:14242".to_string();
        let _sub = NngSubscriber::new(&[addr]).unwrap();
    }
}
