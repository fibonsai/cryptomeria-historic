//! # cryptomeria-nng-client
//!
//! Generic, reusable NNG PUB/SUB subscriber client.
//!
//! Connects to one or more external NNG PUB sockets and emits structured
//! `BrokerOutput` events (`Message`, `Down`, `Up`) on a shared channel.
//!
//! Each configured broker gets its own `nng::Sub0` socket on its own
//! `tokio::task::spawn_blocking` task, so a disconnect on broker B is
//! attributable to B alone — even while sibling brokers keep delivering data
//! (see ADR-008).
//!
//! ## Wire format
//!
//! Messages are a single NNG message: `{topic} ␀ payload` where `topic` is a
//! UTF-8 string and `payload` is an opaque byte slice. The payload deserialisation
//! and topic classification are the consumer's responsibility — this crate stays
//! payload-agnostic so it can serve any topic / payload convention.

pub mod forward;
pub mod subscriber;

pub use forward::{FRAME_SEPARATOR, frame_message, split_frame};
pub use subscriber::{BrokerOutput, BrokerReader, ConnEvent, NngSubscriber, connectivity_event};
