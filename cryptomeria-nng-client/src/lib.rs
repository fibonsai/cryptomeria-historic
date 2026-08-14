//! # cryptomeria-nng-client
//!
//! Reusable NNG PUB/SUB subscriber client for normalised market-data feeds.
//!
//! Connects to one or more external NNG PUB sockets (`cryptomeria-marketdata`),
//! receives framed `{topic}\0{json-payload}` messages, and emits structured
//! `BrokerOutput` events (`Message`, `Down`, `Up`) on a shared channel.
//!
//! Each configured broker gets its own `nng::Sub0` socket on its own
//! `tokio::task::spawn_blocking` task, so a disconnect on broker B is
//! attributable to B alone — even while sibling brokers keep delivering data
//! (see ADR-008).
//!
//! ## Wire format
//!
//! Messages published by `cryptomeria-marketdata` are a single NNG message:
//! `{topic} ␀ payload` where `topic` is a UTF-8 string (`{kind}__{instrument}`,
//! e.g. `lob__btcusdt`) and `payload` is the JSON serialisation of a
//! [`MarketDataItem`].

pub mod forward;
pub mod items;
pub mod subscriber;

pub use forward::{frame_message, parse_frame, split_frame};
pub use items::{LobItem, LobLevel, MarketDataItem, TradeItem};
pub use subscriber::{
    BrokerOutput, BrokerReader, ConnEvent, NngSubscriber, classify_topic, connectivity_event,
};

pub use subscriber::{LOB_TOPIC_PREFIX, TRADE_TOPIC_PREFIX};
