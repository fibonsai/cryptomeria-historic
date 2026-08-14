//! cryptomeria-historic — NNG PUB/SUB subscriber that forwards normalised LOB/trade market
//! data to QuestDB with embedded schema migrations.
//!
//! Connects to an external NNG PUB socket (run by `cryptomeria-marketdata`),
//! receives framed `{topic}\0{json-payload}` messages, deserialises them into
//! normalised `MarketDataItem` values, and writes LOB levels / trades into
//! QuestDB via QWP/WebSocket (QuestDB 10+) with embedded schema-versioned
//! migrations.
//!
//! NNG subscriber / wire-format / items logic lives in the
//! `cryptomeria-nng-client` crate and is re-exported here for compatibility.

pub mod db;
pub mod logging;
pub mod migrate;

pub use cryptomeria_nng_client::forward::{
    FRAME_SEPARATOR, extract_topic_segment, frame_message, parse_frame, split_frame,
};
pub use cryptomeria_nng_client::items::{LobItem, LobLevel, MarketDataItem, TradeItem};
pub use cryptomeria_nng_client::subscriber::{
    BrokerOutput, BrokerReader, ConnEvent, LOB_TOPIC_PREFIX, NngSubscriber, TRADE_TOPIC_PREFIX,
    classify_topic, connectivity_event,
};

pub use db::QuestDb;
