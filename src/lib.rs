//! cryptomeria-historic — NNG PUB/SUB subscriber that forwards normalised LOB/trade market
//! data to QuestDB with embedded schema migrations.
//!
//! Connects to an external NNG PUB socket (run by `cryptomeria-marketdata`),
//! receives framed `{topic}\0{json-payload}` messages, deserialises them into
//! normalised `MarketDataItem` values, and writes LOB levels / trades into
//! QuestDB via QWP/WebSocket (QuestDB 10+) with embedded schema-versioned
//! migrations.
//!
//! Domain-specific types (items, topic classification, payload parsing) live in
//! local modules; generic NNG transport (sockets, connectivity tracking, frame
//! splitting) comes from the `cryptomeria-nng-client` crate.

pub mod db;
pub mod forward;
pub mod items;
pub mod logging;
pub mod migrate;
pub mod topics;

// Re-export generic NNG transport types from the client crate.
pub use cryptomeria_nng_client::subscriber::{
    BrokerOutput, BrokerReader, ConnEvent, NngSubscriber, connectivity_event,
};

// Re-export domain-specific types from local modules.
pub use forward::{FRAME_SEPARATOR, frame_message, parse_frame, split_frame};
pub use items::{LobItem, LobLevel, MarketDataItem, TradeItem};
pub use topics::{LOB_TOPIC_PREFIX, TRADE_TOPIC_PREFIX, classify_topic, extract_topic_segment};

pub use db::QuestDb;
