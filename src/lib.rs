//! cryptomeria-historic — NNG PUB/SUB subscriber forwarding market data to QuestDB.
//!
//! Connects to an external NNG PUB socket (run by `cryptomeria-marketdata`),
//! receives framed `{topic}\0{json-payload}` messages, deserialises them into
//! normalised `MarketDataItem` values, and writes LOB levels / trades into
//! QuestDB via QWP/WebSocket (QuestDB 10+) with embedded schema-versioned
//! migrations.

pub mod db;
pub mod forward;
pub mod items;
pub mod logging;
pub mod migrate;
pub mod subscriber;

pub use db::QuestDb;
pub use forward::{frame_message, parse_frame, split_frame};
pub use items::{LobItem, LobLevel, MarketDataItem, TradeItem};
pub use subscriber::{NngSubscriber, classify_topic};
