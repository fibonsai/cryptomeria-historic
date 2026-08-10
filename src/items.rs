//! Market-data item types that mirror `cryptomeria-ingest`'s normalised schema.
//!
//! The NNG wire payload published by `cryptomeria-marketdata` is the JSON
//! serialisation of `MarketDataItem`.  Defining the types here (instead of
//! depending on the full ingest crate) keeps the forwarder lightweight while
//! guaranteeing a serde layout that is byte-for-byte compatible.

use serde::{Deserialize, Serialize};

/// Normalised market-data item emitted by the ingest pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MarketDataItem {
    Lob(LobItem),
    Trade(TradeItem),
}

impl MarketDataItem {
    /// Exchange timestamp in milliseconds since the Unix epoch.
    pub fn timestamp_ms(&self) -> u64 {
        match self {
            MarketDataItem::Lob(l) => l.ts,
            MarketDataItem::Trade(t) => t.ts,
        }
    }
}

/// Limit-order-book snapshot or incremental update.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LobItem {
    /// Exchange timestamp in milliseconds since the Unix epoch.
    pub ts: u64,
    /// Source exchange name (e.g. `"okx"`, `"kraken"`, `"bitstamp"`).
    pub exchange: String,
    /// Bid levels, sorted descending (best bid first).
    pub bids: Vec<LobLevel>,
    /// Ask levels, sorted ascending (best ask first).
    pub asks: Vec<LobLevel>,
}

/// A single price level in the LOB.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LobLevel {
    #[serde(rename = "p")]
    pub price: f64,
    #[serde(rename = "s")]
    pub size: f64,
}

/// A single trade execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeItem {
    /// Exchange timestamp in milliseconds since the Unix epoch.
    pub ts: u64,
    /// Source exchange name.
    pub exchange: String,
    /// Trade price.
    pub price: f64,
    /// Trade size (quantity).
    pub size: f64,
    /// Trade side: `"buy"` or `"sell"`.
    pub side: String,
    /// Exchange-specific trade ID, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_id: Option<String>,
    /// Exchange-specific sequence ID, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq_id: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_lob_item() {
        let json = r#"{"lob":{"ts":123,"exchange":"okx","bids":[{"p":100.0,"s":1.0}],"asks":[{"p":101.0,"s":2.0}]}}"#;
        let item: MarketDataItem = serde_json::from_str(json).unwrap();
        match item {
            MarketDataItem::Lob(lob) => {
                assert_eq!(lob.ts, 123);
                assert_eq!(lob.exchange, "okx");
                assert_eq!(lob.bids.len(), 1);
                assert_eq!(lob.bids[0].price, 100.0);
                assert_eq!(lob.bids[0].size, 1.0);
                assert_eq!(lob.asks[0].price, 101.0);
            }
            MarketDataItem::Trade(_) => panic!("expected Lob variant"),
        }
    }

    #[test]
    fn deserializes_trade_item() {
        let json = r#"{"trade":{"ts":456,"exchange":"kraken","price":100.0,"size":1.5,"side":"buy","trade_id":"t1","seq_id":42}}"#;
        let item: MarketDataItem = serde_json::from_str(json).unwrap();
        match item {
            MarketDataItem::Trade(t) => {
                assert_eq!(t.ts, 456);
                assert_eq!(t.exchange, "kraken");
                assert_eq!(t.price, 100.0);
                assert_eq!(t.size, 1.5);
                assert_eq!(t.side, "buy");
                assert_eq!(t.trade_id, Some("t1".to_string()));
                assert_eq!(t.seq_id, Some(42));
            }
            MarketDataItem::Lob(_) => panic!("expected Trade variant"),
        }
    }

    #[test]
    fn serializes_back_to_same_schema() {
        let item = MarketDataItem::Trade(TradeItem {
            ts: 1,
            exchange: "okx".into(),
            price: 10.0,
            size: 2.0,
            side: "sell".into(),
            trade_id: None,
            seq_id: None,
        });
        let json = serde_json::to_string(&item).unwrap();
        let re: MarketDataItem = serde_json::from_str(&json).unwrap();
        assert_eq!(item, re);
    }

    #[test]
    fn timestamp_ms_works_for_both_variants() {
        let lob = MarketDataItem::Lob(LobItem {
            ts: 100,
            exchange: "x".into(),
            bids: vec![],
            asks: vec![],
        });
        let trade = MarketDataItem::Trade(TradeItem {
            ts: 200,
            exchange: "x".into(),
            price: 0.0,
            size: 0.0,
            side: "buy".into(),
            trade_id: None,
            seq_id: None,
        });
        assert_eq!(lob.timestamp_ms(), 100);
        assert_eq!(trade.timestamp_ms(), 200);
    }
}
