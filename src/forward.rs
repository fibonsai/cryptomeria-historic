//! NNG wire-frame parsing and framing for cryptomeria-marketdata.
//!
//! The wire format — dictated by `cryptomeria-marketdata` — is a single NNG
//! message composed of `{topic} ␀ payload` where `topic` is a UTF-8 string
//! (`{kind}__{instrument}`, e.g. `lob__btcusdt`) and `payload` is the JSON
//! serialisation of a [`MarketDataItem`].

use crate::items::MarketDataItem;
use anyhow::{Result, anyhow};

pub use cryptomeria_nng_client::forward::{FRAME_SEPARATOR, frame_message, split_frame};

/// Parse a full wire frame: split it into topic + payload, then deserialise
/// the payload into a [`MarketDataItem`].
pub fn parse_frame(bytes: &[u8]) -> Result<(String, MarketDataItem)> {
    let (topic, payload_bytes) =
        split_frame(bytes).ok_or_else(|| anyhow!("message frame has no separator"))?;
    let item: MarketDataItem = serde_json::from_slice(payload_bytes)?;
    Ok((topic, item))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_frame_back_into_topic_and_payload() {
        let bytes = frame_message("trade__btcusdt", b"{\"price\":1.0}");
        let (topic, payload) = split_frame(&bytes).unwrap();
        assert_eq!(topic, "trade__btcusdt");
        assert_eq!(payload, b"{\"price\":1.0}");
    }

    #[test]
    fn split_frame_returns_none_without_separator() {
        assert!(split_frame(b"no separator here").is_none());
    }

    #[test]
    fn splits_frame_with_empty_payload() {
        let bytes = frame_message("lob__btcusdt", b"");
        let (topic, payload) = split_frame(&bytes).unwrap();
        assert_eq!(topic, "lob__btcusdt");
        assert!(payload.is_empty());
    }

    #[test]
    fn parse_frame_extracts_lob_item() {
        let json = r#"{"lob":{"ts":123,"exchange":"okx","bids":[{"p":100.0,"s":1.0}],"asks":[{"p":101.0,"s":2.0}]}}"#;
        let bytes = frame_message("lob__btcusdt", json.as_bytes());
        let (topic, item) = parse_frame(&bytes).unwrap();
        assert_eq!(topic, "lob__btcusdt");
        assert!(matches!(item, MarketDataItem::Lob(_)));
    }

    #[test]
    fn parse_frame_extracts_trade_item() {
        let json =
            r#"{"trade":{"ts":456,"exchange":"kraken","price":100.0,"size":1.5,"side":"buy"}}"#;
        let bytes = frame_message("trade__btcusd", json.as_bytes());
        let (topic, item) = parse_frame(&bytes).unwrap();
        assert_eq!(topic, "trade__btcusd");
        assert!(matches!(item, MarketDataItem::Trade(_)));
    }

    #[test]
    fn parse_frame_errors_on_no_separator() {
        assert!(parse_frame(b"no separator").is_err());
    }
}
