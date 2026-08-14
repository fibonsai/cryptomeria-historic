//! Pure helpers for parsing NNG wire frames.
//!
//! The wire format — dictated by `cryptomeria-marketdata` — is a single NNG
//! message composed of `{topic} ␀ payload` where `topic` is a UTF-8 string
//! (`{kind}__{instrument}`, e.g. `lob__btcusdt`) and `payload` is the JSON
//! serialisation of a [`MarketDataItem`](crate::items::MarketDataItem).

use crate::items::MarketDataItem;
use anyhow::{Result, anyhow};

/// Byte used as the separator between the topic and the JSON payload.
pub const FRAME_SEPARATOR: u8 = b'\0';

/// Split a framed NNG wire message into `(topic, payload_bytes)`.
///
/// Returns `None` when the frame separator is missing.
pub fn split_frame(bytes: &[u8]) -> Option<(String, &[u8])> {
    let idx = bytes.iter().position(|&b| b == FRAME_SEPARATOR)?;
    let topic = String::from_utf8_lossy(&bytes[..idx]).into_owned();
    Some((topic, &bytes[idx + 1..]))
}

/// Extract the `(kind, instrument)` pair from a topic string of the form
/// `{kind}__{instrument}`.
///
/// `kind` is `"lob"` or `"trade"`.  Returns `None` when the topic does not
/// contain the `__` delimiter.
pub fn extract_topic_segment(topic: &str) -> Option<(&str, &str)> {
    let idx = topic.find("__")?;
    let kind = &topic[..idx];
    let instrument = &topic[idx + 2..];
    if kind.is_empty() || instrument.is_empty() {
        return None;
    }
    Some((kind, instrument))
}

/// Parse a full wire frame: split it into topic + payload, then deserialise
/// the payload into a [`MarketDataItem`].
pub fn parse_frame(bytes: &[u8]) -> Result<(String, MarketDataItem)> {
    let (topic, payload_bytes) =
        split_frame(bytes).ok_or_else(|| anyhow!("message frame has no separator"))?;
    let item: MarketDataItem = serde_json::from_slice(payload_bytes)?;
    Ok((topic, item))
}

/// Frame a topic and payload into the wire bytes sent over NNG: `topic ␀ payload`.
/// The topic stays a prefix so NNG SUB topic filtering works while the payload
/// can be split back out by the subscriber.
pub fn frame_message(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(topic.len() + 1 + payload.len());
    bytes.extend_from_slice(topic.as_bytes());
    bytes.push(FRAME_SEPARATOR);
    bytes.extend_from_slice(payload);
    bytes
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
    fn extract_topic_segment_lob() {
        assert_eq!(
            extract_topic_segment("lob__btcusdt"),
            Some(("lob", "btcusdt"))
        );
    }

    #[test]
    fn extract_topic_segment_trade() {
        assert_eq!(
            extract_topic_segment("trade__btcusd"),
            Some(("trade", "btcusd"))
        );
    }

    #[test]
    fn extract_topic_segment_returns_none_without_delimiter() {
        assert_eq!(extract_topic_segment("just-a-topic"), None);
    }

    #[test]
    fn extract_topic_segment_returns_none_for_empty_instrument() {
        assert_eq!(extract_topic_segment("lob__"), None);
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
