//! Pure helpers for splitting and framing NNG wire messages.
//!
//! The wire format is a single NNG message composed of a UTF-8 topic string
//! followed by the `FRAME_SEPARATOR` byte (`\0`) and then an opaque payload.
//! The payload deserialisation is the consumer's responsibility — this crate
//! stays payload-agnostic so it can serve any topic / payload convention.

/// Byte used as the separator between the topic and the payload.
pub const FRAME_SEPARATOR: u8 = b'\0';

/// Split a framed NNG wire message into `(topic, payload_bytes)`.
///
/// Returns `None` when the frame separator is missing.
pub fn split_frame(bytes: &[u8]) -> Option<(String, &[u8])> {
    let idx = bytes.iter().position(|&b| b == FRAME_SEPARATOR)?;
    let topic = String::from_utf8_lossy(&bytes[..idx]).into_owned();
    Some((topic, &bytes[idx + 1..]))
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
}
