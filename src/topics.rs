//! Topic classification for the cryptomeria-marketdata wire format.
//!
//! Topics follow the convention `{kind}__{instrument}` where `kind` is
//! `"lob"` or `"trade"` and `instrument` is an exchange symbol (e.g. `btcusdt`).

/// Topic prefix for LOB messages.
pub const LOB_TOPIC_PREFIX: &str = "lob__";
/// Topic prefix for trade messages.
pub const TRADE_TOPIC_PREFIX: &str = "trade__";

/// Extract the `(kind, instrument)` pair from a topic string of the form
/// `{kind}__{instrument}`.
///
/// `kind` is any non-empty prefix before `__`.  Returns `None` when the topic
/// does not contain the `__` delimiter or either segment is empty.
pub fn extract_topic_segment(topic: &str) -> Option<(&str, &str)> {
    let idx = topic.find("__")?;
    let kind = &topic[..idx];
    let instrument = &topic[idx + 2..];
    if kind.is_empty() || instrument.is_empty() {
        return None;
    }
    Some((kind, instrument))
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
}
