//! Rasant-based global logger for cryptomeria-historic.
//!
//! Following the same pattern used in `cryptomeria-ingest` (`src/logger.rs`),
//! a single process-wide [`Logger`] is initialised once at startup and exposed
//! through free functions so that modules like `migrate` and `db` can log
//! without threading a `Logger` instance through every signature.

use rasant::{Level, Logger};
use std::sync::{Mutex, OnceLock};

static LOGGER: OnceLock<Mutex<Logger>> = OnceLock::new();

/// Initialise the global logger.
///
/// Reads the log level from the `RUST_LOG` environment variable (accepting the
/// rasant level names plus the common `warn` alias) and attaches a stdout sink.
/// Must be called before any [`info`] / [`warn`] / [`error`] calls.
pub fn init() -> Logger {
    let level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|s| parse_level(&s))
        .unwrap_or(Level::Info);

    let mut logger = Logger::new();
    logger.set_level(level);
    logger.add_sink(rasant::sink::stdout::default());
    LOGGER.get_or_init(|| Mutex::new(logger.clone()));
    logger
}

/// Return a cheap clone of the global [`Logger`].
pub fn logger() -> Logger {
    LOGGER
        .get_or_init(|| {
            let mut log = Logger::new();
            log.add_sink(rasant::sink::black_hole::default());
            log.set_level(Level::Info);
            Mutex::new(log)
        })
        .lock()
        .expect("logger mutex poisoned")
        .clone()
}

fn parse_level(s: &str) -> Option<Level> {
    if s.eq_ignore_ascii_case("warn") {
        return Some(Level::Warning);
    }
    Level::try_from(s.trim()).ok()
}

/// Log an informational message under the given category.
pub fn info(category: &str, msg: &str) {
    let mut log = logger();
    rasant::info!(log, &format!("[{category}]: {msg}"));
}

/// Log a warning message under the given category.
pub fn warn(category: &str, msg: &str) {
    let mut log = logger();
    rasant::warn!(log, &format!("[{category}]: {msg}"));
}

/// Log an error message under the given category.
pub fn error(category: &str, msg: &str) {
    let mut log = logger();
    rasant::error!(log, &format!("[{category}]: {msg}"));
}

/// Log a debug message under the given category.
pub fn debug(category: &str, msg: &str) {
    let mut log = logger();
    rasant::debug!(log, &format!("[{category}]: {msg}"));
}
