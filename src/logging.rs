//! Thin `env_logger` initializer for cryptomeria-historic.
//!
//! Wraps `env_logger` so that `main.rs` can call `logging::init()` and modules
//! log through the standard `log` facade macros (`log::info!`, `log::warn!`,
//! `log::error!`, `log::debug!`) with the category embedded in the format
//! string, e.g. `log::info!("message")`.

pub fn init() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
}
