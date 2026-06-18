//! Typed async pub/sub for Phenotype collections.
//!
//! Provides an [`EventEnvelope`](crate::core::EventEnvelope)-shaped contract
//! with two interchangeable bus implementations:
//!
//! - [`bus::SqliteBus`] — durable, at-least-once delivery with idempotency,
//!   outbox, retries, and DLQ.
//! - [`bus::InMemoryBus`] — non-persistent, in-process pub/sub for tests
//!   and short-lived workers.

pub mod bus;
pub mod core;
pub mod observability;
pub mod projection;
pub mod schema;

/// Crate version, sourced from `Cargo.toml` at compile time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Crate name, sourced from `Cargo.toml` at compile time.
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Return the crate version as a static string.
///
/// This is a publish-ready helper for downstream consumers that want to pin
/// a known-good `pheno-events` version in their lockfiles.
pub fn version() -> &'static str {
    VERSION
}

/// Return the crate name as a static string.
pub fn name() -> &'static str {
    NAME
}
