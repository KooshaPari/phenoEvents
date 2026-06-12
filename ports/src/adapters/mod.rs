//! Adapter implementations of the [`crate::event_bus::EventBus`] port.
//!
//! - [`nats`]          — NatsBus (async-nats)
//! - [`redis_streams`] — RedisStreamsBus (redis-rs with streams)

pub mod nats;
pub mod redis_streams;
