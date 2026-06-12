//! T69: PhenoEvents hexagonal port — EventBus.
//!
//! Adapters live under [`adapters`]. Domain code depends on the
//! [`event_bus::EventBus`] trait, not on any specific broker
//! (NATS / Kafka / Redis Streams).
//!
//! SOTA pattern: the port trait is declared upfront; adapters are
//! implemented against it. Dead-code warnings on the adapters are
//! expected until the application crate starts using them.

pub mod adapters;
pub mod event_bus;
