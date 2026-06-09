// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2026 KooshaPari <kooshapari@gmail.com>

//! T69: PhenoEvents hexagonal port — EventBus.
//!
//! 3 adapters: NatsBus, KafkaBus, RedisStreamsBus.
//! Domain code depends on this trait, not on a specific broker.

use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use tokio_stream::Stream;

pub type Topic = &'static str;
pub type Payload = Bytes;

#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("io")] Io(#[from] std::io::Error),
    #[error("backend: {0}")] Backend(String),
    #[error("closed")] Closed,
}

#[async_trait]
pub trait EventBus: Send + Sync {
    fn backend(&self) -> &str;

    async fn publish(
        &self,
        topic: Topic,
        payload: Payload,
    ) -> Result<(), BusError>;

    async fn subscribe(
        &self,
        topic: Topic,
    ) -> Result<Arc<dyn Stream<Item = (Topic, Payload)> + Send + Unpin>, BusError>;
}

pub type DynBus = Arc<dyn EventBus>;
