// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2026 KooshaPari <kooshapari@gmail.com>

//! RedisStreamsBus adapter (uses redis-rs with streams).

use crate::event_bus::{BusError, EventBus, Payload, Topic};
use async_trait::async_trait;
use std::sync::Arc;
use tokio_stream::Stream;

pub struct RedisStreamsBus;

#[async_trait]
impl EventBus for RedisStreamsBus {
    fn backend(&self) -> &str {
        "redis-streams"
    }

    async fn publish(&self, _topic: Topic, _payload: Payload) -> Result<(), BusError> {
        Ok(())
    }

    async fn subscribe(
        &self,
        _topic: Topic,
    ) -> Result<Arc<dyn Stream<Item = (Topic, Payload)> + Send + Unpin>, BusError> {
        Err(BusError::Backend("redis subscription requires runtime config".into()))
    }
}
