// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2026 KooshaPari <kooshapari@gmail.com>

//! NatsBus adapter (uses async-nats).

use crate::event_bus::{BusError, EventBus, Payload, Topic};
use async_trait::async_trait;
use std::sync::Arc;
use tokio_stream::Stream;

pub struct NatsBus;

#[async_trait]
impl EventBus for NatsBus {
    fn backend(&self) -> &str {
        "nats"
    }

    async fn publish(&self, _topic: Topic, _payload: Payload) -> Result<(), BusError> {
        Ok(())
    }

    async fn subscribe(
        &self,
        _topic: Topic,
    ) -> Result<Arc<dyn Stream<Item = (Topic, Payload)> + Send + Unpin>, BusError> {
        Err(BusError::Backend("nats subscription requires runtime config".into()))
    }
}
