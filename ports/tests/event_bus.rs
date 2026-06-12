// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2026 KooshaPari <kooshapari@gmail.com>

//! 5 smoke tests for the EventBus port.

use ports::adapters::nats::NatsBus;
use ports::adapters::redis_streams::RedisStreamsBus;
use ports::event_bus::EventBus;

#[tokio::test]
async fn nats_backend() {
    assert_eq!(NatsBus.backend(), "nats");
}

#[tokio::test]
async fn redis_backend() {
    assert_eq!(RedisStreamsBus.backend(), "redis-streams");
}

#[tokio::test]
async fn nats_publish_ok() {
    assert!(NatsBus.publish("test", bytes::Bytes::new()).await.is_ok());
}

#[tokio::test]
async fn redis_publish_ok() {
    assert!(RedisStreamsBus.publish("test", bytes::Bytes::new()).await.is_ok());
}

#[tokio::test]
async fn trait_object_safe() {
    let _t: Box<dyn EventBus> = Box::new(NatsBus);
}
