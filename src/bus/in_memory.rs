//! In-memory [`Bus`] adapter.
//!
//! Provides a non-persistent, in-process pub/sub implementation of the
//! [`Bus`] trait. Each subscriber gets its own bounded queue and a dedicated
//! worker task; publishing fans out to every live subscriber.
//!
//! Lifted from `phenotype-bus/src/events/bus.rs` and adapted to the
//! [`EventEnvelope`]-shaped contract used by `pheno-events`. Key adaptations:
//!
//! - Replaces `phenotype-bus`'s `dyn Handler` trait + `HandlerSlot` registry
//!   with `async-channel` senders keyed by subscriber id, so per-subscriber
//!   backpressure is independent.
//! - Replaces the `oneshot`-based cancel signal with a tokio worker join
//!   handle wrapped in [`Subscription`]; unsubscribing drops the worker.
//! - Tracks a per-bus `seen` set so [`Ack::duplicate`] mirrors the
//!   "INSERT OR IGNORE" semantics of [`SqliteBus`](super::SqliteBus).
//! - Threads `last_seen` (most recently published event id) through to each
//!   handler invocation, matching the contract `subscribe()` already exposes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::core::EventEnvelope;

use super::{Ack, Bus, Handler, HandlerError, PublishError, SubscribeError, Subscription};

/// Per-subscriber delivery channel capacity. Big enough to absorb short
/// bursts without back-pressuring publishers, small enough that a stuck
/// subscriber can't OOM the process.
const SUBSCRIBER_QUEUE: usize = 1024;

/// In-memory, in-process [`Bus`] implementation.
///
/// Use [`InMemoryBus::new`] for the default unbounded-by-default semantics
/// (bounded by `SUBSCRIBER_QUEUE` per subscriber). There is no durability:
/// published events are only delivered to subscribers that exist at publish
/// time.
#[derive(Clone, Default)]
pub struct InMemoryBus {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    subscribers: HashMap<Uuid, SubscriberEntry>,
    seen: HashSet<Uuid>,
    last_seen: Option<Uuid>,
}

struct SubscriberEntry {
    sender: Sender<(EventEnvelope, Option<Uuid>)>,
}

impl InMemoryBus {
    /// Create a new, empty in-memory bus.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                subscribers: HashMap::new(),
                seen: HashSet::new(),
                last_seen: None,
            })),
        }
    }

    /// Snapshot the number of currently registered subscribers.
    pub async fn subscriber_count(&self) -> usize {
        self.inner.lock().await.subscribers.len()
    }
}

#[async_trait]
impl Bus for InMemoryBus {
    async fn publish(&self, envelope: EventEnvelope) -> Result<Ack, PublishError> {
        envelope.validate()?;

        let event_id = envelope.id;

        // Capture the per-subscriber senders and the previous `last_seen`
        // under a single lock so the value we hand each worker matches the
        // event-ordering contract (worker for event B sees `last_seen == A`,
        // worker for event A sees `last_seen == None`).
        let (senders, prev_last_seen): (Vec<Sender<(EventEnvelope, Option<Uuid>)>>, Option<Uuid>) = {
            let inner = self.inner.lock().await;
            (
                inner
                    .subscribers
                    .values()
                    .map(|s| s.sender.clone())
                    .collect(),
                inner.last_seen,
            )
        };

        let duplicate = {
            let mut inner = self.inner.lock().await;
            if inner.seen.contains(&event_id) {
                true
            } else {
                inner.seen.insert(event_id);
                false
            }
        };

        for sender in senders {
            // A full queue would block; we drop the send rather than block
            // the publisher. In-memory bus has no retry/DLQ story by design.
            let _ = sender.try_send((envelope.clone(), prev_last_seen));
        }

        {
            let mut inner = self.inner.lock().await;
            inner.last_seen = Some(event_id);
        }

        Ok(Ack {
            event_id,
            duplicate,
        })
    }

    async fn subscribe(&self, handler: Handler) -> Result<Subscription, SubscribeError> {
        let (tx, rx): (
            Sender<(EventEnvelope, Option<Uuid>)>,
            Receiver<(EventEnvelope, Option<Uuid>)>,
        ) = async_channel::bounded(SUBSCRIBER_QUEUE);

        let subscriber_id = Uuid::now_v7();
        {
            let mut inner = self.inner.lock().await;
            inner
                .subscribers
                .insert(subscriber_id, SubscriberEntry { sender: tx });
        }

        let inner = Arc::clone(&self.inner);
        let worker = tokio::spawn(async move {
            run_subscriber(rx, inner, subscriber_id, handler).await;
        });

        Ok(Subscription { worker })
    }
}

async fn run_subscriber(
    rx: Receiver<(EventEnvelope, Option<Uuid>)>,
    inner: Arc<Mutex<Inner>>,
    subscriber_id: Uuid,
    handler: Handler,
) {
    while let Ok((envelope, last_seen)) = rx.recv().await {
        let event_id = envelope.id;
        match handler(envelope, last_seen).await {
            Ok(()) => {}
            Err(HandlerError(message)) => {
                pheno_otel::metrics::record_error("pheno_events.bus.handler", "handler_nack");
                tracing::warn!(%event_id, %subscriber_id, error = %message, "handler nacked; dropping event (in-memory bus has no retry)");
            }
        }
    }

    // Channel closed (all senders dropped): remove ourselves.
    let mut inner = inner.lock().await;
    inner.subscribers.remove(&subscriber_id);
}

#[cfg(test)]
mod tests {
    use super::{Bus, InMemoryBus};
    use crate::core::EventEnvelope;
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use tokio::time::{sleep, timeout, Duration};

    fn event(event_type: &str) -> EventEnvelope {
        EventEnvelope::builder(event_type, "tests", json!({"id": 1}))
            .build()
            .expect("event")
    }

    fn recording_handler(
        seen: Arc<Mutex<Vec<uuid::Uuid>>>,
        last_seen_seen: Arc<Mutex<Vec<Option<uuid::Uuid>>>>,
    ) -> crate::bus::Handler {
        Arc::new(move |event, last_seen| {
            let seen = seen.clone();
            let last_seen_seen = last_seen_seen.clone();
            Box::pin(async move {
                seen.lock().expect("seen").push(event.id);
                last_seen_seen.lock().expect("last_seen").push(last_seen);
                Ok(())
            })
        })
    }

    async fn eventually<F, Fut>(mut assertion: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        timeout(Duration::from_secs(2), async {
            loop {
                if assertion().await {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("condition met");
    }

    #[tokio::test]
    async fn publish_returns_ack() {
        let bus = InMemoryBus::new();
        let envelope = event("user.created");

        let ack = bus.publish(envelope.clone()).await.expect("publish");

        assert_eq!(ack.event_id, envelope.id);
        assert!(!ack.duplicate);

        // Re-publishing the same id should now report duplicate.
        let ack = bus.publish(envelope.clone()).await.expect("publish");
        assert_eq!(ack.event_id, envelope.id);
        assert!(ack.duplicate);
    }

    #[tokio::test]
    async fn subscribe_receives_published_event() {
        let bus = InMemoryBus::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let last_seen_seen = Arc::new(Mutex::new(Vec::new()));
        let handler = recording_handler(seen.clone(), last_seen_seen.clone());

        let _subscription = bus.subscribe(handler).await.expect("subscribe");

        let envelope = event("user.created");
        bus.publish(envelope.clone()).await.expect("publish");

        eventually(|| {
            let seen = seen.clone();
            async move { seen.lock().expect("seen").contains(&envelope.id) }
        })
        .await;
    }

    #[tokio::test]
    async fn multi_subscriber_each_receives_once() {
        let bus = InMemoryBus::new();
        let a = Arc::new(Mutex::new(Vec::new()));
        let b = Arc::new(Mutex::new(Vec::new()));
        let handler_a = recording_handler(a.clone(), Arc::new(Mutex::new(Vec::new())));
        let handler_b = recording_handler(b.clone(), Arc::new(Mutex::new(Vec::new())));

        let _sub_a = bus.subscribe(handler_a).await.expect("sub a");
        let _sub_b = bus.subscribe(handler_b).await.expect("sub b");

        let envelope = event("user.created");
        bus.publish(envelope.clone()).await.expect("publish");

        eventually(|| {
            let a = a.clone();
            let b = b.clone();
            async move {
                a.lock().expect("a").contains(&envelope.id)
                    && b.lock().expect("b").contains(&envelope.id)
            }
        })
        .await;

        assert_eq!(a.lock().expect("a").len(), 1);
        assert_eq!(b.lock().expect("b").len(), 1);
    }

    #[tokio::test]
    async fn unsubscribe_stops_delivery() {
        let bus = InMemoryBus::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let handler_counter = counter.clone();
        let handler: crate::bus::Handler = Arc::new(move |_event, _last_seen| {
            let handler_counter = handler_counter.clone();
            Box::pin(async move {
                handler_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });

        let subscription = bus.subscribe(handler).await.expect("subscribe");

        bus.publish(event("user.created")).await.expect("publish");
        eventually(|| async { counter.load(Ordering::SeqCst) == 1 }).await;

        // Dropping the subscription aborts the worker; further publishes
        // should not increment the counter.
        drop(subscription);
        sleep(Duration::from_millis(50)).await;

        bus.publish(event("user.deleted")).await.expect("publish");
        sleep(Duration::from_millis(50)).await;

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn last_seen_passed_to_handler() {
        let bus = InMemoryBus::new();
        let last_seen_seen: Arc<Mutex<Vec<Option<uuid::Uuid>>>> = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler = recording_handler(seen.clone(), last_seen_seen.clone());

        let _subscription = bus.subscribe(handler).await.expect("subscribe");

        let first = event("user.created");
        let second = event("user.deleted");

        bus.publish(first.clone()).await.expect("publish first");
        bus.publish(second.clone()).await.expect("publish second");

        eventually(|| {
            let last_seen_seen = last_seen_seen.clone();
            async move { last_seen_seen.lock().expect("lss").len() == 2 }
        })
        .await;

        let values = last_seen_seen.lock().expect("lss");
        assert_eq!(values[0], None, "first event sees no last_seen");
        assert_eq!(
            values[1],
            Some(first.id),
            "second event sees first event's id as last_seen"
        );
    }

    #[tokio::test]
    async fn handler_nack_does_not_drop_event() {
        // The handler returning Err must not cause the event to be re-published
        // or otherwise disappear from the bus's bookkeeping.
        let bus = InMemoryBus::new();
        let calls = Arc::new(AtomicUsize::new(0));

        let handler_calls = calls.clone();
        let handler: crate::bus::Handler = Arc::new(move |_event, _last_seen| {
            let handler_calls = handler_calls.clone();
            Box::pin(async move {
                handler_calls.fetch_add(1, Ordering::SeqCst);
                Err(crate::bus::HandlerError("transient failure".into()))
            })
        });

        let _subscription = bus.subscribe(handler).await.expect("subscribe");

        let envelope = event("user.created");
        let ack = bus.publish(envelope.clone()).await.expect("publish");

        // The bus still reports a fresh ack (no duplicate).
        assert!(!ack.duplicate);
        assert_eq!(ack.event_id, envelope.id);

        // The handler was invoked exactly once.
        eventually(|| async { calls.load(Ordering::SeqCst) == 1 }).await;
        sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Subscriber registry still contains exactly one entry.
        assert_eq!(bus.subscriber_count().await, 1);
    }
}
