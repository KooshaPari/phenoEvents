use crate::core::EventEnvelope;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use tracing::{span, Level, Span};

static EVENTS_PUBLISHED: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static EVENTS_PROCESSED: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static EVENTS_FAILED: OnceLock<Arc<AtomicU64>> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct Counter {
    value: Arc<AtomicU64>,
}

impl Counter {
    fn new(value: &'static OnceLock<Arc<AtomicU64>>) -> Self {
        Self {
            value: value.get_or_init(|| Arc::new(AtomicU64::new(0))).clone(),
        }
    }

    pub fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

pub fn trace_envelope(envelope: &EventEnvelope) -> Span {
    let correlation_id = envelope
        .correlation_id
        .map(|id| id.to_string())
        .unwrap_or_default();

    span!(
        Level::INFO,
        "event",
        event.id = %envelope.id,
        event.type = %envelope.event_type,
        source = %envelope.source,
        correlation_id = %correlation_id
    )
}

pub fn metrics() -> (Counter, Counter, Counter) {
    (
        Counter::new(&EVENTS_PUBLISHED),
        Counter::new(&EVENTS_PROCESSED),
        Counter::new(&EVENTS_FAILED),
    )
}

#[cfg(test)]
mod tests {
    use super::{metrics, trace_envelope};
    use crate::core::EventEnvelope;
    use serde_json::json;

    #[test]
    fn trace_span_uses_event_name() {
        let envelope = EventEnvelope::builder("user.created", "tests", json!({}))
            .build()
            .expect("event");
        let span = trace_envelope(&envelope);

        assert_eq!(span.metadata().expect("metadata").name(), "event");
    }

    #[test]
    fn counters_increment() {
        let (published, processed, failed) = metrics();
        published.reset();
        processed.reset();
        failed.reset();

        published.increment();
        processed.increment();
        processed.increment();
        failed.increment();

        assert_eq!(published.get(), 1);
        assert_eq!(processed.get(), 2);
        assert_eq!(failed.get(), 1);
    }
}
