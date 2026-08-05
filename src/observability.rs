use crate::core::EventEnvelope;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use tracing::{span, Level, Span};

static TRACING_INIT: OnceLock<()> = OnceLock::new();

/// Initialize global tracing subscriber with `RUST_LOG`-style `EnvFilter`.
///
/// Safe to call multiple times — only the first call has any effect. The
/// default filter is `info,pheno_events=debug,sqlx=warn` so the bus and
/// projections are visible by default while noisy sqlx spans are muted.
///
/// If `OTEL_EXPORTER_OTLP_ENDPOINT` is set, also exports spans to the
/// configured OTLP collector through `phenoevents-observability`.
pub fn init_tracing() {
    TRACING_INIT.get_or_init(|| {
        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();
        let _ = phenoevents_observability::init_tracing("pheno-events", endpoint.as_deref());
    });
}

static EVENTS_PUBLISHED: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static EVENTS_PROCESSED: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static EVENTS_FAILED: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static EVENTS_RETRIED: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static QUEUE_DEPTH: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static DLQ_DEPTH: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static OLDEST_EVENT_AGE_MS: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static WORKER_ERRORS: OnceLock<Arc<AtomicU64>> = OnceLock::new();
static SQLITE_BUSY_RETRIES: OnceLock<Arc<AtomicU64>> = OnceLock::new();

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

#[derive(Debug, Clone)]
pub struct Gauge {
    value: Arc<AtomicU64>,
}

impl Gauge {
    fn new(value: &'static OnceLock<Arc<AtomicU64>>) -> Self {
        Self {
            value: value.get_or_init(|| Arc::new(AtomicU64::new(0))).clone(),
        }
    }

    pub fn set(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
    }

    pub fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_by(&self, amount: u64) {
        let _ = self
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(amount))
            });
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
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

pub fn queue_metrics() -> (Gauge, Gauge) {
    (Gauge::new(&QUEUE_DEPTH), Gauge::new(&DLQ_DEPTH))
}

pub fn retry_metrics() -> (Counter, Gauge) {
    (
        Counter::new(&EVENTS_RETRIED),
        Gauge::new(&OLDEST_EVENT_AGE_MS),
    )
}

/// Count worker-side storage, lock, and decode errors that caused a poll to
/// back off. This is separate from handler delivery failures.
pub fn worker_errors() -> Counter {
    Counter::new(&WORKER_ERRORS)
}

/// Count bounded retries taken after SQLite reports transient contention.
pub fn sqlite_busy_retries() -> Counter {
    Counter::new(&SQLITE_BUSY_RETRIES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub published: u64,
    pub processed: u64,
    pub failed: u64,
    pub retried: u64,
    pub queue_depth: u64,
    pub dlq_depth: u64,
    pub oldest_event_age_ms: u64,
}

impl MetricsSnapshot {
    pub fn emit(&self, mut record: impl FnMut(&'static str, u64)) {
        record("phenotype_event_published_total", self.published);
        record("phenotype_event_processed_total", self.processed);
        record("phenotype_event_failed_total", self.failed);
        record("phenotype_event_retried_total", self.retried);
        record("phenotype_event_queue_depth", self.queue_depth);
        record("phenotype_event_dlq_depth", self.dlq_depth);
        record("phenotype_event_oldest_age_ms", self.oldest_event_age_ms);
    }
}

pub fn snapshot() -> MetricsSnapshot {
    let (published, processed, failed) = metrics();
    let (queue_depth, dlq_depth) = queue_metrics();
    let (retried, oldest_event_age) = retry_metrics();
    MetricsSnapshot {
        published: published.get(),
        processed: processed.get(),
        failed: failed.get(),
        retried: retried.get(),
        queue_depth: queue_depth.get(),
        dlq_depth: dlq_depth.get(),
        oldest_event_age_ms: oldest_event_age.get(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        init_tracing, metrics, queue_metrics, retry_metrics, snapshot, sqlite_busy_retries,
        trace_envelope, worker_errors, MetricsSnapshot,
    };
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

    #[test]
    fn queue_gauges_track_latest_snapshot() {
        let (queue_depth, dlq_depth) = queue_metrics();
        queue_depth.set(12);
        dlq_depth.set(3);
        assert_eq!(queue_depth.get(), 12);
        assert_eq!(dlq_depth.get(), 3);
    }

    #[test]
    fn snapshot_contains_exportable_signal_values() {
        let (retried, oldest_age) = retry_metrics();
        retried.reset();
        retried.increment();
        oldest_age.set(42);
        let current = snapshot();
        assert_eq!(current.retried, 1);
        assert_eq!(current.oldest_event_age_ms, 42);
    }

    #[test]
    fn worker_errors_are_counted_separately() {
        let errors = worker_errors();
        errors.reset();
        errors.increment();
        errors.increment();
        assert_eq!(errors.get(), 2);
    }

    #[test]
    fn sqlite_busy_retries_are_counted_separately() {
        let retries = sqlite_busy_retries();
        let worker_error_count = worker_errors().get();
        retries.reset();
        retries.increment();
        assert_eq!(retries.get(), 1);
        assert_eq!(worker_errors().get(), worker_error_count);
    }

    #[test]
    fn snapshot_emits_all_stable_series() {
        let snapshot = MetricsSnapshot {
            published: 1,
            processed: 2,
            failed: 3,
            retried: 4,
            queue_depth: 5,
            dlq_depth: 6,
            oldest_event_age_ms: 7,
        };
        let mut emitted = Vec::new();
        snapshot.emit(|name, value| emitted.push((name, value)));
        assert_eq!(emitted.len(), 7);
        assert_eq!(emitted[0], ("phenotype_event_published_total", 1));
        assert_eq!(emitted[6], ("phenotype_event_oldest_age_ms", 7));
    }

    #[test]
    fn init_tracing_is_idempotent() {
        // Multiple calls must not panic from "global subscriber already set".
        init_tracing();
        init_tracing();
        init_tracing();
    }

    #[test]
    fn init_tracing_accepts_custom_env_filter() {
        // Calling with a different RUST_LOG value still must not panic on a
        // second invocation; verifies the OnceLock path tolerates redialing.
        std::env::set_var("RUST_LOG", "warn");
        init_tracing();
        std::env::remove_var("RUST_LOG");
    }
}
