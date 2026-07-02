use crate::core::EventEnvelope;
use phenoevents_observability::prelude::{info, instrument};
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
/// configured OTLP collector via `pheno-tracing` (canonical per ADR-012).
pub fn init_tracing() {
    TRACING_INIT.get_or_init(|| {
        use tracing_subscriber::{fmt, prelude::*, EnvFilter};
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,pheno_events=debug,sqlx=warn"));
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_target(false))
            .try_init();

        // OTLP export (best-effort — does not block startup if collector is down)
        if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            let _ = phenoevents_observability::init_tracing("pheno-events", &endpoint);
            info!(endpoint = %endpoint, "pheno-events OTLP tracing initialised");
        }
    });
}

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
    use super::{init_tracing, metrics, trace_envelope};
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
