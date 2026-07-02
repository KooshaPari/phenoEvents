use crate::core::EventEnvelope;
use pheno_tracing::compat::{span, Level, Span};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};

static TRACING_INIT: OnceLock<()> = OnceLock::new();

/// Initialize global tracing subscriber with `RUST_LOG`-style `EnvFilter`.
///
/// Safe to call multiple times — only the first call has any effect. The
/// default filter is `info,pheno_events=debug,sqlx=warn` so the bus and
/// projections are visible by default while noisy sqlx spans are muted.
///
/// # OTLP export (ADR-036B / ADR-037)
///
/// `pheno-events` adopts the canonical `pheno-tracing` port contract
/// (ADR-036) and the `pheno-otel` OTLP exporter (ADR-037). To wire
/// OTLP/HTTP export in a downstream binary, install a
/// [`pheno_otel::exporters::http::HttpExporter`] (or `StdoutExporter` for
/// local dev) and bridge it into the `tracing-subscriber` registry via
/// `tracing_opentelemetry::layer()`. See `examples/otel_quickstart.rs` for
/// a complete reference wiring. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set,
/// downstream binaries should attach the OTLP layer; this crate does not
/// register it automatically because the OpenTelemetry SDK selection is a
/// binary-level decision.
pub fn init_tracing() {
    TRACING_INIT.get_or_init(|| {
        use tracing_subscriber::{fmt, prelude::*, EnvFilter};
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,pheno_events=debug,sqlx=warn"));
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_target(false))
            .try_init();
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

    #[test]
    fn pheno_tracing_compat_macros_resolve() {
        // Smoke test: the `pheno_tracing::compat` re-exports used in this
        // module resolve and behave like the upstream `tracing` macros.
        // This guards against an upstream `tracing` 0.2 bump silently
        // breaking the in-crate span helpers (per ADR-036 forward-compat
        // shim contract).
        use pheno_tracing::compat::{info, span};
        let g = span!(Level::INFO, "compat-smoke");
        info!(parent: &g, "pheno-events compat smoke");
    }

    #[test]
    fn pheno_otel_http_exporter_smoke() {
        // Smoke test: the `pheno-otel` HttpExporter is constructible from
        // an ExporterConfig and reports the right `name()` and endpoint
        // back through the OtlpPort trait. This proves the OTLP wire-format
        // adapter boundary is reachable from `pheno-events` without
        // requiring a live collector.
        use pheno_otel::exporters::ExporterConfig;
        use pheno_otel::exporters::http::HttpExporter;
        use pheno_otel::OtlpPort;
        let cfg = ExporterConfig::new("http://localhost:4318", "pheno-events");
        let exp = HttpExporter::traces(cfg);
        assert_eq!(exp.name(), "http");
        assert_eq!(exp.target_url(), "http://localhost:4318/v1/traces");
        assert!(exp.health().is_ok());
    }
}
