//! phenoEvents observability — thin re-export of `pheno-tracing`.
//!
//! pheno-tracing is the fleet-canonical OTLP tracer (ADR-012). This crate
//! exists so the rest of the workspace depends on a local crate name
//! (`phenoevents-observability`) instead of a git tag, which makes
//! version bumps a single PR.
//!
//! ## Quickstart
//!
//! ```rust,no_run
//! use phenoevents_observability::prelude::*;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     init_tracing("pheno-events", "http://localhost:4317")?;
//!     info!(component = "pheno-events", "starting");
//!     Ok(())
//! }
//! ```

pub use pheno_tracing::*;

/// Convenience prelude — everything you need for OTLP-observed apps.
pub mod prelude {
    pub use pheno_tracing::{
        error, info, instrument, span, warn, Counter, Histogram, OtlpEndpoint, RequestMetrics,
        ServiceName, Span, SpanGuard, TracePort, TraceResult,
    };

    /// Initialize OTLP tracing with sane defaults. Safe to call from `main`.
    ///
    /// Reads `OTEL_EXPORTER_OTLP_ENDPOINT` from the environment if `endpoint`
    /// is `None`.
    pub fn init_tracing(
        service_name: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Result<(), pheno_tracing::TracingError> {
        pheno_tracing::init(service_name, endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::prelude::*;

    #[test]
    fn trace_port_roundtrip() {
        let name: ServiceName = "pheno-events-test".into();
        let endpoint: OtlpEndpoint = "http://localhost:4317".into();
        let port = TracePort::new(name.clone(), endpoint.clone());
        assert_eq!(port.service_name(), &name);
        assert_eq!(port.endpoint(), &endpoint);
        assert!(!port.is_sampled());
    }

    #[test]
    fn request_metrics_construct() {
        let metrics = RequestMetrics::new("pheno-events");
        let counter = metrics.requests_total();
        let histogram = metrics.request_duration_seconds();
        counter.inc();
        histogram.observe(0.123);
        assert_eq!(counter.value(), 1);
    }

    #[test]
    fn span_guard_creates_and_closes() {
        let guard = SpanGuard::new("test-op");
        assert_eq!(guard.name(), "test-op");
        drop(guard);
    }
}
