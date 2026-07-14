//! Canonical tracing and OTLP initialization for phenoEvents.
//!
//! This crate owns the observability integration used by the event bus. It
//! configures structured logs by default and adds OTLP span export when an
//! endpoint is supplied by the application.

#![warn(missing_docs)]

use opentelemetry::{global, trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::{trace, Resource};
use thiserror::Error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub use tracing::{debug, error, info, instrument, span, trace, warn};

/// Error returned when a tracing subscriber or OTLP exporter cannot start.
#[derive(Debug, Error)]
pub enum InitError {
    /// The OTLP tracer pipeline could not be installed.
    #[error("OTLP initialization failed: {0}")]
    Otlp(String),
    /// The global tracing subscriber was already installed or rejected setup.
    #[error("tracing subscriber initialization failed: {0}")]
    Subscriber(String),
}

/// Initialize structured tracing and, when configured, OTLP span export.
///
/// The function should be called once during process startup. An absent
/// endpoint keeps tracing local; a supplied endpoint configures OTLP over
/// gRPC. `RUST_LOG` controls filtering and falls back to the phenoEvents
/// defaults when unset.
pub fn init_tracing(service_name: &str, endpoint: Option<&str>) -> Result<(), InitError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,pheno_events=debug,sqlx=warn"));
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);

    if let Some(endpoint) = endpoint {
        let provider = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint),
            )
            .with_trace_config(trace::Config::default().with_resource(Resource::new(vec![
                KeyValue::new("service.name", service_name.to_owned()),
            ])))
            .install_batch(Tokio)
            .map_err(|error| InitError::Otlp(error.to_string()))?;
        global::set_tracer_provider(provider.clone());
        let tracer = provider.tracer(service_name.to_owned());

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()
            .map_err(|error| InitError::Subscriber(error.to_string()))
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init()
            .map_err(|error| InitError::Subscriber(error.to_string()))
    }
}

/// Common tracing imports for applications using phenoEvents.
pub mod prelude {
    pub use crate::{debug, error, info, instrument, span, trace, warn};
}

#[cfg(test)]
mod tests {
    use super::{init_tracing, InitError};

    #[test]
    fn tracing_initialization_is_safe_to_attempt_multiple_times() {
        let first = init_tracing("pheno-events-test", None);
        let second = init_tracing("pheno-events-test", None);

        assert!(first.is_ok() || matches!(first, Err(InitError::Subscriber(_))));
        assert!(second.is_ok() || matches!(second, Err(InitError::Subscriber(_))));
    }
}
