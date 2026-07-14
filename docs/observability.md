# Observability (T22, OTLP)

`phenoevents-observability` is the canonical OTLP integration for this
repository. It configures structured logs by default and exports spans when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Quickstart (local)

1. Copy `.env.example` to `.env` and edit if needed.
2. Start an OTLP collector:

   ```bash
   docker run --rm -p 4317:4317 -p 4318:4318 \
     otel/opentelemetry-collector-contrib:0.96.0
   ```

3. Call `pheno_events::observability::init_tracing()` during process startup,
   then run the application. Spans show up in your collector of choice
   (Jaeger, Tempo, Honeycomb, ...).

## What ships in this PR

- `crates/phenoevents-observability/` owns the OTLP tracer and structured-log
  configuration.
- `pheno_events::observability::init_tracing()` reads the endpoint from
  `OTEL_EXPORTER_OTLP_ENDPOINT`.
- Event spans include event id, type, source, and correlation id.
- In-process counters track published, processed, and failed events through
  `pheno_events::observability::metrics()`.
- CI: `.github/workflows/observability-smoke.yml` brings up an
  otel/opentelemetry-collector-contrib container, builds the workspace, runs
  the smoke test, and asserts the OTLP receiver is reachable.

## Out of scope

- Federation mTLS (ADR-046) — separate PR.
- W3C tracecontext propagation over `reqwest` — separate PR.
