# Observability (T22, OTLP)

This repo adopts [pheno-tracing](https://github.com/KooshaPari/pheno-tracing) v0.4.0
as the canonical OTLP tracer (ADR-012).

## Quickstart (local)

1. Copy `.env.example` to `.env` and edit if needed.
2. Start an OTLP collector:

   ```bash
   docker run --rm -p 4317:4317 -p 4318:4318 \
     otel/opentelemetry-collector-contrib:0.96.0
   ```

3. Run any binary in the workspace. Spans + metrics show up in your collector
   of choice (Jaeger, Tempo, Honeycomb, ...).

## What ships in this PR

- New crate: `crates/phenoevents-observability/` — re-exports
  `pheno-tracing` under a local name so version bumps are a single PR.
- `init_tracing(service_name, otlp_endpoint)` at top of `main()` in
  `./src/main.rs`.
- `#[tracing::instrument]` on key ops.
- Metrics: `requests_total` (Counter), `request_duration_seconds` (Histogram).
- CI: `.github/workflows/observability-smoke.yml` brings up an
  otel/opentelemetry-collector-contrib container, builds the workspace, runs
  the smoke test, and asserts OTLP receiver got at least one span.

## Sampling

Defaults to `AlwaysOn`. To switch to parent-based ratio sampling, set:

```bash
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1
```

## Out of scope

- Federation mTLS (ADR-046) — separate PR.
- W3C tracecontext propagation over `reqwest` — separate PR.
