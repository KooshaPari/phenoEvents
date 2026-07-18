# phenotype-event-bus

> Typed async pub/sub for the Phenotype ecosystem — durable `SqliteBus`, `InMemoryBus`, schema registry, projections, and tracing/OTel integration.

This crate was absorbed from [`KooshaPari/phenoEvents`](https://github.com/KooshaPari/phenoEvents) on **2026-07-17** as part of the `2026-07-17-queue-refresh-batch2` wave of the kooshapari rationalization program.

## What lives here

| Path | Purpose |
|------|---------|
| `src/bus/` | `SqliteBus` (durable, outbox, retries, DLQ) + `InMemoryBus` (non-persistent) |
| `src/core/` | `EventEnvelope` — v7 UUIDs, causation/correlation IDs, schema versioning |
| `src/observability.rs` | Tracing wiring that delegates to the `phenotype-event-bus-observability` sub-crate |
| `src/projection/` | `OrderProjection` — checkpointed SQL read-model |
| `src/schema/` | `SchemaRegistry` — additive JSON schema validation |
| `observability/` | Sub-crate: OTel/OTLP tracing initialization (`phenotype-event-bus-observability`) |
| `benches/` | Criterion benchmarks for `bus` and `schema` |
| `tests/` | Property tests for envelopes and schema validation |

## Quick start

```rust
use phenotype_event_bus::{bus::SqliteBus, core::EventEnvelope};
use sqlx::SqlitePool;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePool::connect("sqlite::memory:").await?;
    let bus = SqliteBus::new(pool).await?;

    let envelope = EventEnvelope::builder("user.created", "app", json!({"id": 1}))
        .build()?;
    bus.publish(envelope).await?;
    Ok(())
}
```

## Build & test

```bash
cargo test -p phenotype-event-bus          # 37 unit + 8 property tests
cargo bench -p phenotype-event-bus         # Criterion benches
cargo check -p phenotype-event-bus         # Workspace check (passes)
```

## Origin

This crate was previously published as [`pheno-events`](https://crates.io/crates/pheno-events) at `KooshaPari/phenoEvents`. The crate has been **renamed** to `phenotype-event-bus` to:

1. Resolve a phantom workspace dependency in the `pheno` monorepo's `Cargo.toml` (`phenotype-event-bus = { path = "crates/phenotype-event-bus" }` was declared but the crate did not exist).
2. Adopt the `phenotype-*` naming convention used by every other first-party crate in the workspace.
3. Move the observability sub-crate into a nested `observability/` directory so the workspace layout mirrors the source-of-truth (where `crates/<crate>/observability/` is the established pattern).

## Migration from `pheno-events`

| Was | Now |
|-----|-----|
| Crate name: `pheno-events` | `phenotype-event-bus` |
| Imports: `use pheno_events::...` | `use phenotype_event_bus::...` |
| Sub-crate: `phenoevents-observability` | `phenotype-event-bus-observability` (nested under this crate) |
| Tracing filter target: `pheno_events=debug` | `phenotype_event_bus=debug` |
| Test target name: `pheno-events-test` | `phenotype-event-bus-test` |

A compatibility re-export shim was **not** added; the upstream crate was published `0.1.0` with no documented stable-API commitment, and the only known downstream consumer is `pheno` itself (where the rename is applied in lockstep with this commit).

## Boundary doc

See [`docs/boundary/phenotype-event-bus.md`](../../../phenotype-registry/docs/boundary/phenotype-event-bus.md) in the registry spine.

## License

MIT OR Apache-2.0
