# PhenoEvents

Rust event-bus library with SQLite outbox, at-least-once delivery, idempotency,
DLQ, schema registry, and SQL read-model projections.

## Quick start

```rust
use pheno_events::{bus::SqliteBus, core::EventEnvelope};
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

## Layout

| Module | Purpose |
|--------|---------|
| `src/bus` | `SqliteBus` — outbox, publish/subscribe, retries, DLQ |
| `src/core` | `EventEnvelope` — v7 UUID, causation/correlation IDs, schema version |
| `src/observability` | Tracing + counters |
| `src/projection` | `OrderProjection` — checkpointed SQL read-model |
| `src/schema` | `SchemaRegistry` — additive JSON schema validation |

## Build & test

```bash
cargo test          # 22 unit tests
cargo fmt --check   # formatting
cargo clippy        # linting
```

## License

MIT OR Apache-2.0
