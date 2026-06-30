# phenoEvents — Typed Async Event Bus

Typed async pub/sub for Phenotype collections. Provides an `EventEnvelope`
contract with two interchangeable bus implementations plus schema registry
and read-model projections.

## Build & Test

```bash
cargo test                   # 22+ tests (core, bus, schema, projection)
cargo fmt --check
cargo clippy -- -D warnings
cargo doc --no-deps
just check                   # same as above via `just`
```

## Project Layout

```
src/
├── bus/             Bus trait + SqliteBus (durable outbox, retries, DLQ)
│   ├── mod.rs          SqliteBus — SQLite outbox, publish/subscribe, idempotency
│   └── in_memory.rs    InMemoryBus — in-process pub/sub with per-subscriber queues
├── core/            Domain primitives
│   └── envelope.rs     EventEnvelope builder, v7 UUID, validation
├── lib.rs           Public module re-exports
├── observability.rs Tracing init, atomic counters, envelope spans
├── projection/      Read-model projection engine
│   └── mod.rs          OrderProjection — checkpointed SQL read-model
└── schema/          Schema registry & payload validation
    └── registry.rs     SchemaRegistry — additive JSON schema validation
tests/               Integration tests (pact/)
```

| Module | Responsibility |
|---|---|
| `bus` | `Bus` trait, `SqliteBus` (SQLite outbox + DLQ), `InMemoryBus` (in-process) |
| `core` | `EventEnvelope` with causation/correlation IDs, schema version, validation |
| `observability` | `tracing` init, `EnvFilter`, atomic counters, `trace_envelope()` spans |
| `projection` | `OrderProjection` — rebuild read-models from the outbox |
| `schema` | `SchemaRegistry` — enforce additive-only JSON schema evolution |

## Conventions

- Follow the existing code style; do not bypass linters/formatters/type checkers.
- Add or update tests for any new behavior.
- All errors are typed (`EnvelopeError`, `PublishError`, `SubscribeError`).
- Property tests live alongside unit tests in each module's `#[cfg(test)]` block.
- Reference: `~/.claude/CLAUDE.md` and `../../CLAUDE.md` (global Phenotype governance).
