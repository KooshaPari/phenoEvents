# PhenoEvents — Agent Notes

## Stack
- Rust (2021 edition) — `lib.rs` crate
- `sqlx` (SQLite) for outbox + projections
- `tokio` for async runtime
- `tracing` + `tracing-subscriber` for observability
- `serde_json` + `jsonschema` for schema registry
- `uuid` v7 for event IDs

## Build & Test
```bash
cargo test          # 32 tests (bus, core, envelope, projection, schema, observability)
cargo fmt --check
cargo clippy -- -D warnings
cargo doc --no-deps
```

## Key Files
| File | Purpose |
|------|---------|
| `src/lib.rs` | Module re-exports |
| `src/bus/mod.rs` | `SqliteBus` — outbox, publish/subscribe, idempotency, DLQ |
| `src/core/envelope.rs` | `EventEnvelope` builder + v7 UUID |
| `src/projection/mod.rs` | `OrderProjection` — checkpointed SQL read-model |
| `src/schema/registry.rs` | `SchemaRegistry` — additive JSON schema validation |
| `src/observability.rs` | `init_tracing()`, counters, span generation |

## Hygiene Rules
- Dependabot configured for weekly cargo + GitHub Actions updates
- No `target/` or `pheno-events/build/` in tree (`.gitignore` handles it)
- All PRs must pass `fmt`, `clippy`, and `test` (CI enforces)
- `SECURITY.md` exists for vulnerability reporting
- Scorecard workflow runs on push to `main`

## Architecture
- Hexagonal port: domain code depends on `EventBus` trait, not on a specific broker
- Current implementation: `SqliteBus` with in-process SQLite outbox
- Adapters (NATS, Redis, Kafka) are future ports; `src/` is the primary implementation
- `EventEnvelope` carries causation + correlation IDs for traceability
- `OrderProjection` demonstrates checkpointed read-model rebuild from outbox

## Known Gaps
- No external broker adapters yet (NATS/Kafka/Redis are stubbed in old `ports/`)
- No `CHANGELOG.md` yet (add when cutting first release)
- No benchmark suite
