# phenoEvents

Rust event-bus library. SQLite outbox, at-least-once delivery, idempotency, DLQ,
schema registry, and SQL read-model projections.

## Build & Test

```bash
cargo fmt --check          # formatting gate
cargo clippy -- -D warnings  # lint gate
cargo test                 # all unit + integration tests (22+ cases)
cargo test --test property_tests  # proptest property-based suite
cargo bench --bench bus    # Criterion publish throughput
cargo bench --bench schema # Criterion schema register/validate
cargo doc --no-deps        # doc build
```

`just check` runs fmt + clippy + test in one shot (see `justfile`).

## Project Layout

| Path | Role |
|---|---|
| `src/bus/mod.rs` | `SqliteBus` — outbox, retries, DLQ; `Bus` trait |
| `src/bus/in_memory.rs` | `InMemoryBus` — non-durable in-process bus for tests |
| `src/core/` | `EventEnvelope` — v7 UUID, causation/correlation IDs, schema version, builder |
| `src/observability.rs` | `init_tracing()`, atomic counters (`metrics()`), `trace_envelope()` |
| `src/projection/` | `OrderProjection` — checkpointed SQL read-model example |
| `src/schema/registry.rs` | `SchemaRegistry` — additive JSON schema validation |
| `benches/bus.rs` | Criterion publish throughput benchmark |
| `benches/schema.rs` | Criterion schema register/validate benchmark |
| `tests/property_tests.rs` | proptest property-based tests for envelope + schema invariants |
| `.github/workflows/ci.yml` | fmt + clippy + test + coverage + OSV scan |
| `.github/dependabot.yml` | Weekly cargo + Actions dep updates |

## Key Invariants

- `EventEnvelope.id` is a v7 UUID (time-ordered) — never use v4 for outbox ordering.
- `event_type` and `source` must be non-empty; `schema_version` must be >= 1 (validated in `build()`).
- `SqliteBus` uses `INSERT OR IGNORE` for idempotency on publish; duplicate detection is via `handled_events` table.
- Schema evolution is additive-only: adding required fields to an existing version is rejected.
- `Subscription` drop aborts the worker task — always keep the handle alive.

## Conventions

- Follow existing code style (2021 edition, `thiserror` for errors, `async-trait` for trait impls).
- Do not bypass linters/formatters (`cargo fmt`, `cargo clippy -- -D warnings`).
- Add or update tests for any new behavior; property tests preferred for validation logic.
- `Cargo.lock` is committed — do not add it back to `.gitignore`.
- Reference: `~/.claude/CLAUDE.md` and `../../CLAUDE.md` (global Phenotype governance).

## Dependency Hygiene

- `Cargo.lock` is tracked in git (fixed 2026-06-30 — was incorrectly gitignored).
- Dependabot runs weekly for both cargo and GitHub Actions deps.
- OSV Scanner runs in CI on every push/PR against `Cargo.lock`.
- `cargo deny` policy in `deny.toml` — run `cargo deny check` before adding deps.
