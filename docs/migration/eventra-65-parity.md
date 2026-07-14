# Eventra #65 Migration Parity

Date: 2026-07-04
Status: canonical migration evidence

## Source

Eventra PR #65 (`feat: add eventkit otel + sqlite outbox`) merged three runtime-bus concerns into the non-canonical Eventra repository:

- OTLP initialization helpers in `rust/eventkit-obs/src/otel.rs`
- SQLite outbox storage in `rust/phenotype-event-bus/src/outbox.rs`
- outbox relay span fields in `rust/phenotype-event-bus/src/outbox_relay.rs`

Eventra's runtime-bus boundary says new reusable runtime-bus capabilities belong in `phenoEvents`. This document records the canonical disposition so Eventra can be archived after downstream references move here.

## Disposition

| Eventra #65 behavior | phenoEvents canonical location | Disposition |
| --- | --- | --- |
| OTLP initialization helper | `crates/phenoevents-observability` and `src/observability.rs` | Implemented locally in the canonical repository. Do not copy Eventra's `eventkit-obs` API or depend on unavailable external tracing repositories. |
| SQLite-backed outbox | `src/bus/mod.rs::SqliteBus` | Already covered by the canonical `SqliteBus` outbox, at-least-once delivery, idempotent duplicate detection, retries, and DLQ. |
| Pending outbox count for health/metrics | `SqliteBus::pending_count` | Added as the canonical inspection surface for actionable queued work. |
| DLQ count for operators | `SqliteBus::dlq_count` | Added as the canonical inspection surface for exhausted retries. |
| Last publish/handler error | `SqliteBus::last_error` | Added as the canonical diagnostic equivalent of Eventra's outbox `last_error`. |
| Relay span correlation fields | `src/observability.rs::trace_envelope` and `SqliteBus` publish/process instrumentation | Covered through event spans carrying event id, event type, source, and correlation id. phenoEvents does not carry Eventra's separate aggregate id field in its envelope model. |

## Archive Gate

Eventra can be archived after this migration lands and consumers are pointed at `phenoEvents` for runtime event-bus behavior. Eventra should keep only historical compatibility notes and links to this document; new SQLite outbox, DLQ, retry, projection, and OTLP runtime-bus work should land here.
