# Threat Model

> **Source of truth:** phenoEvents (EventBus port with hexagonal architecture, Rust)

## Assets

1. The SQLite outbox table is the source of truth: unpublished events queued for delivery, holding payload, schema version, causation/correlation IDs (v7 UUIDs), and producer identity.
2. The Dead Letter Queue (DLQ) stores events that exhausted retries — it carries sensitive payload data and must not leak.
3. The `SchemaRegistry` governs JSON-Schema versions and the additive-evolution invariants that downstream consumers depend on.
4. The `OrderProjection` and any other `projection::*` read-models reflect committed event state in SQL; corruption here causes silent business-data drift.
5. The publish/subscribe API surface in `bus::SqliteBus` (and any future `Bus` trait ports) is the trust boundary that any process crossing into the bus must authenticate against.
6. Observability traces + counters expose event flow, retry counts, and DLQ growth — leaking them reveals system shape to attackers.

## Threats (STRIDE)

- **Spoofing** — a producer forges `causation_id`/`correlation_id` to splice events into someone else's workflow, or replays an old envelope to re-trigger a downstream handler.
- **Tampering** — direct SQL writes to the outbox/DLQ tables bypass JSON-Schema validation; schema files in `SchemaRegistry` are mutated to weaken additive-evolution guarantees; projection checkpoints are rewound to replay history.
- **Repudiation** — missing or mutable producer identity on the envelope lets a publisher deny having emitted a side-effectful event.
- **Information Disclosure** — DLQ contents, outbox payloads, and `tracing` spans may contain PII or secrets in JSON; SQLite file theft exposes all queued and dead-lettered events.
- **Denial of Service** — unbounded `publish()` rate fills the outbox; malformed-but-schema-valid events trigger expensive projection rebuilds; retry storms amplify load; a flood of `schema_version` upgrades forces registry churn.
- **Elevation of Privilege** — a misconfigured consumer registers a handler that subscribes to all topics; a buggy migration escalates `SqliteBus` to a multi-tenant role it was not designed for; future `Bus` ports (HTTP/NATS/Kafka) introduce network-reachable surfaces absent from the local-only SQLite baseline.

## Residual Risk & Revision Cadence

Residual risk is concentrated in three places: (a) the SQL trust boundary — any process with write access to the outbox/projection tables can subvert guarantees that the Rust layer is designed to enforce; mitigation = `PRAGMA` hardening + WAL mode + a separate DB role for the bus; (b) schema-evolution drift — additive-only is a process guarantee, not a code one; mitigation = CI check that fails on non-additive schema PRs; (c) DLQ-as-data-lake — DLQ contents age and may outlive their containing event's retention policy; mitigation = scheduled DLQ reaper with redaction. This model is reviewed at every minor version bump (every change to `bus`, `core::EventEnvelope`, `schema::SchemaRegistry`, or any new transport port), on every CVE in `sqlx`/`tokio`/`jsonschema`/`serde_json`, and at least quarterly. Owners: bus = bus maintainer, schema = schema maintainer, projection = projection maintainer; cross-cutting risk is owned by the security-designated reviewer named in CODEOWNERS. If a `Bus` trait port lands, the model is re-baselined before that port is marked stable.
