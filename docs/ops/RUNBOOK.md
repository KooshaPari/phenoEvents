# phenoEvents — Ops Runbook

## Outbox stall (events stuck in `pending`/`retrying`)

**Symptom:** `SELECT status, COUNT(*) FROM outbox GROUP BY status` shows a growing
`pending` or `retrying` count; `acked` count is not increasing.

**Causes:**
- No active subscriber (worker was dropped or process crashed).
- `next_attempt_at` is far in the future due to exponential backoff.
- SQLite WAL checkpoint lag blocking writes.

**Resolution:**
1. Confirm a subscriber is running: check process list / service health.
2. Inspect the stalled row: `SELECT event_id, attempts, last_error, next_attempt_at FROM outbox WHERE status IN ('pending','retrying') LIMIT 10;`
3. If `next_attempt_at` is in the future and the error is transient, wait for the backoff
   window to pass (default: `10ms * attempts`).
4. To force immediate retry: `UPDATE outbox SET next_attempt_at = datetime('now'), status = 'pending' WHERE status = 'retrying' AND event_id = '<id>';`
5. If the process crashed, restart the subscriber. The outbox is durable — events will
   resume from `pending` state automatically.

## DLQ drain

**Symptom:** `SELECT COUNT(*) FROM outbox WHERE status = 'dlq'` is non-zero.

**Meaning:** Event exceeded `max_retries` (default: 3). `last_error` contains the
terminal failure reason.

**Resolution:**
1. Inspect: `SELECT event_id, last_error, attempts FROM outbox WHERE status = 'dlq';`
2. Fix the underlying handler error (code bug, bad payload, downstream outage).
3. Requeue: `UPDATE outbox SET status = 'pending', attempts = 0, next_attempt_at = datetime('now') WHERE status = 'dlq' AND event_id = '<id>';`
4. To bulk requeue all DLQ events (use with care): `UPDATE outbox SET status = 'pending', attempts = 0, next_attempt_at = datetime('now') WHERE status = 'dlq';`

## Retry storm mitigation

**Risk:** Bulk requeue of DLQ events while the downstream is still unhealthy causes a
retry storm that fills the outbox worker's poll loop.

**Mitigation:**
- Requeue in small batches (10–50 events).
- Increase `poll_interval` temporarily: `SqliteBus::new(pool).await?.with_poll_interval(Duration::from_secs(5))`.
- Monitor `EVENTS_FAILED` counter from `pheno_events::observability::metrics()` to track failure rate.

## Schema breaking-change rejection

**Symptom:** `SchemaRegistry::register` returns `ValidationError::BreakingChange`.

**Meaning:** A new schema version removed required fields or changed field types from
a previously registered schema — this violates the additive-evolution policy.

**Resolution:**
1. Increment the schema version (do not overwrite an existing version).
2. Make the old fields optional in the new version, or keep them.
3. Old consumers pinned to the old version continue to work unaffected.

## SLO targets (advisory, not yet enforced in CI)

| Metric | Target |
|---|---|
| `publish` p99 latency | < 5 ms (SQLite in-process) |
| `subscribe` handler dispatch p99 | < 10 ms (from outbox poll to handler call) |
| DLQ rate | < 0.1% of published events under normal conditions |
| Outbox staleness (time from publish to ack) | < 100 ms (default poll interval 25 ms) |

Run `cargo bench --bench bus` to check publish throughput against these targets locally.
