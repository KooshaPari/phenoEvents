use crate::core::EventEnvelope;
use crate::observability::{
    metrics, queue_metrics, retry_metrics, snapshot, sqlite_busy_retries, trace_envelope,
    worker_errors, MetricsSnapshot,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use sqlx::{Pool, Row, Sqlite};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration as StdDuration};
use tokio::{task::JoinHandle, time};
use tracing::Instrument;
use uuid::Uuid;
pub type HandlerResult = Result<(), HandlerError>;
pub type HandlerFuture = Pin<Box<dyn Future<Output = HandlerResult> + Send>>;
pub type Handler = Arc<dyn Fn(EventEnvelope, Option<Uuid>) -> HandlerFuture + Send + Sync>;

const SQLITE_BUSY_RETRY_LIMIT: u32 = 8;

pub mod in_memory;
pub use in_memory::InMemoryBus;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    pub event_id: Uuid,
    pub duplicate: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("handler nack: {0}")]
pub struct HandlerError(pub String);

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("invalid envelope: {0}")]
    InvalidEnvelope(#[from] crate::core::EnvelopeError),
    #[error("sqlite: {0}")]
    Sqlite(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] sqlx::Error),
}

pub struct Subscription {
    worker: JoinHandle<()>,
}

struct ClaimedEvent {
    envelope: EventEnvelope,
    claim_token: String,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[async_trait]
pub trait Bus: Send + Sync {
    async fn publish(&self, envelope: EventEnvelope) -> Result<Ack, PublishError>;
    async fn subscribe(&self, handler: Handler) -> Result<Subscription, SubscribeError>;
}

#[derive(Clone)]
pub struct SqliteBus {
    db: Pool<Sqlite>,
    max_retries: i64,
    poll_interval: StdDuration,
    lease_timeout: StdDuration,
}

impl SqliteBus {
    pub async fn new(db: Pool<Sqlite>) -> Result<Self, sqlx::Error> {
        let bus = Self {
            db,
            max_retries: 3,
            poll_interval: StdDuration::from_millis(25),
            lease_timeout: StdDuration::from_secs(60),
        };
        sqlx::query("PRAGMA busy_timeout = 5000")
            .execute(&bus.db)
            .await?;
        bus.migrate().await?;
        bus.refresh_metrics().await?;
        Ok(bus)
    }

    pub fn with_max_retries(mut self, max_retries: i64) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_lease_timeout(mut self, lease_timeout: StdDuration) -> Self {
        self.lease_timeout = lease_timeout.max(StdDuration::from_millis(1));
        self
    }

    async fn migrate(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS outbox (
                event_id TEXT PRIMARY KEY,
                envelope TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                claim_token TEXT
            );
            "#,
        )
        .execute(&self.db)
        .await?;

        let has_claim_token: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM pragma_table_info('outbox') WHERE name = 'claim_token' LIMIT 1",
        )
        .fetch_optional(&self.db)
        .await?;
        if has_claim_token.is_none() {
            match sqlx::query("ALTER TABLE outbox ADD COLUMN claim_token TEXT")
                .execute(&self.db)
                .await
            {
                Ok(_) => {}
                Err(error) if error.to_string().contains("duplicate column name") => {}
                Err(error) => return Err(error),
            }
        }

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_outbox_claim_due ON outbox (status, next_attempt_at, created_at)",
        )
        .execute(&self.db)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_outbox_claim_lease ON outbox (status, updated_at, created_at)",
        )
        .execute(&self.db)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_outbox_dlq_retention ON outbox (status, updated_at)",
        )
        .execute(&self.db)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS handled_events (
                event_id TEXT PRIMARY KEY,
                handled_at TEXT NOT NULL
            );
            "#,
        )
        .execute(&self.db)
        .await?;

        Ok(())
    }

    /// Reconcile queue gauges from durable SQLite state and return the current
    /// canonical snapshot. Deployments can call this before exporting metrics
    /// when rows may have changed outside the worker transition path.
    pub async fn refresh_metrics(&self) -> Result<MetricsSnapshot, sqlx::Error> {
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox WHERE status IN ('pending', 'retrying', 'in_progress')",
        )
        .fetch_one(&self.db)
        .await?;
        let dlq: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox WHERE status = 'dlq'")
            .fetch_one(&self.db)
            .await?;
        let oldest: Option<String> = sqlx::query_scalar(
            "SELECT MIN(created_at) FROM outbox WHERE status IN ('pending', 'retrying', 'in_progress')",
        )
        .fetch_one(&self.db)
        .await?;
        let oldest_age_ms = oldest
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
            .map(|value| {
                (Utc::now() - value.with_timezone(&Utc))
                    .num_milliseconds()
                    .max(0) as u64
            })
            .unwrap_or(0);
        let (queue_depth, dlq_depth) = queue_metrics();
        queue_depth.set(pending.max(0) as u64);
        dlq_depth.set(dlq.max(0) as u64);
        let (_, oldest_age) = retry_metrics();
        oldest_age.set(oldest_age_ms);
        let current = snapshot();
        Ok(MetricsSnapshot {
            queue_depth: pending.max(0) as u64,
            dlq_depth: dlq.max(0) as u64,
            oldest_event_age_ms: oldest_age_ms,
            ..current
        })
    }

    fn retry_delay(event_id: Uuid, attempt: i64) -> Duration {
        let exponent = attempt.saturating_sub(1).min(12) as u32;
        let base_ms = 10_i64.saturating_mul(1_i64 << exponent);
        let jitter_window = (base_ms / 4).max(1);
        let jitter = (event_id.as_u128() % jitter_window as u128) as i64;
        Duration::milliseconds((base_ms + jitter).min(30_000))
    }

    fn is_sqlite_busy(error: &sqlx::Error) -> bool {
        match error {
            sqlx::Error::Database(database_error) => {
                database_error.code().is_some_and(|code| code == "5")
                    || database_error.message().contains("database is locked")
            }
            _ => false,
        }
    }

    async fn insert_with_busy_retry(
        &self,
        event_id: &str,
        envelope_json: &str,
        now: &str,
    ) -> Result<bool, sqlx::Error> {
        let mut delay = StdDuration::from_millis(2);
        for attempt in 0..=SQLITE_BUSY_RETRY_LIMIT {
            let result = sqlx::query(
                r#"
                INSERT OR IGNORE INTO outbox
                    (event_id, envelope, status, attempts, next_attempt_at, created_at, updated_at)
                VALUES (?, ?, 'pending', 0, ?, ?, ?)
                "#,
            )
            .bind(event_id)
            .bind(envelope_json)
            .bind(now)
            .bind(now)
            .bind(now)
            .execute(&self.db)
            .await;

            match result {
                Ok(result) => return Ok(result.rows_affected() == 0),
                Err(error) if attempt < SQLITE_BUSY_RETRY_LIMIT && Self::is_sqlite_busy(&error) => {
                    sqlite_busy_retries().increment();
                    time::sleep(delay).await;
                    delay = (delay * 2).min(StdDuration::from_millis(250));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("busy retry loop always returns")
    }

    async fn claim_next(&self) -> Result<Option<ClaimedEvent>, sqlx::Error> {
        let mut delay = StdDuration::from_millis(2);
        for attempt in 0..=SQLITE_BUSY_RETRY_LIMIT {
            match self.claim_next_once().await {
                Ok(result) => return Ok(result),
                Err(error) if attempt < SQLITE_BUSY_RETRY_LIMIT && Self::is_sqlite_busy(&error) => {
                    sqlite_busy_retries().increment();
                    time::sleep(delay).await;
                    delay = (delay * 2).min(StdDuration::from_millis(250));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("busy retry loop always returns")
    }

    async fn claim_next_once(&self) -> Result<Option<ClaimedEvent>, sqlx::Error> {
        let now_dt = Utc::now();
        let now = now_dt.to_rfc3339();
        let lease_cutoff = (now_dt
            - Duration::from_std(self.lease_timeout).unwrap_or_else(|_| Duration::seconds(60)))
        .to_rfc3339();
        let mut tx = self.db.begin().await?;
        let row = sqlx::query(
            r#"
            SELECT event_id, envelope
            FROM outbox
            WHERE (status IN ('pending', 'retrying') AND next_attempt_at <= ?)
               OR (status = 'in_progress' AND updated_at < ?)
            ORDER BY created_at
            LIMIT 1
            "#,
        )
        .bind(&now)
        .bind(&lease_cutoff)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };

        let event_id: String = row.get("event_id");
        let envelope_json: String = row.get("envelope");
        let claim_token = Uuid::now_v7().to_string();
        let changed = sqlx::query(
            r#"
            UPDATE outbox
            SET status = 'in_progress', updated_at = ?, claim_token = ?
            WHERE event_id = ? AND (
                (status IN ('pending', 'retrying') AND next_attempt_at <= ?)
                OR (status = 'in_progress' AND updated_at < ?)
            )
            "#,
        )
        .bind(&now)
        .bind(&claim_token)
        .bind(event_id)
        .bind(&now)
        .bind(&lease_cutoff)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        tx.commit().await?;
        if changed == 0 {
            return Ok(None);
        }

        serde_json::from_str(&envelope_json)
            .map(|envelope| {
                Some(ClaimedEvent {
                    envelope,
                    claim_token,
                })
            })
            .map_err(|err| sqlx::Error::Decode(Box::new(err)))
    }

    async fn last_seen(&self) -> Result<Option<Uuid>, sqlx::Error> {
        let row =
            sqlx::query("SELECT event_id FROM handled_events ORDER BY handled_at DESC LIMIT 1")
                .fetch_optional(&self.db)
                .await?;

        row.map(|row| {
            let event_id: String = row.get("event_id");
            Uuid::parse_str(&event_id).map_err(|err| sqlx::Error::Decode(Box::new(err)))
        })
        .transpose()
    }

    async fn already_handled(&self, event_id: Uuid) -> Result<bool, sqlx::Error> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM handled_events WHERE event_id = ?")
                .bind(event_id.to_string())
                .fetch_one(&self.db)
                .await?;
        Ok(count > 0)
    }

    async fn mark_handled(&self, event_id: Uuid, claim_token: &str) -> Result<bool, sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.db.begin().await?;
        let changed = sqlx::query(
            "UPDATE outbox SET status = 'acked', updated_at = ?, claim_token = NULL WHERE event_id = ? AND status = 'in_progress' AND claim_token = ?",
        )
            .bind(&now)
            .bind(event_id.to_string())
            .bind(claim_token)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if changed == 0 {
            tx.commit().await?;
            return Ok(false);
        }
        sqlx::query("INSERT OR IGNORE INTO handled_events (event_id, handled_at) VALUES (?, ?)")
            .bind(event_id.to_string())
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        let (queue_depth, _) = queue_metrics();
        queue_depth.decrement_by(1);
        Ok(true)
    }

    async fn mark_failed(
        &self,
        event_id: Uuid,
        claim_token: &str,
        error: String,
    ) -> Result<bool, sqlx::Error> {
        let now = Utc::now();
        let mut tx = self.db.begin().await?;
        let attempts: Option<i64> =
            sqlx::query_scalar("SELECT attempts FROM outbox WHERE event_id = ?")
                .bind(event_id.to_string())
                .fetch_optional(&mut *tx)
                .await?;
        let Some(attempts) = attempts else {
            // Event was already removed from the outbox — nothing to update.
            tx.commit().await?;
            return Ok(false);
        };
        let next_attempts = attempts + 1;
        let status = if next_attempts >= self.max_retries {
            "dlq"
        } else {
            "retrying"
        };
        let next = (now + Self::retry_delay(event_id, next_attempts)).to_rfc3339();
        let changed = sqlx::query(
            r#"
            UPDATE outbox
            SET status = ?, attempts = ?, next_attempt_at = ?, last_error = ?, updated_at = ?, claim_token = NULL
            WHERE event_id = ? AND status = 'in_progress' AND claim_token = ?
            "#,
        )
        .bind(status)
        .bind(next_attempts)
        .bind(next)
        .bind(error)
        .bind(now.to_rfc3339())
        .bind(event_id.to_string())
        .bind(claim_token)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        if changed > 0 {
            if status == "retrying" {
                let (retried, _) = retry_metrics();
                retried.increment();
            } else {
                let (queue_depth, dlq_depth) = queue_metrics();
                queue_depth.decrement_by(1);
                dlq_depth.increment();
            }
        }
        Ok(changed > 0)
    }

    async fn process_once(&self, handler: &Handler) -> Result<bool, sqlx::Error> {
        let Some(claimed) = self.claim_next().await? else {
            return Ok(false);
        };
        let envelope = claimed.envelope;
        let claim_token = claimed.claim_token;

        if self.already_handled(envelope.id).await? {
            if self.mark_handled(envelope.id, &claim_token).await? {
                let (_, events_processed, _) = metrics();
                events_processed.increment();
            }
            return Ok(true);
        }

        let span = trace_envelope(&envelope);
        async {
            let last_seen = self.last_seen().await?;
            match handler(envelope.clone(), last_seen).await {
                Ok(()) => {
                    if self.mark_handled(envelope.id, &claim_token).await? {
                        let (_, events_processed, _) = metrics();
                        events_processed.increment();
                    }
                }
                Err(err) => {
                    if self
                        .mark_failed(envelope.id, &claim_token, err.to_string())
                        .await?
                    {
                        let (_, _, events_failed) = metrics();
                        events_failed.increment();
                    }
                }
            }
            Ok::<(), sqlx::Error>(())
        }
        .instrument(span)
        .await?;

        Ok(true)
    }

    pub async fn status(&self, event_id: Uuid) -> Result<Option<String>, sqlx::Error> {
        sqlx::query_scalar("SELECT status FROM outbox WHERE event_id = ?")
            .bind(event_id.to_string())
            .fetch_optional(&self.db)
            .await
    }

    pub async fn attempts(&self, event_id: Uuid) -> Result<Option<i64>, sqlx::Error> {
        sqlx::query_scalar("SELECT attempts FROM outbox WHERE event_id = ?")
            .bind(event_id.to_string())
            .fetch_optional(&self.db)
            .await
    }

    pub async fn last_error(&self, event_id: Uuid) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query("SELECT last_error FROM outbox WHERE event_id = ?")
            .bind(event_id.to_string())
            .fetch_optional(&self.db)
            .await?;
        Ok(row.and_then(|row| row.get("last_error")))
    }

    pub async fn pending_count(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox WHERE status IN ('pending', 'retrying', 'in_progress')",
        )
        .fetch_one(&self.db)
        .await
    }

    pub async fn dlq_count(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox WHERE status = 'dlq'")
            .fetch_one(&self.db)
            .await
    }

    pub async fn replay_dlq(&self, event_id: Uuid) -> Result<bool, sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE outbox SET status = 'pending', attempts = 0, next_attempt_at = ?, last_error = NULL, updated_at = ?, claim_token = NULL WHERE event_id = ? AND status = 'dlq'",
        )
        .bind(&now)
        .bind(&now)
        .bind(event_id.to_string())
        .execute(&self.db)
        .await?;
        if result.rows_affected() > 0 {
            let (queue_depth, dlq_depth) = queue_metrics();
            queue_depth.increment();
            dlq_depth.decrement_by(1);
        }
        Ok(result.rows_affected() > 0)
    }

    pub async fn prune_dlq_before(
        &self,
        cutoff: chrono::DateTime<Utc>,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query("DELETE FROM outbox WHERE status = 'dlq' AND updated_at < ?")
            .bind(cutoff.to_rfc3339())
            .execute(&self.db)
            .await?;
        if result.rows_affected() > 0 {
            let (_, dlq_depth) = queue_metrics();
            dlq_depth.decrement_by(result.rows_affected());
        }
        Ok(result.rows_affected())
    }
}

#[async_trait]
impl Bus for SqliteBus {
    async fn publish(&self, envelope: EventEnvelope) -> Result<Ack, PublishError> {
        let span = trace_envelope(&envelope);
        let _guard = span.enter();
        envelope.validate()?;
        let now = Utc::now().to_rfc3339();
        let event_id = envelope.id;
        let envelope_json = serde_json::to_string(&envelope)
            .map_err(|err| PublishError::Sqlite(sqlx::Error::Encode(Box::new(err))))?;
        let duplicate = self
            .insert_with_busy_retry(&event_id.to_string(), &envelope_json, &now)
            .await?;
        let (events_published, _, _) = metrics();
        events_published.increment();
        if !duplicate {
            let (queue_depth, _) = queue_metrics();
            queue_depth.increment();
        }

        Ok(Ack {
            event_id,
            duplicate,
        })
    }

    async fn subscribe(&self, handler: Handler) -> Result<Subscription, SubscribeError> {
        self.migrate().await?;
        let bus = self.clone();
        let worker = tokio::spawn(async move {
            loop {
                match bus.process_once(&handler).await {
                    Ok(true) => {}
                    Ok(false) => time::sleep(bus.poll_interval).await,
                    Err(error) => {
                        worker_errors().increment();
                        tracing::error!(error = %error, "event-bus worker poll failed");
                        time::sleep(bus.poll_interval).await;
                    }
                }
            }
        });

        Ok(Subscription { worker })
    }
}

#[cfg(test)]
mod tests {
    use super::{Bus, Handler, HandlerError, SqliteBus};
    use crate::core::EventEnvelope;
    use chrono::{Duration as ChronoDuration, Utc};
    use serde_json::json;
    use sqlx::{
        sqlite::{SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
        Row, SqlitePool,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use std::{collections::HashSet, str::FromStr, time::Instant};
    use tempfile::NamedTempFile;
    use tokio::time::{sleep, timeout, Duration};
    use uuid::Uuid;

    async fn bus() -> SqliteBus {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite pool");
        SqliteBus::new(pool).await.expect("bus")
    }

    async fn file_pool(url: &str) -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::from_str(url)
                    .expect("sqlite options")
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Normal)
                    .busy_timeout(Duration::from_secs(5)),
            )
            .await
            .expect("sqlite file pool")
    }

    fn event() -> EventEnvelope {
        EventEnvelope::builder("user.created", "tests", json!({"id": 1}))
            .build()
            .expect("event")
    }

    async fn eventually<F, Fut>(mut assertion: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        eventually_with_timeout(Duration::from_secs(2), &mut assertion).await;
    }

    async fn eventually_with_timeout<F, Fut>(max_wait: Duration, mut assertion: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        timeout(max_wait, async {
            loop {
                if assertion().await {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("condition met");
    }

    #[tokio::test]
    async fn publish_persists_event() {
        let bus = bus().await;
        let envelope = event();
        let ack = bus.publish(envelope.clone()).await.expect("publish");

        assert_eq!(ack.event_id, envelope.id);
        assert!(!ack.duplicate);
        assert_eq!(
            bus.status(envelope.id).await.expect("status"),
            Some("pending".into())
        );
        assert_eq!(bus.pending_count().await.expect("pending count"), 1);
        assert_eq!(bus.dlq_count().await.expect("dlq count"), 0);
    }

    #[tokio::test]
    async fn subscribe_handles_and_acks_event() {
        let bus = bus().await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler_seen = seen.clone();
        let handler: Handler = Arc::new(move |event, _last_seen| {
            let handler_seen = handler_seen.clone();
            Box::pin(async move {
                handler_seen.lock().expect("seen").push(event.id);
                Ok(())
            })
        });

        let _subscription = bus.subscribe(handler).await.expect("subscribe");
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");

        eventually(|| {
            let seen = seen.clone();
            async move { seen.lock().expect("seen").contains(&envelope.id) }
        })
        .await;
        assert_eq!(
            bus.status(envelope.id).await.expect("status"),
            Some("acked".into())
        );
        assert_eq!(bus.pending_count().await.expect("pending count"), 0);
    }

    #[tokio::test]
    async fn subscriber_poll_errors_increment_worker_error_metric() {
        let bus = bus().await;
        let before = crate::observability::worker_errors().get();
        let handler: Handler = Arc::new(move |_event, _last_seen| Box::pin(async { Ok(()) }));
        let subscription = bus.subscribe(handler).await.expect("subscribe");

        bus.db.close().await;
        eventually_with_timeout(Duration::from_secs(2), || async {
            crate::observability::worker_errors().get() > before
        })
        .await;
        drop(subscription);
    }

    #[tokio::test]
    async fn nack_retries_until_success() {
        let bus = bus().await.with_max_retries(3);
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let handler: Handler = Arc::new(move |_event, _last_seen| {
            let handler_calls = handler_calls.clone();
            Box::pin(async move {
                if handler_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(HandlerError("try again".into()))
                } else {
                    Ok(())
                }
            })
        });

        let _subscription = bus.subscribe(handler).await.expect("subscribe");
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");

        eventually(|| async { bus.status(envelope.id).await.unwrap() == Some("acked".into()) })
            .await;
        assert_eq!(bus.attempts(envelope.id).await.expect("attempts"), Some(1));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn nack_moves_to_dlq_after_retry_budget() {
        let bus = bus().await.with_max_retries(2);
        let handler: Handler = Arc::new(move |_event, _last_seen| {
            Box::pin(async move { Err(HandlerError("always fails".into())) })
        });

        let _subscription = bus.subscribe(handler).await.expect("subscribe");
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");

        eventually(|| async { bus.status(envelope.id).await.unwrap() == Some("dlq".into()) }).await;
        assert_eq!(bus.attempts(envelope.id).await.expect("attempts"), Some(2));
        assert_eq!(
            bus.last_error(envelope.id).await.expect("last error"),
            Some("handler nack: always fails".into())
        );
        assert_eq!(bus.pending_count().await.expect("pending count"), 0);
        assert_eq!(bus.dlq_count().await.expect("dlq count"), 1);
    }

    #[tokio::test]
    async fn dlq_event_can_be_replayed_and_acked() {
        let bus = bus().await.with_max_retries(1);
        let failing: Handler = Arc::new(move |_event, _last_seen| {
            Box::pin(async { Err(HandlerError("temporary outage".into())) })
        });
        let subscription = bus.subscribe(failing).await.expect("subscribe");
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");
        eventually(|| async { bus.status(envelope.id).await.unwrap() == Some("dlq".into()) }).await;
        drop(subscription);

        assert!(bus.replay_dlq(envelope.id).await.expect("replay"));
        assert!(!bus
            .replay_dlq(envelope.id)
            .await
            .expect("idempotent replay"));
        assert_eq!(bus.attempts(envelope.id).await.expect("attempts"), Some(0));
        assert_eq!(bus.last_error(envelope.id).await.expect("last error"), None);

        let succeeding: Handler = Arc::new(move |_event, _last_seen| Box::pin(async { Ok(()) }));
        let _subscription = bus.subscribe(succeeding).await.expect("resubscribe");
        eventually(|| async { bus.status(envelope.id).await.unwrap() == Some("acked".into()) })
            .await;
    }

    #[tokio::test]
    async fn handler_receives_last_seen_for_idempotency_context() {
        let bus = bus().await;
        let last_seen_values = Arc::new(Mutex::new(Vec::<Option<Uuid>>::new()));
        let values = last_seen_values.clone();
        let handler: Handler = Arc::new(move |_event, last_seen| {
            let values = values.clone();
            Box::pin(async move {
                values.lock().expect("values").push(last_seen);
                Ok(())
            })
        });

        let _subscription = bus.subscribe(handler).await.expect("subscribe");
        let first = event();
        let second = event();
        bus.publish(first.clone()).await.expect("publish first");
        bus.publish(second.clone()).await.expect("publish second");

        eventually(|| {
            let values = last_seen_values.clone();
            async move { values.lock().expect("values").len() == 2 }
        })
        .await;
        let values = last_seen_values.lock().expect("values");
        assert_eq!(values[0], None);
        assert_eq!(values[1], Some(first.id));
    }

    #[tokio::test]
    async fn attempts_returns_none_for_unknown_event() {
        let bus = bus().await;
        let result = bus.attempts(Uuid::now_v7()).await.expect("query");
        assert_eq!(result, None);
    }

    #[test]
    fn retry_delay_is_bounded_exponential_and_event_stable() {
        let event_id = Uuid::from_u128(7);
        assert!(SqliteBus::retry_delay(event_id, 2) > SqliteBus::retry_delay(event_id, 1));
        assert_eq!(
            SqliteBus::retry_delay(event_id, 100),
            chrono::Duration::seconds(30)
        );
        assert_eq!(
            SqliteBus::retry_delay(event_id, 3),
            SqliteBus::retry_delay(event_id, 3)
        );
    }

    #[tokio::test]
    async fn crash_recovery_processes_pending_outbox_after_new_subscriber() {
        let bus = bus().await;
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");

        let seen = Arc::new(AtomicUsize::new(0));
        let handler_seen = seen.clone();
        let handler: Handler = Arc::new(move |_event, _last_seen| {
            let handler_seen = handler_seen.clone();
            Box::pin(async move {
                handler_seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });
        let _subscription = bus.subscribe(handler).await.expect("subscribe");

        eventually(|| async { bus.status(envelope.id).await.unwrap() == Some("acked".into()) })
            .await;
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dlq_can_be_replayed_and_pruned() {
        let bus = bus().await.with_max_retries(1);
        let handler: Handler = Arc::new(move |_event, _last_seen| {
            Box::pin(async move { Err(HandlerError("permanent".into())) })
        });
        let subscription = bus.subscribe(handler).await.expect("subscribe");
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");
        eventually(|| async { bus.status(envelope.id).await.unwrap() == Some("dlq".into()) }).await;
        drop(subscription);
        assert!(bus.replay_dlq(envelope.id).await.expect("replay"));
        assert!(!bus
            .replay_dlq(envelope.id)
            .await
            .expect("idempotent replay"));
        assert_eq!(
            bus.status(envelope.id).await.expect("status"),
            Some("pending".into())
        );
        let removed = bus
            .prune_dlq_before(Utc::now() + ChronoDuration::seconds(1))
            .await
            .expect("prune");
        assert_eq!(removed, 0, "replayed events are not DLQ rows");
    }

    #[tokio::test]
    async fn dlq_retention_prunes_only_expired_dlq_events() {
        let bus = bus().await.with_max_retries(1);
        let failing: Handler = Arc::new(move |_event, _last_seen| {
            Box::pin(async { Err(HandlerError("retention test".into())) })
        });
        let _subscription = bus.subscribe(failing).await.expect("subscribe");
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");
        eventually(|| async { bus.status(envelope.id).await.unwrap() == Some("dlq".into()) }).await;

        let deleted = bus
            .prune_dlq_before(Utc::now() + ChronoDuration::seconds(1))
            .await
            .expect("prune");
        assert_eq!(deleted, 1);
        assert_eq!(bus.status(envelope.id).await.expect("status"), None);
        assert_eq!(bus.dlq_count().await.expect("dlq count"), 0);
    }

    #[tokio::test]
    async fn refresh_metrics_reconciles_durable_queue_and_dlq_state() {
        let bus = bus().await;
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");

        let old_created_at = (Utc::now() - ChronoDuration::seconds(3)).to_rfc3339();
        sqlx::query(
            "UPDATE outbox SET status = 'pending', created_at = ?, updated_at = ? WHERE event_id = ?",
        )
        .bind(&old_created_at)
        .bind(&old_created_at)
        .bind(envelope.id.to_string())
        .execute(&bus.db)
        .await
        .expect("age durable row");

        let pending = bus.refresh_metrics().await.expect("refresh pending");
        assert_eq!(pending.queue_depth, 1);
        assert_eq!(pending.dlq_depth, 0);
        assert!(pending.oldest_event_age_ms >= 2_000);

        sqlx::query("UPDATE outbox SET status = 'dlq', updated_at = ? WHERE event_id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(envelope.id.to_string())
            .execute(&bus.db)
            .await
            .expect("move durable row to dlq");

        let dlq = bus.refresh_metrics().await.expect("refresh dlq");
        assert_eq!(dlq.queue_depth, 0);
        assert_eq!(dlq.dlq_depth, 1);
        assert_eq!(dlq.oldest_event_age_ms, 0);
    }

    #[tokio::test]
    async fn stale_in_progress_event_is_reclaimed() {
        let bus = bus().await.with_lease_timeout(Duration::from_millis(1));
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");
        sqlx::query("UPDATE outbox SET status = 'in_progress', updated_at = ? WHERE event_id = ?")
            .bind((Utc::now() - ChronoDuration::seconds(5)).to_rfc3339())
            .bind(envelope.id.to_string())
            .execute(&bus.db)
            .await
            .expect("mark stale");
        let seen = Arc::new(AtomicUsize::new(0));
        let handler_seen = seen.clone();
        let handler: Handler = Arc::new(move |_event, _last_seen| {
            let handler_seen = handler_seen.clone();
            Box::pin(async move {
                handler_seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });
        let _subscription = bus.subscribe(handler).await.expect("subscribe");
        eventually(|| async { bus.status(envelope.id).await.unwrap() == Some("acked".into()) })
            .await;
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn late_worker_completion_cannot_overwrite_newer_terminal_state() {
        let bus = bus().await;
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");
        let claimed = bus
            .claim_next()
            .await
            .expect("claim")
            .expect("claimed event");
        sqlx::query("UPDATE outbox SET status = 'acked' WHERE event_id = ?")
            .bind(envelope.id.to_string())
            .execute(&bus.db)
            .await
            .expect("mark newer completion");

        assert!(!bus
            .mark_failed(envelope.id, &claimed.claim_token, "late failure".into())
            .await
            .expect("late failure"));
        assert!(!bus
            .mark_handled(envelope.id, &claimed.claim_token)
            .await
            .expect("late success"));

        assert_eq!(
            bus.status(envelope.id).await.expect("status"),
            Some("acked".into())
        );
        assert_eq!(bus.attempts(envelope.id).await.expect("attempts"), Some(0));
    }

    #[tokio::test]
    async fn stale_worker_cannot_finish_newer_claim() {
        let bus = bus().await.with_lease_timeout(Duration::from_millis(1));
        let envelope = event();
        bus.publish(envelope.clone()).await.expect("publish");

        let first = bus
            .claim_next()
            .await
            .expect("first claim")
            .expect("first claimed event");
        sqlx::query("UPDATE outbox SET updated_at = ? WHERE event_id = ?")
            .bind((Utc::now() - ChronoDuration::seconds(5)).to_rfc3339())
            .bind(envelope.id.to_string())
            .execute(&bus.db)
            .await
            .expect("expire first claim");
        let second = bus
            .claim_next()
            .await
            .expect("second claim")
            .expect("second claimed event");
        assert_ne!(first.claim_token, second.claim_token);

        assert!(!bus
            .mark_failed(envelope.id, &first.claim_token, "late failure".into())
            .await
            .expect("stale failure"));
        assert!(!bus
            .mark_handled(envelope.id, &first.claim_token)
            .await
            .expect("stale success"));
        assert_eq!(
            bus.status(envelope.id).await.expect("status"),
            Some("in_progress".into())
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM handled_events WHERE event_id = ?")
                .bind(envelope.id.to_string())
                .fetch_one(&bus.db)
                .await
                .expect("handled count"),
            0
        );

        assert!(bus
            .mark_handled(envelope.id, &second.claim_token)
            .await
            .expect("newer success"));
        assert_eq!(
            bus.status(envelope.id).await.expect("status"),
            Some("acked".into())
        );
    }

    #[tokio::test]
    async fn outbox_indexes_cover_claim_and_retention_paths() {
        let bus = bus().await;
        let indexes: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_outbox_%' ORDER BY name",
        )
        .fetch_all(&bus.db)
        .await
        .expect("indexes");

        assert_eq!(
            indexes,
            vec![
                "idx_outbox_claim_due".to_owned(),
                "idx_outbox_claim_lease".to_owned(),
                "idx_outbox_dlq_retention".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn migration_upgrades_legacy_outbox_schema() {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite pool");
        sqlx::query(
            "CREATE TABLE outbox (event_id TEXT PRIMARY KEY, envelope TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
        )
        .execute(&pool)
        .await
        .expect("legacy outbox");

        let _bus = SqliteBus::new(pool.clone()).await.expect("migrate");
        let has_claim_token: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('outbox') WHERE name = 'claim_token'",
        )
        .fetch_one(&pool)
        .await
        .expect("claim token column");
        assert_eq!(has_claim_token, 1);

        let index_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_outbox_%'",
        )
        .fetch_one(&pool)
        .await
        .expect("indexes");
        assert_eq!(index_count, 3);
    }

    #[tokio::test]
    async fn outbox_query_plans_use_status_time_indexes() {
        let bus = bus().await;
        let now = Utc::now().to_rfc3339();
        for index in 0..96 {
            let status = match index % 3 {
                0 => "pending",
                1 => "in_progress",
                _ => "dlq",
            };
            sqlx::query(
                "INSERT INTO outbox (event_id, envelope, status, attempts, next_attempt_at, created_at, updated_at) VALUES (?, '{}', ?, 0, ?, ?, ?)",
            )
            .bind(Uuid::now_v7().to_string())
            .bind(status)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .execute(&bus.db)
            .await
            .expect("seed outbox");
        }

        let due_details: Vec<String> = sqlx::query(
            "EXPLAIN QUERY PLAN SELECT event_id FROM outbox WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY created_at LIMIT 1",
        )
        .bind(&now)
        .fetch_all(&bus.db)
        .await
        .expect("due plan")
        .into_iter()
        .map(|row| row.get("detail"))
        .collect();
        assert!(
            due_details
                .iter()
                .any(|detail| detail.contains("idx_outbox_claim_due")),
            "due plan did not use claim index: {due_details:?}"
        );

        let lease_details: Vec<String> = sqlx::query(
            "EXPLAIN QUERY PLAN SELECT event_id FROM outbox WHERE status = 'in_progress' AND updated_at < ? ORDER BY created_at LIMIT 1",
        )
        .bind(&now)
        .fetch_all(&bus.db)
        .await
        .expect("lease plan")
        .into_iter()
        .map(|row| row.get("detail"))
        .collect();
        assert!(
            lease_details.iter().any(|detail| {
                detail.contains("idx_outbox_claim_lease")
                    || detail.contains("idx_outbox_dlq_retention")
            }),
            "lease plan did not use status/time index: {lease_details:?}"
        );

        let retention_details: Vec<String> = sqlx::query(
            "EXPLAIN QUERY PLAN DELETE FROM outbox WHERE status = 'dlq' AND updated_at < ?",
        )
        .bind(&now)
        .fetch_all(&bus.db)
        .await
        .expect("retention plan")
        .into_iter()
        .map(|row| row.get("detail"))
        .collect();
        assert!(
            retention_details
                .iter()
                .any(|detail| detail.contains("idx_outbox_dlq_retention")),
            "retention plan did not use DLQ index: {retention_details:?}"
        );
    }

    #[tokio::test]
    async fn file_backed_workers_claim_each_event_once() {
        let file = NamedTempFile::new().expect("sqlite file");
        let url = format!("sqlite://{}", file.path().display());
        let pool_a = file_pool(&url).await;
        let pool_b = file_pool(&url).await;
        let bus_a = SqliteBus::new(pool_a).await.expect("bus a");
        let bus_b = SqliteBus::new(pool_b).await.expect("bus b");
        let seen = Arc::new(AtomicUsize::new(0));
        let make_handler = |seen: Arc<AtomicUsize>| {
            Arc::new(move |_event: EventEnvelope, _last_seen: Option<Uuid>| {
                let seen = seen.clone();
                Box::pin(async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }) as super::HandlerFuture
            }) as Handler
        };
        let _sub_a = bus_a
            .subscribe(make_handler(seen.clone()))
            .await
            .expect("sub a");
        let _sub_b = bus_b
            .subscribe(make_handler(seen.clone()))
            .await
            .expect("sub b");
        let envelope = event();
        bus_a.publish(envelope.clone()).await.expect("publish");
        eventually_with_timeout(Duration::from_secs(30), || async {
            bus_a.status(envelope.id).await.unwrap() == Some("acked".into())
        })
        .await;
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn file_backed_workers_drain_1000_events_without_duplicates() {
        let file = NamedTempFile::new().expect("sqlite file");
        let url = format!("sqlite://{}", file.path().display());
        let pool_a = file_pool(&url).await;
        let pool_b = file_pool(&url).await;
        let bus_a = SqliteBus::new(pool_a).await.expect("bus a");
        let bus_b = SqliteBus::new(pool_b).await.expect("bus b");
        let seen = Arc::new(Mutex::new(HashSet::new()));
        let duplicates = Arc::new(AtomicUsize::new(0));
        let make_handler = |seen: Arc<Mutex<HashSet<Uuid>>>, duplicates: Arc<AtomicUsize>| {
            Arc::new(move |event: EventEnvelope, _last_seen: Option<Uuid>| {
                let seen = seen.clone();
                let duplicates = duplicates.clone();
                Box::pin(async move {
                    if !seen.lock().expect("seen").insert(event.id) {
                        duplicates.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(())
                }) as super::HandlerFuture
            }) as Handler
        };
        let started = Instant::now();
        timeout(Duration::from_secs(30), async {
            for _ in 0..1_000 {
                bus_a.publish(event()).await.expect("publish");
            }
        })
        .await
        .expect("publishing 1,000 events");

        let _sub_a = bus_a
            .subscribe(make_handler(seen.clone(), duplicates.clone()))
            .await
            .expect("sub a");
        let _sub_b = bus_b
            .subscribe(make_handler(seen.clone(), duplicates.clone()))
            .await
            .expect("sub b");

        // File-backed SQLite can be substantially slower on a busy Windows
        // host; keep the correctness bound finite while avoiding a false
        // timeout before the duplicate/pending invariants are checked.
        eventually_with_timeout(Duration::from_secs(180), || async {
            let count = seen.lock().expect("seen").len();
            count == 1_000 && bus_a.pending_count().await.unwrap() == 0
        })
        .await;

        assert_eq!(duplicates.load(Ordering::SeqCst), 0);
        assert_eq!(seen.lock().expect("seen").len(), 1_000);
        assert!(started.elapsed() < Duration::from_secs(180));
    }
}
