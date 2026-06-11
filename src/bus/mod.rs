use crate::core::EventEnvelope;
use crate::observability::{metrics, trace_envelope};
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
}

impl SqliteBus {
    pub async fn new(db: Pool<Sqlite>) -> Result<Self, sqlx::Error> {
        let bus = Self {
            db,
            max_retries: 3,
            poll_interval: StdDuration::from_millis(25),
        };
        bus.migrate().await?;
        Ok(bus)
    }

    pub fn with_max_retries(mut self, max_retries: i64) -> Self {
        self.max_retries = max_retries;
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
                updated_at TEXT NOT NULL
            );
            "#,
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

    async fn claim_next(&self) -> Result<Option<EventEnvelope>, sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.db.begin().await?;
        let row = sqlx::query(
            r#"
            SELECT event_id, envelope
            FROM outbox
            WHERE status IN ('pending', 'retrying')
              AND next_attempt_at <= ?
            ORDER BY created_at
            LIMIT 1
            "#,
        )
        .bind(&now)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };

        let event_id: String = row.get("event_id");
        let envelope_json: String = row.get("envelope");
        let changed = sqlx::query(
            r#"
            UPDATE outbox
            SET status = 'in_progress', updated_at = ?
            WHERE event_id = ? AND status IN ('pending', 'retrying')
            "#,
        )
        .bind(&now)
        .bind(event_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        tx.commit().await?;
        if changed == 0 {
            return Ok(None);
        }

        serde_json::from_str(&envelope_json)
            .map(Some)
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

    async fn mark_handled(&self, event_id: Uuid) -> Result<(), sqlx::Error> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.db.begin().await?;
        sqlx::query("INSERT OR IGNORE INTO handled_events (event_id, handled_at) VALUES (?, ?)")
            .bind(event_id.to_string())
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE outbox SET status = 'acked', updated_at = ? WHERE event_id = ?")
            .bind(now)
            .bind(event_id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }

    async fn mark_failed(&self, event_id: Uuid, error: String) -> Result<(), sqlx::Error> {
        let now = Utc::now();
        let attempts: i64 = sqlx::query_scalar("SELECT attempts FROM outbox WHERE event_id = ?")
            .bind(event_id.to_string())
            .fetch_one(&self.db)
            .await?;
        let next_attempts = attempts + 1;
        let status = if next_attempts >= self.max_retries {
            "dlq"
        } else {
            "retrying"
        };
        let next = (now + Duration::milliseconds(10)).to_rfc3339();
        sqlx::query(
            r#"
            UPDATE outbox
            SET status = ?, attempts = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
            WHERE event_id = ?
            "#,
        )
        .bind(status)
        .bind(next_attempts)
        .bind(next)
        .bind(error)
        .bind(now.to_rfc3339())
        .bind(event_id.to_string())
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn process_once(&self, handler: &Handler) -> Result<bool, sqlx::Error> {
        let Some(envelope) = self.claim_next().await? else {
            return Ok(false);
        };

        if self.already_handled(envelope.id).await? {
            self.mark_handled(envelope.id).await?;
            let (_, events_processed, _) = metrics();
            events_processed.increment();
            return Ok(true);
        }

        let span = trace_envelope(&envelope);
        async {
            let last_seen = self.last_seen().await?;
            match handler(envelope.clone(), last_seen).await {
                Ok(()) => {
                    self.mark_handled(envelope.id).await?;
                    let (_, events_processed, _) = metrics();
                    events_processed.increment();
                }
                Err(err) => {
                    self.mark_failed(envelope.id, err.to_string()).await?;
                    let (_, _, events_failed) = metrics();
                    events_failed.increment();
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

    pub async fn attempts(&self, event_id: Uuid) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT attempts FROM outbox WHERE event_id = ?")
            .bind(event_id.to_string())
            .fetch_one(&self.db)
            .await
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
        let result = sqlx::query(
            r#"
            INSERT OR IGNORE INTO outbox
                (event_id, envelope, status, attempts, next_attempt_at, created_at, updated_at)
            VALUES (?, ?, 'pending', 0, ?, ?, ?)
            "#,
        )
        .bind(event_id.to_string())
        .bind(envelope_json)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .execute(&self.db)
        .await?;
        let (events_published, _, _) = metrics();
        events_published.increment();

        Ok(Ack {
            event_id,
            duplicate: result.rows_affected() == 0,
        })
    }

    async fn subscribe(&self, handler: Handler) -> Result<Subscription, SubscribeError> {
        self.migrate().await?;
        let bus = self.clone();
        let worker = tokio::spawn(async move {
            loop {
                match bus.process_once(&handler).await {
                    Ok(true) => {}
                    Ok(false) | Err(_) => time::sleep(bus.poll_interval).await,
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
    use serde_json::json;
    use sqlx::SqlitePool;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use tokio::time::{sleep, timeout, Duration};
    use uuid::Uuid;

    async fn bus() -> SqliteBus {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite pool");
        SqliteBus::new(pool).await.expect("bus")
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
        timeout(Duration::from_secs(2), async {
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
        assert_eq!(bus.attempts(envelope.id).await.expect("attempts"), 1);
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
        assert_eq!(bus.attempts(envelope.id).await.expect("attempts"), 2);
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
}
