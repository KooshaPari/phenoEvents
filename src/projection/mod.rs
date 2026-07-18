use crate::{
    core::EventEnvelope,
    observability::{metrics, trace_envelope},
};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use sqlx::{Pool, Row, Sqlite};
use tracing::Instrument;

#[derive(Debug, thiserror::Error)]
pub enum ProjectionError {
    #[error("missing payload field: {0}")]
    MissingField(&'static str),
    #[error("invalid payload field: {0}")]
    InvalidField(&'static str),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("sqlite: {0}")]
    Sqlite(#[from] sqlx::Error),
}

#[async_trait]
pub trait Projection {
    async fn apply(&mut self, envelope: EventEnvelope) -> Result<(), ProjectionError>;
}

#[derive(Debug, Clone)]
pub struct SqlProjection {
    pub pool: Pool<Sqlite>,
    pub name: String,
    pub offset: i64,
}

impl SqlProjection {
    pub async fn new(pool: Pool<Sqlite>, name: impl Into<String>) -> Result<Self, ProjectionError> {
        migrate_checkpoint(&pool).await?;
        let name = name.into();
        let offset = checkpoint(&pool, &name).await?;
        Ok(Self { pool, name, offset })
    }

    async fn checkpoint(&mut self, offset: i64) -> Result<(), ProjectionError> {
        checkpoint_projection(&self.pool, &self.name, offset).await?;
        self.offset = offset;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OrderProjection {
    inner: SqlProjection,
}

impl OrderProjection {
    pub async fn new(pool: Pool<Sqlite>) -> Result<Self, ProjectionError> {
        migrate_orders(&pool).await?;
        Ok(Self {
            inner: SqlProjection::new(pool, "orders").await?,
        })
    }

    pub fn offset(&self) -> i64 {
        self.inner.offset
    }

    pub async fn rebuild_from_offset(&mut self, offset: i64) -> Result<(), ProjectionError> {
        let rows = sqlx::query(
            r#"
            SELECT rowid AS offset, envelope
            FROM outbox
            WHERE rowid > ?
            ORDER BY rowid
            "#,
        )
        .bind(offset)
        .fetch_all(&self.inner.pool)
        .await?;

        for row in rows {
            let next_offset: i64 = row.get("offset");
            let envelope_json: String = row.get("envelope");
            let envelope = serde_json::from_str(&envelope_json)?;
            self.apply_at_offset(envelope, next_offset).await?;
        }

        Ok(())
    }

    async fn apply_at_offset(
        &mut self,
        envelope: EventEnvelope,
        offset: i64,
    ) -> Result<(), ProjectionError> {
        let span = trace_envelope(&envelope);
        let result = async {
            apply_order_event(&self.inner.pool, envelope).await?;
            self.inner.checkpoint(offset).await
        }
        .instrument(span)
        .await;

        match result {
            Ok(()) => {
                let (_, events_processed, _) = metrics();
                events_processed.increment();
                Ok(())
            }
            Err(err) => {
                let (_, _, events_failed) = metrics();
                events_failed.increment();
                Err(err)
            }
        }
    }
}

#[async_trait]
impl Projection for OrderProjection {
    async fn apply(&mut self, envelope: EventEnvelope) -> Result<(), ProjectionError> {
        let next_offset = self.inner.offset + 1;
        self.apply_at_offset(envelope, next_offset).await
    }
}

async fn migrate_checkpoint(pool: &Pool<Sqlite>) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS projection_checkpoints (
            name TEXT PRIMARY KEY,
            offset INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn migrate_orders(pool: &Pool<Sqlite>) -> Result<(), sqlx::Error> {
    migrate_checkpoint(pool).await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            status TEXT NOT NULL,
            total_cents INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS order_items (
            order_id TEXT NOT NULL,
            sku TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price_cents INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (order_id, sku)
        );
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn checkpoint(pool: &Pool<Sqlite>, name: &str) -> Result<i64, sqlx::Error> {
    let offset = sqlx::query_scalar("SELECT offset FROM projection_checkpoints WHERE name = ?")
        .bind(name)
        .fetch_optional(pool)
        .await?;
    Ok(offset.unwrap_or(0))
}

async fn checkpoint_projection(
    pool: &Pool<Sqlite>,
    name: &str,
    offset: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO projection_checkpoints (name, offset, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET offset = excluded.offset, updated_at = excluded.updated_at
        "#,
    )
    .bind(name)
    .bind(offset)
    .bind(Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

async fn apply_order_event(
    pool: &Pool<Sqlite>,
    envelope: EventEnvelope,
) -> Result<(), ProjectionError> {
    match envelope.event_type.as_str() {
        "order.created" => upsert_order(pool, &envelope.payload).await,
        "order.item_added" => upsert_item(pool, &envelope.payload).await,
        "order.cancelled" => set_order_status(pool, &envelope.payload, "cancelled").await,
        _ => Ok(()),
    }
}

async fn upsert_order(pool: &Pool<Sqlite>, payload: &Value) -> Result<(), ProjectionError> {
    let order_id = required_str(payload, "order_id")?;
    let customer_id = required_str(payload, "customer_id")?;
    let status = payload
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("open");

    sqlx::query(
        r#"
        INSERT INTO orders (order_id, customer_id, status, total_cents, updated_at)
        VALUES (?, ?, ?, 0, ?)
        ON CONFLICT(order_id) DO UPDATE SET
            customer_id = excluded.customer_id,
            status = excluded.status,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(order_id)
    .bind(customer_id)
    .bind(status)
    .bind(Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_item(pool: &Pool<Sqlite>, payload: &Value) -> Result<(), ProjectionError> {
    let order_id = required_str(payload, "order_id")?;
    let sku = required_str(payload, "sku")?;
    let quantity = required_i64(payload, "quantity")?;
    let unit_price_cents = required_i64(payload, "unit_price_cents")?;
    let updated_at = Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    sqlx::query(
        r#"
        INSERT INTO order_items (order_id, sku, quantity, unit_price_cents, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(order_id, sku) DO UPDATE SET
            quantity = excluded.quantity,
            unit_price_cents = excluded.unit_price_cents,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(order_id)
    .bind(sku)
    .bind(quantity)
    .bind(unit_price_cents)
    .bind(&updated_at)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE orders
        SET total_cents = (
            SELECT COALESCE(SUM(quantity * unit_price_cents), 0)
            FROM order_items
            WHERE order_id = ?
        ),
        updated_at = ?
        WHERE order_id = ?
        "#,
    )
    .bind(order_id)
    .bind(updated_at)
    .bind(order_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

async fn set_order_status(
    pool: &Pool<Sqlite>,
    payload: &Value,
    status: &str,
) -> Result<(), ProjectionError> {
    let order_id = required_str(payload, "order_id")?;
    sqlx::query("UPDATE orders SET status = ?, updated_at = ? WHERE order_id = ?")
        .bind(status)
        .bind(Utc::now().to_rfc3339())
        .bind(order_id)
        .execute(pool)
        .await?;
    Ok(())
}

fn required_str<'a>(payload: &'a Value, field: &'static str) -> Result<&'a str, ProjectionError> {
    payload
        .get(field)
        .ok_or(ProjectionError::MissingField(field))?
        .as_str()
        .ok_or(ProjectionError::InvalidField(field))
}

fn required_i64(payload: &Value, field: &'static str) -> Result<i64, ProjectionError> {
    payload
        .get(field)
        .ok_or(ProjectionError::MissingField(field))?
        .as_i64()
        .ok_or(ProjectionError::InvalidField(field))
}

#[cfg(test)]
mod tests {
    use super::{OrderProjection, Projection};
    use crate::{bus::Bus, bus::SqliteBus, core::EventEnvelope};
    use serde_json::json;
    use sqlx::{Pool, Sqlite, SqlitePool};

    async fn pool() -> Pool<Sqlite> {
        SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite pool")
    }

    fn event(event_type: &str, payload: serde_json::Value) -> EventEnvelope {
        EventEnvelope::builder(event_type, "orders", payload)
            .build()
            .expect("event")
    }

    fn order_created() -> EventEnvelope {
        event(
            "order.created",
            json!({"order_id": "ord_1", "customer_id": "cust_1"}),
        )
    }

    fn item_added(sku: &str, quantity: i64, unit_price_cents: i64) -> EventEnvelope {
        event(
            "order.item_added",
            json!({
                "order_id": "ord_1",
                "sku": sku,
                "quantity": quantity,
                "unit_price_cents": unit_price_cents
            }),
        )
    }

    #[tokio::test]
    async fn applies_three_events_to_order_read_model() {
        let pool = pool().await;
        let mut projection = OrderProjection::new(pool.clone())
            .await
            .expect("projection");

        projection.apply(order_created()).await.expect("created");
        projection
            .apply(item_added("sku_1", 2, 500))
            .await
            .expect("item 1");
        projection
            .apply(item_added("sku_2", 1, 300))
            .await
            .expect("item 2");

        let total: i64 = sqlx::query_scalar("SELECT total_cents FROM orders WHERE order_id = ?")
            .bind("ord_1")
            .fetch_one(&pool)
            .await
            .expect("total");
        let items: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM order_items WHERE order_id = ?")
            .bind("ord_1")
            .fetch_one(&pool)
            .await
            .expect("items");

        assert_eq!(total, 1300);
        assert_eq!(items, 2);
        assert_eq!(projection.offset(), 3);
    }

    #[tokio::test]
    async fn rebuilds_from_outbox_offset_zero() {
        let pool = pool().await;
        let bus = SqliteBus::new(pool.clone()).await.expect("bus");
        bus.publish(order_created()).await.expect("created");
        bus.publish(item_added("sku_1", 2, 500))
            .await
            .expect("item 1");
        bus.publish(item_added("sku_2", 1, 300))
            .await
            .expect("item 2");
        let mut projection = OrderProjection::new(pool.clone())
            .await
            .expect("projection");

        projection.rebuild_from_offset(0).await.expect("rebuild");

        let total: i64 = sqlx::query_scalar("SELECT total_cents FROM orders WHERE order_id = ?")
            .bind("ord_1")
            .fetch_one(&pool)
            .await
            .expect("total");
        assert_eq!(total, 1300);
        assert_eq!(projection.offset(), 3);
    }

    #[tokio::test]
    async fn applying_same_events_is_idempotent() {
        let pool = pool().await;
        let mut projection = OrderProjection::new(pool.clone())
            .await
            .expect("projection");
        let created = order_created();
        let item = item_added("sku_1", 2, 500);

        projection.apply(created.clone()).await.expect("created");
        projection.apply(item.clone()).await.expect("item");
        projection.apply(created).await.expect("created duplicate");
        projection.apply(item).await.expect("item duplicate");

        let total: i64 = sqlx::query_scalar("SELECT total_cents FROM orders WHERE order_id = ?")
            .bind("ord_1")
            .fetch_one(&pool)
            .await
            .expect("total");
        let items: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM order_items WHERE order_id = ?")
            .bind("ord_1")
            .fetch_one(&pool)
            .await
            .expect("items");

        assert_eq!(total, 1000);
        assert_eq!(items, 1);
    }
}
