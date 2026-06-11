use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::{NoContext, Timestamp, Uuid};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: Uuid,
    pub event_type: String,
    pub source: String,
    pub timestamp: DateTime<Utc>,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub schema_version: u32,
    pub payload: Value,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EnvelopeError {
    #[error("event_type must not be empty")]
    EmptyEventType,
    #[error("source must not be empty")]
    EmptySource,
    #[error("schema_version must be at least 1")]
    InvalidSchemaVersion,
}

impl EventEnvelope {
    pub fn new(
        event_type: impl Into<String>,
        source: impl Into<String>,
        schema_version: u32,
        payload: Value,
    ) -> Result<Self, EnvelopeError> {
        Self::builder(event_type, source, payload)
            .schema_version(schema_version)
            .build()
    }

    pub fn builder(
        event_type: impl Into<String>,
        source: impl Into<String>,
        payload: Value,
    ) -> EventEnvelopeBuilder {
        EventEnvelopeBuilder {
            event_type: event_type.into(),
            source: source.into(),
            payload,
            causation_id: None,
            correlation_id: None,
            schema_version: 1,
        }
    }

    pub fn validate(&self) -> Result<(), EnvelopeError> {
        validate_event_type(&self.event_type)?;
        validate_source(&self.source)?;
        validate_schema_version(self.schema_version)
    }
}

#[derive(Debug, Clone)]
pub struct EventEnvelopeBuilder {
    event_type: String,
    source: String,
    payload: Value,
    causation_id: Option<Uuid>,
    correlation_id: Option<Uuid>,
    schema_version: u32,
}

impl EventEnvelopeBuilder {
    pub fn causation_id(mut self, id: Uuid) -> Self {
        self.causation_id = Some(id);
        self
    }

    pub fn correlation_id(mut self, id: Uuid) -> Self {
        self.correlation_id = Some(id);
        self
    }

    pub fn schema_version(mut self, schema_version: u32) -> Self {
        self.schema_version = schema_version;
        self
    }

    pub fn build(self) -> Result<EventEnvelope, EnvelopeError> {
        validate_event_type(&self.event_type)?;
        validate_source(&self.source)?;
        validate_schema_version(self.schema_version)?;

        Ok(EventEnvelope {
            id: new_uuid_v7(),
            event_type: self.event_type,
            source: self.source,
            timestamp: Utc::now(),
            causation_id: self.causation_id,
            correlation_id: self.correlation_id,
            schema_version: self.schema_version,
            payload: self.payload,
        })
    }
}

fn validate_event_type(event_type: &str) -> Result<(), EnvelopeError> {
    if event_type.trim().is_empty() {
        return Err(EnvelopeError::EmptyEventType);
    }

    Ok(())
}

fn validate_source(source: &str) -> Result<(), EnvelopeError> {
    if source.trim().is_empty() {
        return Err(EnvelopeError::EmptySource);
    }

    Ok(())
}

fn validate_schema_version(schema_version: u32) -> Result<(), EnvelopeError> {
    if schema_version == 0 {
        return Err(EnvelopeError::InvalidSchemaVersion);
    }

    Ok(())
}

fn new_uuid_v7() -> Uuid {
    Uuid::new_v7(Timestamp::now(NoContext))
}

#[cfg(test)]
mod tests {
    use super::{EnvelopeError, EventEnvelope};
    use serde_json::json;
    use uuid::Version;

    #[test]
    fn builder_creates_valid_envelope_with_v7_id() {
        let causation_id = uuid::Uuid::now_v7();
        let correlation_id = uuid::Uuid::now_v7();

        let envelope = EventEnvelope::builder("user.created", "accounts", json!({"id": 1}))
            .causation_id(causation_id)
            .correlation_id(correlation_id)
            .schema_version(2)
            .build()
            .expect("valid envelope");

        assert_eq!(envelope.id.get_version(), Some(Version::SortRand));
        assert_eq!(envelope.event_type, "user.created");
        assert_eq!(envelope.source, "accounts");
        assert_eq!(envelope.causation_id, Some(causation_id));
        assert_eq!(envelope.correlation_id, Some(correlation_id));
        assert_eq!(envelope.schema_version, 2);
        assert_eq!(envelope.payload, json!({"id": 1}));
        assert!(envelope.validate().is_ok());
    }

    #[test]
    fn defaults_schema_version_to_one() {
        let envelope = EventEnvelope::builder("user.created", "accounts", json!({}))
            .build()
            .expect("valid envelope");

        assert_eq!(envelope.schema_version, 1);
    }

    #[test]
    fn rejects_empty_event_type() {
        let error = EventEnvelope::builder(" ", "accounts", json!({}))
            .build()
            .expect_err("empty event type should fail");

        assert_eq!(error, EnvelopeError::EmptyEventType);
    }

    #[test]
    fn rejects_empty_source() {
        let error = EventEnvelope::builder("user.created", "\t", json!({}))
            .build()
            .expect_err("empty source should fail");

        assert_eq!(error, EnvelopeError::EmptySource);
    }

    #[test]
    fn rejects_zero_schema_version() {
        let error = EventEnvelope::builder("user.created", "accounts", json!({}))
            .schema_version(0)
            .build()
            .expect_err("zero schema version should fail");

        assert_eq!(error, EnvelopeError::InvalidSchemaVersion);
    }
}
