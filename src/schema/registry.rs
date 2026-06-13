use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

#[derive(Debug, Default)]
pub struct SchemaRegistry {
    schemas: HashMap<(String, u32), Value>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("schema version must be at least 1")]
    InvalidVersion,
    #[error("schema not found for event type {event_type} version {version}")]
    SchemaNotFound { event_type: String, version: u32 },
    #[error("invalid JSON schema: {0}")]
    InvalidSchema(String),
    #[error("payload failed schema validation: {0}")]
    PayloadInvalid(String),
    #[error("breaking schema change rejected: {0}")]
    BreakingChange(String),
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        event_type: String,
        version: u32,
        schema: Value,
    ) -> Result<(), ValidationError> {
        if version == 0 {
            return Err(ValidationError::InvalidVersion);
        }

        validate_schema_shape(&schema)?;

        if let Some(previous) = self.latest_schema_for(&event_type, version) {
            enforce_additive_policy(previous, &schema)?;
        }

        self.schemas.insert((event_type, version), schema);
        Ok(())
    }

    pub fn validate(
        &self,
        event_type: String,
        version: u32,
        payload: &Value,
    ) -> Result<(), ValidationError> {
        if version == 0 {
            return Err(ValidationError::InvalidVersion);
        }

        let schema = self.schemas.get(&(event_type.clone(), version)).ok_or(
            ValidationError::SchemaNotFound {
                event_type,
                version,
            },
        )?;
        validate_against_schema(schema, payload).map_err(ValidationError::PayloadInvalid)
    }

    fn latest_schema_for(&self, event_type: &str, version: u32) -> Option<&Value> {
        self.schemas
            .iter()
            .filter(|((candidate_type, candidate_version), _)| {
                candidate_type == event_type && *candidate_version < version
            })
            .max_by_key(|((_, candidate_version), _)| *candidate_version)
            .map(|(_, schema)| schema)
    }
}

fn enforce_additive_policy(previous: &Value, next: &Value) -> Result<(), ValidationError> {
    let previous_properties = object_properties(previous);
    let next_properties = object_properties(next);

    for (name, previous_property) in previous_properties {
        let next_property = next_properties.get(name).ok_or_else(|| {
            ValidationError::BreakingChange(format!("property '{name}' was removed"))
        })?;

        if *next_property != previous_property {
            return Err(ValidationError::BreakingChange(format!(
                "property '{name}' changed"
            )));
        }
    }

    let previous_required = required_fields(previous);
    let next_required = required_fields(next);
    if let Some(required) = next_required.difference(&previous_required).next() {
        return Err(ValidationError::BreakingChange(format!(
            "new required property '{required}' is not additive"
        )));
    }

    Ok(())
}

fn validate_schema_shape(schema: &Value) -> Result<(), ValidationError> {
    if !schema.is_object() {
        return Err(ValidationError::InvalidSchema(
            "schema must be a JSON object".to_string(),
        ));
    }

    if let Some(properties) = schema.get("properties") {
        if !properties.is_object() {
            return Err(ValidationError::InvalidSchema(
                "properties must be an object".to_string(),
            ));
        }
    }

    if let Some(required) = schema.get("required") {
        let Some(required) = required.as_array() else {
            return Err(ValidationError::InvalidSchema(
                "required must be an array".to_string(),
            ));
        };

        if required.iter().any(|field| !field.is_string()) {
            return Err(ValidationError::InvalidSchema(
                "required entries must be strings".to_string(),
            ));
        }
    }

    Ok(())
}

fn validate_against_schema(schema: &Value, payload: &Value) -> Result<(), String> {
    if let Some(schema_type) = schema.get("type").and_then(Value::as_str) {
        validate_type(schema_type, payload, "$")?;
    }

    let Some(payload_object) = payload.as_object() else {
        return Ok(());
    };

    validate_required(schema, payload_object)?;
    validate_properties(schema, payload_object)?;
    validate_additional_properties(schema, payload_object)
}

fn validate_required(schema: &Value, payload: &Map<String, Value>) -> Result<(), String> {
    for field in required_fields(schema) {
        if !payload.contains_key(field) {
            return Err(format!("missing required property '{field}'"));
        }
    }

    Ok(())
}

fn validate_properties(schema: &Value, payload: &Map<String, Value>) -> Result<(), String> {
    for (field, field_schema) in object_properties(schema) {
        let Some(value) = payload.get(field) else {
            continue;
        };

        if let Some(field_type) = field_schema.get("type").and_then(Value::as_str) {
            validate_type(field_type, value, field)?;
        }
    }

    Ok(())
}

fn validate_additional_properties(
    schema: &Value,
    payload: &Map<String, Value>,
) -> Result<(), String> {
    if schema
        .get("additionalProperties")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return Ok(());
    }

    let properties = object_properties(schema);
    for field in payload.keys() {
        if !properties.contains_key(field.as_str()) {
            return Err(format!("additional property '{field}' is not allowed"));
        }
    }

    Ok(())
}

fn validate_type(expected: &str, value: &Value, path: &str) -> Result<(), String> {
    let matches = match expected {
        "array" => value.is_array(),
        "boolean" => value.is_boolean(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "null" => value.is_null(),
        "number" => value.is_number(),
        "object" => value.is_object(),
        "string" => value.is_string(),
        other => return Err(format!("unsupported schema type '{other}' at {path}")),
    };

    if matches {
        Ok(())
    } else {
        Err(format!("expected {path} to be {expected}"))
    }
}

fn object_properties(schema: &Value) -> HashMap<&str, &Value> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| {
            properties
                .iter()
                .map(|(name, value)| (name.as_str(), value))
                .collect()
        })
        .unwrap_or_default()
}

fn required_fields(schema: &Value) -> HashSet<&str> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|required| required.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{SchemaRegistry, ValidationError};
    use serde_json::json;

    fn user_created_schema() -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "email": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    #[test]
    fn registers_schema() {
        let mut registry = SchemaRegistry::new();

        let result = registry.register("user.created".to_string(), 1, user_created_schema());

        assert!(result.is_ok());
    }

    #[test]
    fn validates_matching_payload() {
        let mut registry = SchemaRegistry::new();
        registry
            .register("user.created".to_string(), 1, user_created_schema())
            .expect("schema should register");

        let result = registry.validate(
            "user.created".to_string(),
            1,
            &json!({ "id": "user_1", "email": "a@example.com" }),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn rejects_broken_payload() {
        let mut registry = SchemaRegistry::new();
        registry
            .register("user.created".to_string(), 1, user_created_schema())
            .expect("schema should register");

        let error = registry
            .validate("user.created".to_string(), 1, &json!({ "email": 10 }))
            .expect_err("payload should fail validation");

        assert!(matches!(error, ValidationError::PayloadInvalid(_)));
    }

    #[test]
    fn rejects_version_mismatch() {
        let mut registry = SchemaRegistry::new();
        registry
            .register("user.created".to_string(), 1, user_created_schema())
            .expect("schema should register");

        let error = registry
            .validate("user.created".to_string(), 2, &json!({ "id": "user_1" }))
            .expect_err("unregistered version should fail");

        assert_eq!(
            error,
            ValidationError::SchemaNotFound {
                event_type: "user.created".to_string(),
                version: 2
            }
        );
    }
}
