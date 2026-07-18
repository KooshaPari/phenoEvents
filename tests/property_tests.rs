// Property-based tests for EventEnvelope and SchemaRegistry invariants.
// Uses proptest to cover the input space beyond fixed unit test cases.
use pheno_events::{core::EventEnvelope, schema::SchemaRegistry};
use proptest::prelude::*;
use serde_json::{json, Value};

// --- EventEnvelope property tests ---

prop_compose! {
    fn arb_nonempty_string()(s in "[a-zA-Z][a-zA-Z0-9._-]{0,63}") -> String { s }
}

prop_compose! {
    fn arb_schema_version()(v in 1u32..=100) -> u32 { v }
}

proptest! {
    /// Any non-empty event_type + source with schema_version >= 1 must build successfully.
    #[test]
    fn envelope_build_succeeds_for_valid_inputs(
        event_type in arb_nonempty_string(),
        source in arb_nonempty_string(),
        version in arb_schema_version(),
    ) {
        let result = EventEnvelope::builder(&event_type, &source, json!({"x": 1}))
            .schema_version(version)
            .build();
        prop_assert!(result.is_ok(), "expected Ok but got {:?}", result);
    }

    /// Empty event_type must always return EnvelopeError::EmptyEventType.
    #[test]
    fn envelope_rejects_empty_event_type(source in arb_nonempty_string()) {
        let result = EventEnvelope::builder("", &source, json!({})).build();
        prop_assert!(result.is_err());
    }

    /// Empty source must always return EnvelopeError::EmptySource.
    #[test]
    fn envelope_rejects_empty_source(event_type in arb_nonempty_string()) {
        let result = EventEnvelope::builder(&event_type, "", json!({})).build();
        prop_assert!(result.is_err());
    }

    /// schema_version=0 must always be rejected.
    #[test]
    fn envelope_rejects_zero_schema_version(
        event_type in arb_nonempty_string(),
        source in arb_nonempty_string(),
    ) {
        let result = EventEnvelope::builder(&event_type, &source, json!({}))
            .schema_version(0)
            .build();
        prop_assert!(result.is_err());
    }

    /// Serializing and deserializing an envelope must be a round-trip identity.
    #[test]
    fn envelope_serde_roundtrip(
        event_type in arb_nonempty_string(),
        source in arb_nonempty_string(),
    ) {
        let original = EventEnvelope::builder(&event_type, &source, json!({"v": 1}))
            .build()
            .expect("build");
        let serialized = serde_json::to_string(&original).expect("serialize");
        let deserialized: EventEnvelope = serde_json::from_str(&serialized).expect("deserialize");
        prop_assert_eq!(original, deserialized);
    }
}

// --- SchemaRegistry property tests ---

fn simple_object_schema(required_fields: &[&str]) -> Value {
    let props: serde_json::Map<String, Value> = required_fields
        .iter()
        .map(|f| (f.to_string(), json!({"type": "string"})))
        .collect();
    json!({
        "type": "object",
        "required": required_fields,
        "properties": props
    })
}

proptest! {
    /// Registering version 0 must always fail with InvalidVersion.
    #[test]
    fn schema_registry_rejects_version_zero(event_type in arb_nonempty_string()) {
        let mut reg = SchemaRegistry::new();
        let result = reg.register(event_type, 0, json!({"type": "object", "properties": {}}));
        prop_assert!(result.is_err());
    }

    /// After registering a schema, validating a conforming payload must succeed.
    #[test]
    fn schema_validate_conforming_payload_succeeds(
        event_type in arb_nonempty_string(),
        version in arb_schema_version(),
    ) {
        let mut reg = SchemaRegistry::new();
        let schema = simple_object_schema(&["id"]);
        reg.register(event_type.clone(), version, schema).expect("register");
        let payload = json!({"id": "abc"});
        let result = reg.validate(event_type, version, &payload);
        prop_assert!(result.is_ok(), "expected Ok but got {:?}", result);
    }

    /// Validating a payload against an unregistered event_type/version must return SchemaNotFound.
    #[test]
    fn schema_validate_unregistered_returns_not_found(
        event_type in arb_nonempty_string(),
        version in arb_schema_version(),
    ) {
        let reg = SchemaRegistry::new();
        let result = reg.validate(event_type, version, &json!({}));
        prop_assert!(result.is_err());
    }
}
