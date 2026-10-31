//! Namespace metadata and attribute schemas.
//!
//! A namespace's schema maps attribute names to types. Types are inferred
//! from the first value written for an attribute and enforced afterwards;
//! full-text search must be declared explicitly.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::model::{AttributeValue, FieldType, MetadataFieldSpec, VECTOR_FIELD_NAME};
use crate::serde::collection_meta::DistanceMetric;

/// Attribute name holding a row's id in request and response JSON.
pub const ID_KEY: &str = "id";

/// Attribute every passage row of a document carries: the document id.
pub const DOC_ID_ATTRIBUTE: &str = "$doc_id";

/// Attribute every passage row of a document carries: its position.
pub const PASSAGE_ATTRIBUTE: &str = "$passage";

/// Value type of a namespace attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeType {
    #[serde(rename = "string")]
    String,
    #[serde(rename = "int")]
    Int,
    #[serde(rename = "float")]
    Float,
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "[]string")]
    StringArray,
    #[serde(rename = "[]int")]
    IntArray,
}

impl AttributeType {
    pub(crate) fn of(value: &AttributeValue) -> Option<Self> {
        Some(match value {
            AttributeValue::String(_) => AttributeType::String,
            AttributeValue::Int64(_) => AttributeType::Int,
            AttributeValue::Float64(_) => AttributeType::Float,
            AttributeValue::Bool(_) => AttributeType::Bool,
            AttributeValue::StringArray(_) => AttributeType::StringArray,
            AttributeValue::Int64Array(_) => AttributeType::IntArray,
            AttributeValue::Vector(_) => return None,
        })
    }
}

/// Schema of one attribute.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttributeSchema {
    #[serde(rename = "type")]
    pub attribute_type: AttributeType,
    /// Index the attribute for BM25 full-text search. Only for `string`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub full_text_search: bool,
    /// Embed the attribute into the vector of rows written without one.
    /// Only for `string`, and one attribute per namespace.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub embed: bool,
}

impl AttributeSchema {
    /// The schema of an attribute first seen in a write.
    pub(crate) fn inferred(attribute_type: AttributeType) -> Self {
        Self {
            attribute_type,
            full_text_search: false,
            embed: false,
        }
    }
}

/// Wire name of a distance metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Metric {
    #[serde(rename = "cosine_distance")]
    #[default]
    Cosine,
    #[serde(rename = "euclidean_squared")]
    EuclideanSquared,
    #[serde(rename = "dot_product")]
    DotProduct,
}

impl Metric {
    pub(crate) fn distance_metric(self) -> DistanceMetric {
        match self {
            Metric::Cosine => DistanceMetric::Cosine,
            Metric::EuclideanSquared => DistanceMetric::L2,
            Metric::DotProduct => DistanceMetric::DotProduct,
        }
    }

    /// Converts an engine score to the reported `$dist` (lower is closer):
    /// cosine distance `1 - cos`, squared euclidean distance, or the negated
    /// dot product.
    pub(crate) fn distance(self, score: f32) -> f32 {
        match self {
            // Cosine runs as squared L2 over unit vectors: |a-b|² = 2 - 2cos.
            Metric::Cosine => score / 2.0,
            Metric::EuclideanSquared => score,
            Metric::DotProduct => -score,
        }
    }
}

/// Persistent description of a namespace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamespaceMeta {
    pub name: String,
    /// Hex of the namespace's key id.
    pub id: String,
    /// Shard holding the namespace's data.
    pub shard: u32,
    pub dimensions: u16,
    pub distance_metric: Metric,
    #[serde(default)]
    pub schema: BTreeMap<String, AttributeSchema>,
    /// [`Embedder::model_id`](super::Embedder::model_id) of the model
    /// embedding the namespace, fixed when an attribute first embeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_model: Option<String>,
    pub created_at_ms: u64,
}

impl NamespaceMeta {
    /// Full-text fields, as the `metadata_fields` of the namespace's
    /// [`Config`](crate::Config).
    pub(crate) fn text_fields(&self) -> Vec<MetadataFieldSpec> {
        self.schema
            .iter()
            .filter(|(_, s)| s.full_text_search)
            .map(|(name, _)| MetadataFieldSpec::new(name.clone(), FieldType::Text, false))
            .collect()
    }

    /// The declared type of `attribute`, if known.
    pub(crate) fn attribute_type(&self, attribute: &str) -> Option<AttributeType> {
        match attribute {
            DOC_ID_ATTRIBUTE => Some(AttributeType::String),
            PASSAGE_ATTRIBUTE => Some(AttributeType::Int),
            _ => self.schema.get(attribute).map(|s| s.attribute_type),
        }
    }

    /// The attribute embedded into rows' vectors, if any.
    pub fn embed_attribute(&self) -> Option<&str> {
        self.schema
            .iter()
            .find(|(_, s)| s.embed)
            .map(|(name, _)| name.as_str())
    }

    /// Merges explicit schema entries, rejecting type changes. Returns
    /// whether the set of full-text fields changed.
    pub(crate) fn merge_schema(
        &mut self,
        updates: &BTreeMap<String, AttributeSchema>,
    ) -> Result<bool> {
        let mut text_changed = false;
        for (name, update) in updates {
            validate_attribute_name(name)?;
            for (flag, set) in [
                ("full_text_search", update.full_text_search),
                ("embed", update.embed),
            ] {
                if set && update.attribute_type != AttributeType::String {
                    return Err(Error::InvalidInput(format!(
                        "attribute '{name}': {flag} needs type string"
                    )));
                }
            }
            match self.schema.get(name) {
                Some(existing) if existing.attribute_type != update.attribute_type => {
                    return Err(Error::InvalidInput(format!(
                        "attribute '{name}' has type {:?}; cannot change it to {:?}",
                        existing.attribute_type, update.attribute_type
                    )));
                }
                Some(existing) => {
                    text_changed |= existing.full_text_search != update.full_text_search;
                }
                None => text_changed |= update.full_text_search,
            }
            self.schema.insert(name.clone(), update.clone());
        }
        let embedded: Vec<&String> = self
            .schema
            .iter()
            .filter(|(_, s)| s.embed)
            .map(|(name, _)| name)
            .collect();
        if embedded.len() > 1 {
            return Err(Error::InvalidInput(format!(
                "only one attribute can embed, but {embedded:?} do; set embed: false \
                 on all but one"
            )));
        }
        Ok(text_changed)
    }
}

pub(crate) fn validate_attribute_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 128 {
        return Err(Error::InvalidInput(format!(
            "attribute name '{name}' must be 1-128 bytes"
        )));
    }
    if name == ID_KEY || name == VECTOR_FIELD_NAME {
        return Err(Error::InvalidInput(format!(
            "attribute name '{name}' is reserved"
        )));
    }
    if name.starts_with('$') && name != DOC_ID_ATTRIBUTE && name != PASSAGE_ATTRIBUTE {
        return Err(Error::InvalidInput(format!(
            "attribute names starting with '$' are reserved: '{name}'"
        )));
    }
    Ok(())
}

/// Renders a JSON id (string or non-negative integer) as the stored id.
pub(crate) fn parse_id(value: &Value) -> Result<String> {
    let id = match value {
        Value::String(s) => s.clone(),
        Value::Number(n) if n.is_u64() => n.to_string(),
        other => {
            return Err(Error::InvalidInput(format!(
                "id must be a string or non-negative integer, got {other}"
            )));
        }
    };
    if id.is_empty() {
        return Err(Error::InvalidInput("id must not be empty".to_string()));
    }
    if id.len() > crate::db::MAX_EXTERNAL_ID_LEN {
        return Err(Error::InvalidInput(format!(
            "id too long: {} bytes (max {})",
            id.len(),
            crate::db::MAX_EXTERNAL_ID_LEN
        )));
    }
    Ok(id)
}

/// Parses a JSON vector of numbers.
pub(crate) fn parse_vector(value: &Value) -> Result<Vec<f32>> {
    let Value::Array(items) = value else {
        return Err(Error::InvalidInput(
            "vector must be an array of numbers".to_string(),
        ));
    };
    items
        .iter()
        .map(|v| {
            v.as_f64()
                .map(|f| f as f32)
                .ok_or_else(|| Error::InvalidInput("vector must be an array of numbers".into()))
        })
        .collect()
}

/// Converts a JSON attribute value to an attribute of `expected` type, or
/// infers the type when `expected` is `None`. `null` means "absent".
pub(crate) fn parse_attribute(
    name: &str,
    value: &Value,
    expected: Option<AttributeType>,
) -> Result<Option<AttributeValue>> {
    let mismatch =
        |want: &str| Error::InvalidInput(format!("attribute '{name}' must be {want}, got {value}"));
    if value.is_null() {
        return Ok(None);
    }
    let parsed = match expected {
        Some(AttributeType::String) => {
            AttributeValue::String(value.as_str().ok_or_else(|| mismatch("a string"))?.into())
        }
        Some(AttributeType::Int) => {
            AttributeValue::Int64(value.as_i64().ok_or_else(|| mismatch("an integer"))?)
        }
        Some(AttributeType::Float) => {
            AttributeValue::Float64(value.as_f64().ok_or_else(|| mismatch("a number"))?)
        }
        Some(AttributeType::Bool) => {
            AttributeValue::Bool(value.as_bool().ok_or_else(|| mismatch("a boolean"))?)
        }
        Some(AttributeType::StringArray) => AttributeValue::StringArray(
            value
                .as_array()
                .ok_or_else(|| mismatch("an array of strings"))?
                .iter()
                .map(|v| v.as_str().map(String::from))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| mismatch("an array of strings"))?,
        ),
        Some(AttributeType::IntArray) => AttributeValue::Int64Array(
            value
                .as_array()
                .ok_or_else(|| mismatch("an array of integers"))?
                .iter()
                .map(Value::as_i64)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| mismatch("an array of integers"))?,
        ),
        None => infer_attribute(name, value)?,
    };
    Ok(Some(parsed))
}

fn infer_attribute(name: &str, value: &Value) -> Result<AttributeValue> {
    Ok(match value {
        Value::String(s) => AttributeValue::String(s.clone()),
        Value::Bool(b) => AttributeValue::Bool(*b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => AttributeValue::Int64(i),
            None => AttributeValue::Float64(n.as_f64().unwrap_or_default()),
        },
        Value::Array(items) if items.iter().all(Value::is_string) => AttributeValue::StringArray(
            items
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
        ),
        Value::Array(items) if items.iter().all(|v| v.as_i64().is_some()) && !items.is_empty() => {
            AttributeValue::Int64Array(items.iter().filter_map(Value::as_i64).collect())
        }
        other => {
            return Err(Error::InvalidInput(format!(
                "attribute '{name}': unsupported value {other}; use a string, number, \
                 boolean, or array of strings or integers"
            )));
        }
    })
}

/// Renders an attribute as JSON.
pub(crate) fn attribute_to_json(value: &AttributeValue) -> Value {
    match value {
        AttributeValue::String(s) => Value::String(s.clone()),
        AttributeValue::Int64(i) => Value::from(*i),
        AttributeValue::Float64(f) => Value::from(*f),
        AttributeValue::Bool(b) => Value::Bool(*b),
        AttributeValue::StringArray(v) => Value::from(v.clone()),
        AttributeValue::Int64Array(v) => Value::from(v.clone()),
        AttributeValue::Vector(v) => Value::from(v.clone()),
    }
}

/// A row parsed from a write request.
#[derive(Debug, Clone)]
pub(crate) struct ParsedRow {
    pub(crate) id: String,
    pub(crate) vector: Option<Vec<f32>>,
    pub(crate) attributes: Vec<(String, AttributeValue)>,
}

/// Parses a JSON row object, inferring types for new attributes into
/// `inferred` (checked against `meta` and earlier rows of the request).
pub(crate) fn parse_row(
    row: &Map<String, Value>,
    meta: Option<&NamespaceMeta>,
    inferred: &mut BTreeMap<String, AttributeSchema>,
    allow_reserved: bool,
) -> Result<ParsedRow> {
    let id = parse_id(
        row.get(ID_KEY)
            .ok_or_else(|| Error::InvalidInput("row is missing 'id'".to_string()))?,
    )?;
    let mut vector = None;
    let mut attributes = Vec::new();
    for (name, value) in row {
        if name == ID_KEY {
            continue;
        }
        if name == VECTOR_FIELD_NAME {
            if !value.is_null() {
                vector = Some(parse_vector(value)?);
            }
            continue;
        }
        if !allow_reserved && (name == DOC_ID_ATTRIBUTE || name == PASSAGE_ATTRIBUTE) {
            return Err(Error::InvalidInput(format!(
                "attribute '{name}' is set by document writes"
            )));
        }
        validate_attribute_name(name)?;
        let expected = meta
            .and_then(|m| m.attribute_type(name))
            .or_else(|| inferred.get(name).map(|s| s.attribute_type));
        let Some(parsed) = parse_attribute(name, value, expected)? else {
            continue;
        };
        if expected.is_none()
            && let Some(attribute_type) = AttributeType::of(&parsed)
        {
            inferred.insert(name.clone(), AttributeSchema::inferred(attribute_type));
        }
        attributes.push((name.clone(), parsed));
    }
    Ok(ParsedRow {
        id,
        vector,
        attributes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meta() -> NamespaceMeta {
        NamespaceMeta {
            name: "ns".into(),
            id: "00".into(),
            shard: 0,
            dimensions: 2,
            distance_metric: Metric::Cosine,
            schema: BTreeMap::from([(
                "price".to_string(),
                AttributeSchema {
                    attribute_type: AttributeType::Float,
                    full_text_search: false,
                    embed: false,
                },
            )]),
            embedding_model: None,
            created_at_ms: 0,
        }
    }

    #[test]
    fn should_infer_and_coerce_row_attributes() {
        // given
        let row = json!({
            "id": 7,
            "vector": [0.5, 1],
            "price": 3,
            "tags": ["a", "b"],
            "user_ids": [1, 2],
            "skip": null,
        });
        let mut inferred = BTreeMap::new();

        // when
        let parsed = parse_row(
            row.as_object().unwrap(),
            Some(&meta()),
            &mut inferred,
            false,
        )
        .unwrap();

        // then
        assert_eq!(parsed.id, "7");
        assert_eq!(parsed.vector, Some(vec![0.5, 1.0]));
        let attrs: BTreeMap<_, _> = parsed.attributes.into_iter().collect();
        assert_eq!(attrs["price"], AttributeValue::Float64(3.0));
        assert_eq!(
            attrs["tags"],
            AttributeValue::StringArray(vec!["a".into(), "b".into()])
        );
        assert_eq!(attrs["user_ids"], AttributeValue::Int64Array(vec![1, 2]));
        assert!(!attrs.contains_key("skip"));
        assert_eq!(inferred["tags"].attribute_type, AttributeType::StringArray);
        assert!(!inferred.contains_key("price"));
    }

    #[test]
    fn should_reject_type_mismatch_and_reserved_names() {
        let mut inferred = BTreeMap::new();
        let bad_type = json!({"id": "a", "price": "cheap"});
        assert!(
            parse_row(
                bad_type.as_object().unwrap(),
                Some(&meta()),
                &mut inferred,
                false
            )
            .is_err()
        );
        let reserved = json!({"id": "a", "$doc_id": "x"});
        assert!(
            parse_row(
                reserved.as_object().unwrap(),
                Some(&meta()),
                &mut inferred,
                false
            )
            .is_err()
        );
        let dollar = json!({"id": "a", "$other": 1});
        assert!(
            parse_row(
                dollar.as_object().unwrap(),
                Some(&meta()),
                &mut inferred,
                false
            )
            .is_err()
        );
    }

    #[test]
    fn should_reject_schema_type_changes() {
        let mut meta = meta();
        let change = BTreeMap::from([(
            "price".to_string(),
            AttributeSchema {
                attribute_type: AttributeType::Int,
                full_text_search: false,
                embed: false,
            },
        )]);
        assert!(meta.merge_schema(&change).is_err());

        let text = BTreeMap::from([(
            "body".to_string(),
            AttributeSchema {
                attribute_type: AttributeType::String,
                full_text_search: true,
                embed: false,
            },
        )]);
        assert!(meta.merge_schema(&text).unwrap());
        assert_eq!(meta.text_fields().len(), 1);
    }
}
