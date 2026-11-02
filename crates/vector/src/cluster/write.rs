//! Namespace writes: row upserts, patches, deletes, and documents.
//!
//! A document is stored as one row per passage: passage `i` of document
//! `d` is row `"{d}#{i}"` carrying the document's attributes, the
//! passage's own attributes, `$doc_id = d`, and `$passage = i`. Upserting a
//! document replaces all of its passages; queries collapse passages back
//! to documents with `limit.per`. A document written without passages in a
//! namespace that embeds has its embed attribute split into passages sized
//! for the namespace's model, each passage row holding its part of the text.
//!
//! The operations of one request apply in the order `delete_by_filter`,
//! `delete_documents`, `upsert_documents`, `upsert_rows`, `patch_rows`,
//! `deletes`. Each is atomic on its own; a request as a whole is not.

use std::borrow::Cow;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::embedding::{self, Embedder, InputKind};
use super::namespace::Namespace;
use super::query::{FilterExpr, parse_filter};
use super::schema::{
    AttributeSchema, AttributeType, DOC_ID_ATTRIBUTE, ID_KEY, Metric, NamespaceMeta,
    PASSAGE_ATTRIBUTE, ParsedRow, parse_attribute, parse_id, parse_row, parse_vector,
};
use crate::error::{Error, Result};
use crate::model::{Attribute, AttributeValue, Filter, VECTOR_FIELD_NAME, Vector};

/// Rows loaded per batch when resolving deletes by filter.
const DELETE_BATCH: usize = 1_024;

/// A write to one namespace. Writing to a missing namespace creates it,
/// with dimensions taken from the first vector.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    /// Rows to insert or replace: `{"id": .., "vector": [..], attr: value}`.
    #[serde(default)]
    pub upsert_rows: Option<Vec<Map<String, Value>>>,
    /// Partial rows merged into existing rows; `null` removes an
    /// attribute. Missing rows are skipped.
    #[serde(default)]
    pub patch_rows: Option<Vec<Map<String, Value>>>,
    /// Row ids to delete.
    #[serde(default)]
    pub deletes: Option<Vec<Value>>,
    /// Deletes every row matching the filter.
    #[serde(default)]
    pub delete_by_filter: Option<FilterExpr>,
    /// Documents to insert or replace, one row per passage.
    #[serde(default)]
    pub upsert_documents: Option<Vec<DocumentInput>>,
    /// Document ids whose passages to delete.
    #[serde(default)]
    pub delete_documents: Option<Vec<Value>>,
    /// Metric of a namespace this write creates (default
    /// `cosine_distance`). Must match an existing namespace's.
    #[serde(default)]
    pub distance_metric: Option<Metric>,
    /// Attribute schema entries to add before writing.
    #[serde(default)]
    pub schema: Option<BTreeMap<String, AttributeSchema>>,
}

/// A document of several passages.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentInput {
    pub id: Value,
    /// Attributes copied to every passage row.
    #[serde(default)]
    pub attributes: Map<String, Value>,
    /// Passages: `{"vector": [..], attr: value}`. Without any, the server
    /// splits the embed attribute in `attributes` into passages.
    #[serde(default)]
    pub passages: Vec<Map<String, Value>>,
}

type Attributes = Map<String, Value>;

/// `document`'s attributes and passages: its own, or, without passages,
/// its embed attribute split into passages for the namespace's model.
fn passages<'a>(
    document: &'a DocumentInput,
    meta: &NamespaceMeta,
    embedder: Option<&Embedder>,
) -> Result<(Cow<'a, Attributes>, Cow<'a, [Attributes]>)> {
    if !document.passages.is_empty() {
        return Ok((
            Cow::Borrowed(&document.attributes),
            Cow::Borrowed(&document.passages),
        ));
    }
    let id = parse_id(&document.id)?;
    let Some(attribute) = meta.embed_attribute() else {
        return Err(Error::InvalidInput(format!(
            "document '{id}' has no passages; use delete_documents to remove it"
        )));
    };
    let Some(text) = document.attributes.get(attribute).and_then(Value::as_str) else {
        return Err(Error::InvalidInput(format!(
            "document '{id}' needs passages, or a string '{attribute}' to split into passages"
        )));
    };
    let passages: Vec<Attributes> = embedding::for_namespace(meta, embedder)?
        .split(text)
        .into_iter()
        .map(|part| Attributes::from_iter([(attribute.to_string(), Value::String(part))]))
        .collect();
    if passages.is_empty() {
        return Err(Error::InvalidInput(format!(
            "document '{id}' has no text in '{attribute}' to split into passages"
        )));
    }
    let mut attributes = document.attributes.clone();
    attributes.remove(attribute);
    Ok((Cow::Owned(attributes), Cow::Owned(passages)))
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct WriteResponse {
    pub rows_upserted: usize,
    pub rows_patched: usize,
    pub rows_deleted: usize,
    pub documents_upserted: usize,
    pub documents_deleted: usize,
}

impl WriteRequest {
    /// Dimensions of the first vector in the request.
    pub(crate) fn first_dimensions(&self) -> Option<usize> {
        let rows = self.upsert_rows.iter().flatten();
        let passages = self
            .upsert_documents
            .iter()
            .flatten()
            .flat_map(|d| d.passages.iter());
        rows.chain(passages)
            .filter_map(|row| row.get(VECTOR_FIELD_NAME))
            .find_map(|v| parse_vector(v).ok())
            .map(|v| v.len())
    }
}

/// Applies `request` to `namespace`, returning the counts and any
/// attribute types inferred from it.
pub(crate) async fn write(
    namespace: &Namespace,
    request: WriteRequest,
    embedder: Option<&Embedder>,
) -> Result<(WriteResponse, BTreeMap<String, AttributeSchema>)> {
    let meta = namespace.meta();
    let mut inferred = BTreeMap::new();
    let mut response = WriteResponse::default();

    // Parse everything that doesn't depend on stored rows up front, so a
    // malformed request changes nothing.
    let delete_filter = request
        .delete_by_filter
        .as_ref()
        .map(|f| parse_filter(f, &meta))
        .transpose()?;
    let deleted_documents = request
        .delete_documents
        .iter()
        .flatten()
        .map(parse_id)
        .collect::<Result<Vec<_>>>()?;
    let mut documents = Vec::new();
    for document in request.upsert_documents.iter().flatten() {
        documents.push(parse_document(document, &meta, embedder, &mut inferred)?);
    }
    let mut upserts = request
        .upsert_rows
        .iter()
        .flatten()
        .map(|row| parse_row(row, Some(&meta), &mut inferred, false))
        .collect::<Result<Vec<_>>>()?;
    {
        let rows = documents
            .iter_mut()
            .flat_map(|(_, rows)| rows.iter_mut())
            .chain(upserts.iter_mut());
        embed_missing(rows, &meta, embedder).await?;
    }
    let deletes = request
        .deletes
        .iter()
        .flatten()
        .map(parse_id)
        .collect::<Result<Vec<_>>>()?;

    if let Some(filter) = delete_filter {
        response.rows_deleted += delete_matching(namespace, &filter).await?;
    }
    if !deleted_documents.is_empty() {
        let filter = Filter::In(
            DOC_ID_ATTRIBUTE.into(),
            deleted_documents
                .iter()
                .map(|id| AttributeValue::String(id.clone()))
                .collect(),
        );
        response.rows_deleted += delete_matching(namespace, &filter).await?;
        response.documents_deleted = deleted_documents.len();
    }
    if !documents.is_empty() {
        // Drop passages beyond each document's new passage count.
        let stale = Filter::Or(
            documents
                .iter()
                .map(|(id, rows)| {
                    Filter::And(vec![
                        Filter::Eq(DOC_ID_ATTRIBUTE.into(), AttributeValue::String(id.clone())),
                        Filter::Gte(
                            PASSAGE_ATTRIBUTE.into(),
                            AttributeValue::Int64(rows.len() as i64),
                        ),
                    ])
                })
                .collect(),
        );
        response.rows_deleted += delete_matching(namespace, &stale).await?;
        response.documents_upserted = documents.len();
        let rows: Vec<ParsedRow> = documents.into_iter().flat_map(|(_, rows)| rows).collect();
        response.rows_upserted += rows.len();
        namespace
            .db()?
            .write(rows.into_iter().map(to_vector).collect())
            .await?;
    }
    if !upserts.is_empty() {
        response.rows_upserted += upserts.len();
        namespace
            .db()?
            .write(upserts.into_iter().map(to_vector).collect())
            .await?;
    }
    if let Some(patches) = &request.patch_rows
        && !patches.is_empty()
    {
        let patched = patch(namespace, &meta, patches, &mut inferred, embedder).await?;
        response.rows_patched = patched.len();
        if !patched.is_empty() {
            namespace.db()?.write(patched).await?;
        }
    }
    if !deletes.is_empty() {
        response.rows_deleted += deletes.len();
        namespace.db()?.delete(deletes).await?;
    }
    Ok((response, inferred))
}

/// Fills in the vector of each row written without one by embedding its
/// embed attribute.
async fn embed_missing<'a>(
    rows: impl Iterator<Item = &'a mut ParsedRow>,
    meta: &NamespaceMeta,
    embedder: Option<&Embedder>,
) -> Result<()> {
    let mut missing = Vec::new();
    for row in rows.filter(|row| row.vector.is_none()) {
        let Some(attribute) = meta.embed_attribute() else {
            return Err(Error::InvalidInput(format!(
                "row '{}' is missing 'vector'",
                row.id
            )));
        };
        match embed_text(row, attribute) {
            Some(text) => missing.push((text.to_string(), row)),
            None => {
                return Err(Error::InvalidInput(format!(
                    "row '{}' needs a 'vector' or a '{attribute}' to embed",
                    row.id
                )));
            }
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let embedder = embedding::for_namespace(meta, embedder)?;
    let texts: Vec<&str> = missing.iter().map(|(text, _)| text.as_str()).collect();
    let vectors = embedder.embed(&texts, InputKind::Document).await?;
    for ((_, row), vector) in missing.into_iter().zip(vectors) {
        row.vector = Some(vector.to_vec());
    }
    Ok(())
}

fn embed_text<'a>(row: &'a ParsedRow, attribute: &str) -> Option<&'a str> {
    row.attributes.iter().find_map(|(name, value)| match value {
        AttributeValue::String(text) if name == attribute => Some(text.as_str()),
        _ => None,
    })
}

/// Embeds the texts `request` will embed into the cache, so the provider
/// is called before the namespace's write lock is taken.
pub(crate) async fn prefetch_embeddings(
    request: &WriteRequest,
    meta: &NamespaceMeta,
    embedder: Option<&Embedder>,
) -> Result<()> {
    let Some(attribute) = meta.embed_attribute() else {
        return Ok(());
    };
    let Some(embedder) = embedder.filter(|e| e.caches()) else {
        return Ok(());
    };
    // Documents that fail to split are left for the write to reject.
    let documents: Vec<_> = request
        .upsert_documents
        .iter()
        .flatten()
        .filter_map(|d| passages(d, meta, Some(embedder)).ok())
        .collect();
    let passage_texts = documents.iter().flat_map(|(attributes, passages)| {
        passages
            .iter()
            .filter(|p| !p.contains_key(VECTOR_FIELD_NAME))
            .filter_map(|p| p.get(attribute).or_else(|| attributes.get(attribute)))
    });
    let rows = request
        .upsert_rows
        .iter()
        .flatten()
        .chain(request.patch_rows.iter().flatten())
        .filter(|row| !row.contains_key(VECTOR_FIELD_NAME))
        .filter_map(|row| row.get(attribute));
    let texts: Vec<&str> = passage_texts
        .chain(rows)
        .filter_map(Value::as_str)
        .collect();
    if texts.is_empty() {
        return Ok(());
    }
    embedding::for_namespace(meta, Some(embedder))?
        .embed(&texts, InputKind::Document)
        .await?;
    Ok(())
}

fn to_vector(row: ParsedRow) -> Vector {
    let mut vector = Vector::new(row.id, row.vector.unwrap_or_default());
    vector.attributes.extend(
        row.attributes
            .into_iter()
            .map(|(name, value)| Attribute::new(name, value)),
    );
    vector
}

/// Expands a document into its passage rows.
fn parse_document(
    document: &DocumentInput,
    meta: &NamespaceMeta,
    embedder: Option<&Embedder>,
    inferred: &mut BTreeMap<String, AttributeSchema>,
) -> Result<(String, Vec<ParsedRow>)> {
    let id = parse_id(&document.id)?;
    for reserved in [
        ID_KEY,
        VECTOR_FIELD_NAME,
        DOC_ID_ATTRIBUTE,
        PASSAGE_ATTRIBUTE,
    ] {
        if document.attributes.contains_key(reserved) {
            return Err(Error::InvalidInput(format!(
                "document '{id}': '{reserved}' is not a document attribute"
            )));
        }
    }
    let (attributes, passages) = passages(document, meta, embedder)?;
    let mut rows = Vec::with_capacity(passages.len());
    for (i, passage) in passages.iter().enumerate() {
        for reserved in [ID_KEY, DOC_ID_ATTRIBUTE, PASSAGE_ATTRIBUTE] {
            if passage.contains_key(reserved) {
                return Err(Error::InvalidInput(format!(
                    "document '{id}' passage {i}: '{reserved}' is set by the server"
                )));
            }
        }
        let mut row = attributes.as_ref().clone();
        row.extend(passage.iter().map(|(k, v)| (k.clone(), v.clone())));
        row.insert(ID_KEY.into(), Value::String(format!("{id}#{i}")));
        row.insert(DOC_ID_ATTRIBUTE.into(), Value::String(id.clone()));
        row.insert(PASSAGE_ATTRIBUTE.into(), Value::from(i as i64));
        rows.push(parse_row(&row, Some(meta), inferred, true)?);
    }
    Ok((id, rows))
}

/// Deletes every row matching `filter` in the latest state, returning how
/// many were deleted.
async fn delete_matching(namespace: &Namespace, filter: &Filter) -> Result<usize> {
    let engine = namespace.engine(true).await?;
    let prepared = engine.prepare_filter(Some(filter)).await?;
    let ids: Vec<u64> = engine.matching_ids(&prepared).await?.iter().collect();
    let mut deleted = 0;
    for chunk in ids.chunks(DELETE_BATCH) {
        let external: Vec<String> = engine
            .load_rows(chunk)
            .await?
            .into_iter()
            .map(|row| row.id)
            .collect();
        deleted += external.len();
        if !external.is_empty() {
            namespace.db()?.delete(external).await?;
        }
    }
    Ok(deleted)
}

/// Merges `patches` into the stored rows they name.
async fn patch(
    namespace: &Namespace,
    meta: &NamespaceMeta,
    patches: &[Map<String, Value>],
    inferred: &mut BTreeMap<String, AttributeSchema>,
    embedder: Option<&Embedder>,
) -> Result<Vec<Vector>> {
    let engine = namespace.engine(true).await?;
    let embed_attribute = meta.embed_attribute();
    let mut out = Vec::with_capacity(patches.len());
    // Rows whose embedded text changed without a new vector, by index into
    // `out`.
    let mut reembed: Vec<(usize, String)> = Vec::new();
    for patch in patches {
        let id = parse_id(
            patch
                .get(ID_KEY)
                .ok_or_else(|| Error::InvalidInput("patch row is missing 'id'".into()))?,
        )?;
        let Some(mut row) = engine.get(&id).await? else {
            continue;
        };
        for (name, value) in patch {
            if name == ID_KEY {
                continue;
            }
            if name == DOC_ID_ATTRIBUTE || name == PASSAGE_ATTRIBUTE {
                return Err(Error::InvalidInput(format!(
                    "attribute '{name}' is set by document writes"
                )));
            }
            if name == VECTOR_FIELD_NAME {
                if value.is_null() {
                    return Err(Error::InvalidInput("cannot remove a row's vector".into()));
                }
                let vector = AttributeValue::Vector(parse_vector(value)?);
                set_attribute(&mut row, name, Some(vector));
                continue;
            }
            super::schema::validate_attribute_name(name)?;
            let expected = meta
                .attribute_type(name)
                .or_else(|| inferred.get(name).map(|s| s.attribute_type));
            let parsed = parse_attribute(name, value, expected)?;
            if expected.is_none()
                && let Some(attribute_type) = parsed.as_ref().and_then(AttributeType::of)
            {
                inferred.insert(name.clone(), AttributeSchema::inferred(attribute_type));
            }
            set_attribute(&mut row, name, parsed);
        }
        if let Some(attribute) = embed_attribute
            && !patch.contains_key(VECTOR_FIELD_NAME)
            && let Some(Value::String(text)) = patch.get(attribute)
        {
            reembed.push((out.len(), text.clone()));
        }
        out.push(row);
    }
    if !reembed.is_empty() {
        let embedder = embedding::for_namespace(meta, embedder)?;
        let texts: Vec<&str> = reembed.iter().map(|(_, text)| text.as_str()).collect();
        let vectors = embedder.embed(&texts, InputKind::Document).await?;
        for ((i, _), vector) in reembed.iter().zip(vectors) {
            set_attribute(
                &mut out[*i],
                VECTOR_FIELD_NAME,
                Some(AttributeValue::Vector(vector.to_vec())),
            );
        }
    }
    Ok(out)
}

fn set_attribute(row: &mut Vector, name: &str, value: Option<AttributeValue>) {
    row.attributes.retain(|a| a.name != name);
    if let Some(value) = value {
        row.attributes.push(Attribute::new(name, value));
    }
}
