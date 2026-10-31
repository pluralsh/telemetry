//! Embedding text into vectors with a hosted model.
//!
//! With an `embedding` section in the cluster config, a namespace can mark
//! one `string` attribute `embed` in its schema: rows written without a
//! `vector` get the embedding of that attribute, and queries can rank by
//! `{"op": "ANN", "text": ..}`. The first namespace schema to embed pins the
//! model's [`Embedder::model_id`] in the namespace, so a namespace never
//! mixes vectors of two models.
//!
//! Providers are reached through [Rig](rig_core): OpenAI (and any
//! OpenAI-compatible endpoint, e.g. Azure, vLLM or Ollama), Cohere, Gemini,
//! and Amazon Bedrock's Titan text models through [`rig_bedrock`]. Cohere
//! embeds documents and queries as such (its `input_type`).
//!
//! Embeddings are cached in memory with foyer, keyed by model, input kind
//! and text: a re-upserted row or a repeated query reuses its vector.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use foyer::{Cache, CacheBuilder};
use rig_bedrock::client::BedrockRuntime;
use rig_core::DynModel;
use rig_core::driver::Model;
use rig_core::operation::Embedding;
use rig_core::providers::{cohere, gemini, openai};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use super::schema::NamespaceMeta;
use crate::error::{Error, Result};

/// Embedding vectors cached when `cache.capacity` isn't set: 64 MiB.
pub const DEFAULT_CACHE_CAPACITY: u64 = 64 * 1024 * 1024;

const PROBE_TEXT: &str = "dimensions";

/// The `embedding` section of the cluster config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingConfig {
    pub provider: EmbeddingProvider,
    /// The provider's model name, e.g. `text-embedding-3-small`.
    pub model: String,
    /// Sent as the provider's API key. Optional for endpoints that need none.
    /// Bedrock authenticates through the AWS credential chain instead (IAM,
    /// or a Bedrock API key in `AWS_BEARER_TOKEN_BEDROCK`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<ApiKey>,
    /// API root replacing the provider's, e.g. `http://ollama:11434/v1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Bedrock only: the AWS region of the runtime endpoint. Without it, the
    /// region of the AWS config chain (`AWS_REGION`, profile, IMDS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Output dimensions to request, for models that can shorten their
    /// embeddings (OpenAI's `text-embedding-3-*`, Gemini, Titan v2). Without
    /// it, the model's default size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<u16>,
    /// Most texts sent in one provider request.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Provider request timeout, in seconds.
    #[serde(default = "default_timeout", with = "crate::model::duration_secs")]
    pub timeout: Duration,
    #[serde(default)]
    pub cache: EmbeddingCacheConfig,
}

fn default_batch_size() -> usize {
    96
}

fn default_timeout() -> Duration {
    Duration::from_secs(30)
}

/// An embedding API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingProvider {
    /// OpenAI, or an OpenAI-compatible `endpoint`.
    #[serde(rename = "openai")]
    OpenAi,
    Cohere,
    Gemini,
    /// Amazon Bedrock's Titan Text Embeddings v2.
    Bedrock,
}

impl EmbeddingProvider {
    fn name(self) -> &'static str {
        match self {
            EmbeddingProvider::OpenAi => "openai",
            EmbeddingProvider::Cohere => "cohere",
            EmbeddingProvider::Gemini => "gemini",
            EmbeddingProvider::Bedrock => "bedrock",
        }
    }
}

/// The Bedrock model family rig-bedrock's request body fits.
const BEDROCK_TITAN_V2: &str = "amazon.titan-embed-text-v2";
/// Titan v2's output sizes; it embeds at the largest unless asked.
const TITAN_V2_DIMENSIONS: [u16; 3] = [256, 512, 1024];

/// Where the API key comes from. File keys are re-read on each request, so
/// a rotated Secret takes effect without a restart.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApiKey {
    Env { name: String },
    File { path: PathBuf },
    Literal { value: String },
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiKey::Env { name } => write!(f, "ApiKey(env {name})"),
            ApiKey::File { path } => write!(f, "ApiKey(file {})", path.display()),
            ApiKey::Literal { .. } => f.write_str("ApiKey([REDACTED])"),
        }
    }
}

impl ApiKey {
    fn expose(&self) -> std::result::Result<String, String> {
        let value = match self {
            ApiKey::Env { name } => {
                std::env::var(name).map_err(|_| format!("environment variable {name} is unset"))?
            }
            ApiKey::File { path } => std::fs::read_to_string(path)
                .map_err(|e| format!("cannot read {}: {e}", path.display()))?,
            ApiKey::Literal { value } => value.clone(),
        };
        let value = value.trim_end_matches(['\r', '\n']);
        if value.is_empty() {
            return Err("the embedding API key is empty".into());
        }
        Ok(value.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EmbeddingCacheConfig {
    /// Bytes of embeddings to keep in memory; `0` disables the cache.
    pub capacity: u64,
}

impl Default for EmbeddingCacheConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_CACHE_CAPACITY,
        }
    }
}

/// What a text is embedded as: a stored document, or a search query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InputKind {
    Document,
    Query,
}

/// A model turning texts into vectors.
#[async_trait]
pub trait EmbeddingModel: Send + Sync {
    /// Identifies the model and its output: vectors of two ids never mix in
    /// a namespace.
    fn id(&self) -> &str;

    /// One vector per text, in order.
    async fn embed(&self, texts: Vec<String>, kind: InputKind) -> Result<Vec<Vec<f32>>>;
}

/// The Rig-backed [`EmbeddingModel`] of an [`EmbeddingConfig`].
struct ProviderModel {
    id: String,
    provider: EmbeddingProvider,
    model: String,
    api_key: Option<ApiKey>,
    endpoint: Option<String>,
    dimensions: Option<u16>,
    timeout: Duration,
    bedrock: Option<BedrockRuntime>,
}

/// The id pinned in namespaces: provider, model and requested dimensions.
fn model_id(config: &EmbeddingConfig) -> String {
    let mut id = format!("{}:{}", config.provider.name(), config.model);
    if let Some(dimensions) = config.dimensions {
        id.push_str(&format!(":{dimensions}"));
    }
    id
}

/// The output size requested of the provider: the configured one, or Titan
/// v2's default, which its request body always names.
fn requested_dimensions(config: &EmbeddingConfig) -> Option<u16> {
    match config.provider {
        EmbeddingProvider::Bedrock => Some(config.dimensions.unwrap_or(TITAN_V2_DIMENSIONS[2])),
        _ => config.dimensions,
    }
}

/// The [`EmbeddingModel`] of `config`'s provider.
fn provider_model(config: &EmbeddingConfig) -> Result<Arc<dyn EmbeddingModel>> {
    let invalid = |message: String| Err(Error::InvalidInput(message));
    if config.model.is_empty() {
        return invalid("embedding.model is empty".into());
    }
    let provider = config.provider.name();
    match config.provider {
        EmbeddingProvider::Bedrock => {
            if config.api_key.is_some() || config.endpoint.is_some() {
                return invalid(
                    "embedding.api_key and embedding.endpoint don't apply to bedrock; it uses the \
                     AWS credential chain (IAM, or AWS_BEARER_TOKEN_BEDROCK)"
                        .into(),
                );
            }
            if !config.model.contains(BEDROCK_TITAN_V2) {
                return invalid(format!(
                    "bedrock embeds with {BEDROCK_TITAN_V2} models, not '{}'",
                    config.model
                ));
            }
            if let Some(dimensions) = config.dimensions
                && !TITAN_V2_DIMENSIONS.contains(&dimensions)
            {
                return invalid(format!(
                    "{BEDROCK_TITAN_V2} embeds at {TITAN_V2_DIMENSIONS:?} dimensions, not {dimensions}"
                ));
            }
        }
        _ if config.region.is_some() => {
            return invalid("embedding.region only applies to the bedrock provider".into());
        }
        EmbeddingProvider::Cohere if config.dimensions.is_some() => {
            return invalid(format!(
                "embedding.dimensions isn't supported by {provider}; its models embed at a fixed size"
            ));
        }
        _ => {}
    }
    let bedrock = (config.provider == EmbeddingProvider::Bedrock).then(|| {
        let mut builder = BedrockRuntime::builder();
        if let Some(region) = &config.region {
            builder = builder.region(region);
        }
        builder.build()
    });
    Ok(Arc::new(ProviderModel {
        id: model_id(config),
        provider: config.provider,
        model: config.model.clone(),
        api_key: config.api_key.clone(),
        endpoint: config.endpoint.clone(),
        dimensions: requested_dimensions(config),
        timeout: config.timeout,
        bedrock,
    }))
}

impl ProviderModel {
    /// The Rig model for one request. Built per request because it holds the
    /// API key, which a file source may rotate.
    fn connect(&self, kind: InputKind) -> Result<DynModel<Embedding>> {
        let api_key = match &self.api_key {
            Some(key) => key.expose().map_err(Error::InvalidInput)?,
            None => String::new(),
        };
        let dimensions = self.dimensions.map(usize::from);
        let http = rig_reqwest::shared();
        Ok(match self.provider {
            EmbeddingProvider::OpenAi => {
                let mut config = openai::OpenAIConfig::new(api_key)
                    .with_auth(openai::wire::Auth::OptionalBearer);
                if let Some(endpoint) = &self.endpoint {
                    config = config.with_base_url(endpoint.clone());
                }
                Model::new(
                    openai::wire::Embeddings::new(config, self.model.clone(), dimensions),
                    http,
                )
                .erase()
            }
            EmbeddingProvider::Cohere => {
                let mut config = cohere::CohereConfig::new(api_key);
                if let Some(endpoint) = &self.endpoint {
                    config = config.with_base_url(endpoint);
                }
                let input_type = match kind {
                    InputKind::Document => "search_document",
                    InputKind::Query => "search_query",
                };
                let wire = cohere::embeddings::Embeddings {
                    provider: config,
                    model: self.model.clone(),
                    ndims: 0,
                    input_type: input_type.into(),
                };
                Model::new(wire, http).erase()
            }
            EmbeddingProvider::Gemini => {
                let mut config = gemini::GeminiConfig::new(api_key);
                if let Some(endpoint) = &self.endpoint {
                    config = config.with_base_url(endpoint.clone());
                }
                Model::new(
                    gemini::embedding::Embeddings::new(config, self.model.clone(), dimensions),
                    http,
                )
                .erase()
            }
            EmbeddingProvider::Bedrock => self
                .bedrock
                .as_ref()
                .expect("a bedrock model has a runtime")
                .embedding(self.model.clone(), dimensions)
                .erase(),
        })
    }
}

#[async_trait]
impl EmbeddingModel for ProviderModel {
    fn id(&self) -> &str {
        &self.id
    }

    async fn embed(&self, texts: Vec<String>, kind: InputKind) -> Result<Vec<Vec<f32>>> {
        let model = self.connect(kind)?;
        let response = tokio::time::timeout(self.timeout, model.call(texts))
            .await
            .map_err(|_| {
                Error::Upstream(format!(
                    "embedding with {}: no reply in {:?}",
                    self.id, self.timeout
                ))
            })?
            .map_err(|e| Error::Upstream(format!("embedding with {}: {e}", self.id)))?;
        let mut vectors: Vec<Vec<f32>> = response
            .embeddings
            .into_iter()
            .map(|e| e.vec.into_iter().map(|x| x as f32).collect())
            .collect();
        if let Some(order) = reply_order(&response.raw, vectors.len()) {
            let mut sorted = vec![Vec::new(); vectors.len()];
            for (vector, index) in vectors.into_iter().zip(order) {
                sorted[index] = vector;
            }
            vectors = sorted;
        }
        Ok(vectors)
    }
}

/// The input position of each vector of an OpenAI-shaped reply, from its
/// `data[].index`: Rig keeps reply order, which OpenAI-compatible servers
/// needn't match. `None` unless the indices are a permutation of `0..count`.
fn reply_order(raw: &serde_json::Value, count: usize) -> Option<Vec<usize>> {
    let order: Vec<usize> = raw
        .get("data")?
        .as_array()?
        .iter()
        .map(|item| item.get("index")?.as_u64().map(|i| i as usize))
        .collect::<Option<_>>()?;
    let mut seen = vec![false; count];
    for &i in &order {
        if i >= count || std::mem::replace(&mut seen[i], true) {
            return None;
        }
    }
    (order.len() == count).then_some(order)
}

/// The embedder for `meta`'s embed attribute: `embedder`, if it is the
/// model the namespace pinned.
pub(crate) fn for_namespace<'a>(
    meta: &NamespaceMeta,
    embedder: Option<&'a Embedder>,
) -> Result<&'a Embedder> {
    let Some(pinned) = &meta.embedding_model else {
        return Err(Error::InvalidInput(format!(
            "namespace '{}' doesn't embed text; mark a string attribute embed in its schema",
            meta.name
        )));
    };
    let Some(embedder) = embedder else {
        return Err(Error::InvalidInput(format!(
            "namespace '{}' embeds with {pinned}, but no embedding model is configured",
            meta.name
        )));
    };
    if embedder.model_id() != pinned {
        return Err(Error::InvalidInput(format!(
            "namespace '{}' embeds with {pinned}, but the configured model is {}",
            meta.name,
            embedder.model_id()
        )));
    }
    Ok(embedder)
}

/// Pins the namespace's model when its schema first embeds, checking that
/// `embedder` makes vectors of the namespace's dimensions.
pub(crate) async fn pin(meta: &mut NamespaceMeta, embedder: Option<&Embedder>) -> Result<()> {
    let Some(attribute) = meta.embed_attribute() else {
        return Ok(());
    };
    if meta.embedding_model.is_some() {
        return Ok(());
    }
    let Some(embedder) = embedder else {
        return Err(Error::InvalidInput(format!(
            "attribute '{attribute}' embeds, but no embedding model is configured"
        )));
    };
    let dimensions = embedder.dimensions().await?;
    if dimensions != meta.dimensions {
        return Err(Error::InvalidInput(format!(
            "{} makes {dimensions}-dimensional vectors; namespace '{}' has {}",
            embedder.model_id(),
            meta.name,
            meta.dimensions
        )));
    }
    meta.embedding_model = Some(embedder.model_id().to_string());
    Ok(())
}

type CacheKey = [u8; 32];

/// Embeds texts with a model, through a cache. See the module docs.
pub struct Embedder {
    model: Arc<dyn EmbeddingModel>,
    cache: Option<Cache<CacheKey, Arc<[f32]>>>,
    batch_size: usize,
    dimensions: OnceCell<u16>,
}

impl fmt::Debug for Embedder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Embedder")
            .field("model", &self.model.id())
            .finish()
    }
}

impl Embedder {
    pub fn new(config: &EmbeddingConfig) -> Result<Self> {
        let embedder = Self::with_model(
            provider_model(config)?,
            config.cache.capacity,
            config.batch_size,
        );
        if let Some(dimensions) = requested_dimensions(config) {
            embedder
                .dimensions
                .set(dimensions)
                .expect("a new cell is empty");
        }
        Ok(embedder)
    }

    /// An embedder over any `model`, caching up to `cache_capacity` bytes.
    pub fn with_model(
        model: Arc<dyn EmbeddingModel>,
        cache_capacity: u64,
        batch_size: usize,
    ) -> Self {
        let cache = (cache_capacity > 0).then(|| {
            CacheBuilder::new(usize::try_from(cache_capacity).unwrap_or(usize::MAX))
                .with_name("vector_embedding_cache")
                .with_weighter(|_: &CacheKey, vector: &Arc<[f32]>| {
                    size_of::<CacheKey>() + vector.len() * size_of::<f32>()
                })
                .build()
        });
        Self {
            model,
            cache,
            batch_size: batch_size.max(1),
            dimensions: OnceCell::new(),
        }
    }

    /// The model's [`EmbeddingModel::id`], pinned in namespaces it embeds.
    pub fn model_id(&self) -> &str {
        self.model.id()
    }

    pub fn caches(&self) -> bool {
        self.cache.is_some()
    }

    /// The length of the model's vectors, from the config or by embedding a
    /// probe text once.
    pub async fn dimensions(&self) -> Result<u16> {
        self.dimensions
            .get_or_try_init(|| async {
                let probe = self.embed(&[PROBE_TEXT], InputKind::Document).await?;
                u16::try_from(probe[0].len()).map_err(|_| {
                    Error::Upstream(format!(
                        "{} returned {} dimensions; at most {} are supported",
                        self.model_id(),
                        probe[0].len(),
                        u16::MAX
                    ))
                })
            })
            .await
            .copied()
    }

    /// One vector per text, in order. Cached texts aren't sent; the rest
    /// are sent once each, in batches.
    pub async fn embed(&self, texts: &[&str], kind: InputKind) -> Result<Vec<Arc<[f32]>>> {
        let keys: Vec<CacheKey> = texts.iter().map(|t| self.key(t, kind)).collect();
        let mut out: Vec<Option<Arc<[f32]>>> = keys
            .iter()
            .map(|key| {
                self.cache
                    .as_ref()
                    .and_then(|c| c.get(key))
                    .map(|e| e.value().clone())
            })
            .collect();
        let hits = out.iter().filter(|v| v.is_some()).count();
        metrics::counter!(crate::metric_names::EMBEDDING_CACHE_HITS_TOTAL).increment(hits as u64);

        // Each distinct missing text, with every position it fills.
        let mut missing: Vec<(CacheKey, &str, Vec<usize>)> = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            if out[i].is_some() {
                continue;
            }
            match missing.iter_mut().find(|(k, _, _)| k == key) {
                Some((_, _, positions)) => positions.push(i),
                None => missing.push((*key, texts[i], vec![i])),
            }
        }
        metrics::counter!(crate::metric_names::EMBEDDING_CACHE_MISSES_TOTAL)
            .increment(missing.len() as u64);
        for batch in missing.chunks(self.batch_size) {
            let start = Instant::now();
            let result = self
                .model
                .embed(batch.iter().map(|(_, t, _)| t.to_string()).collect(), kind)
                .await;
            metrics::histogram!(crate::metric_names::EMBEDDING_REQUEST_DURATION_SECONDS)
                .record(start.elapsed().as_secs_f64());
            let vectors = match result {
                Ok(vectors) => vectors,
                Err(e) => {
                    metrics::counter!(crate::metric_names::EMBEDDING_REQUEST_ERRORS_TOTAL)
                        .increment(1);
                    return Err(e);
                }
            };
            for ((key, _, positions), vector) in batch.iter().zip(vectors) {
                let vector: Arc<[f32]> = vector.into();
                if let Some(expected) = self.dimensions.get()
                    && vector.len() != usize::from(*expected)
                {
                    return Err(Error::Upstream(format!(
                        "{} returned {} dimensions, not {expected}",
                        self.model_id(),
                        vector.len()
                    )));
                }
                if let Some(cache) = &self.cache {
                    cache.insert(*key, vector.clone());
                }
                for &i in positions {
                    out[i] = Some(vector.clone());
                }
            }
        }
        Ok(out
            .into_iter()
            .map(|v| v.expect("every text is embedded"))
            .collect())
    }

    fn key(&self, text: &str, kind: InputKind) -> CacheKey {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.model_id().as_bytes());
        hasher.update(&[0, kind as u8]);
        hasher.update(text.as_bytes());
        *hasher.finalize().as_bytes()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A model mapping a text to a fixed function of its bytes, recording
    /// what it was asked to embed.
    pub(crate) struct FakeModel {
        pub(crate) id: String,
        pub(crate) dimensions: usize,
        pub(crate) calls: Mutex<Vec<(Vec<String>, InputKind)>>,
    }

    impl FakeModel {
        pub(crate) fn new(dimensions: usize) -> Arc<Self> {
            Arc::new(Self {
                id: format!("fake:{dimensions}"),
                dimensions,
                calls: Mutex::default(),
            })
        }

        pub(crate) fn vector(&self, text: &str) -> Vec<f32> {
            let hash = blake3::hash(text.as_bytes());
            (0..self.dimensions)
                .map(|i| f32::from(hash.as_bytes()[i % 32]) + 1.0)
                .collect()
        }

        pub(crate) fn texts(&self) -> Vec<String> {
            let calls = self.calls.lock().unwrap();
            calls.iter().flat_map(|(texts, _)| texts.clone()).collect()
        }
    }

    #[async_trait]
    impl EmbeddingModel for FakeModel {
        fn id(&self) -> &str {
            &self.id
        }

        async fn embed(&self, texts: Vec<String>, kind: InputKind) -> Result<Vec<Vec<f32>>> {
            let vectors = texts.iter().map(|t| self.vector(t)).collect();
            self.calls.lock().unwrap().push((texts, kind));
            Ok(vectors)
        }
    }

    #[tokio::test]
    async fn should_embed_each_distinct_text_once_and_cache_it() {
        // given
        let model = FakeModel::new(4);
        let embedder = Embedder::with_model(model.clone(), 1 << 20, 2);

        // when
        let first = embedder
            .embed(&["a", "b", "a", "c"], InputKind::Document)
            .await
            .unwrap();
        let second = embedder
            .embed(&["c", "a"], InputKind::Document)
            .await
            .unwrap();
        let query = embedder.embed(&["a"], InputKind::Query).await.unwrap();

        // then
        assert_eq!(&*first[0], model.vector("a").as_slice());
        assert_eq!(first[0], first[2]);
        assert_eq!(&*first[3], model.vector("c").as_slice());
        assert_eq!(second[0], first[3]);
        assert_eq!(&*query[0], model.vector("a").as_slice());
        let calls = model.calls.lock().unwrap();
        let batches: Vec<_> = calls.iter().map(|(t, k)| (t.clone(), *k)).collect();
        assert_eq!(
            batches,
            vec![
                (vec!["a".to_string(), "b".to_string()], InputKind::Document),
                (vec!["c".to_string()], InputKind::Document),
                (vec!["a".to_string()], InputKind::Query),
            ],
            "batched by 2, cached across calls, and keyed by input kind"
        );
    }

    #[tokio::test]
    async fn should_send_every_text_without_a_cache() {
        let model = FakeModel::new(2);
        let embedder = Embedder::with_model(model.clone(), 0, 8);
        embedder.embed(&["a"], InputKind::Query).await.unwrap();
        embedder.embed(&["a"], InputKind::Query).await.unwrap();
        assert_eq!(model.texts(), vec!["a", "a"]);
    }

    #[tokio::test]
    async fn should_probe_dimensions_once() {
        let model = FakeModel::new(3);
        let embedder = Embedder::with_model(model.clone(), 1 << 20, 8);
        assert_eq!(embedder.dimensions().await.unwrap(), 3);
        assert_eq!(embedder.dimensions().await.unwrap(), 3);
        assert_eq!(model.texts(), vec![PROBE_TEXT]);
    }

    /// Requests an OpenAI-, Cohere- and Gemini-shaped mock provider
    /// received: path and query, `Authorization` header, and body.
    #[cfg(feature = "http-server")]
    type Received = Arc<Mutex<Vec<(String, String, serde_json::Value)>>>;

    #[cfg(feature = "http-server")]
    async fn mock_provider() -> (String, Received) {
        use axum::Json;
        use axum::extract::{OriginalUri, State};
        use axum::http::HeaderMap;
        use serde_json::{Value, json};

        async fn embed(
            State(received): State<Received>,
            OriginalUri(uri): OriginalUri,
            headers: HeaderMap,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            let auth = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let target = uri.path_and_query().map(|p| p.to_string());
            received
                .lock()
                .unwrap()
                .push((target.unwrap_or_default(), auth, body.clone()));
            let gemini_texts = body
                .get("requests")
                .and_then(Value::as_array)
                .map(|requests| {
                    requests
                        .iter()
                        .map(|r| {
                            r["content"]["parts"][0]["text"]
                                .as_str()
                                .unwrap()
                                .to_string()
                        })
                        .collect::<Vec<_>>()
                });
            let texts: Vec<String> = gemini_texts.unwrap_or_else(|| {
                body.get("input")
                    .or_else(|| body.get("texts"))
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default()
            });
            let vectors: Vec<Vec<f32>> = texts
                .iter()
                .map(|t| vec![t.len() as f32, 1.0, 0.0])
                .collect();
            Json(if uri.path().ends_with("/v1/embed") {
                json!({"id": "1", "embeddings": vectors})
            } else if uri.path().ends_with(":batchEmbedContents") {
                let embeddings: Vec<Value> = vectors.iter().map(|v| json!({"values": v})).collect();
                json!({"embeddings": embeddings})
            } else {
                // Out of order: the embedder reorders by `index`.
                let data: Vec<Value> = vectors
                    .iter()
                    .enumerate()
                    .rev()
                    .map(|(index, embedding)| json!({"index": index, "embedding": embedding}))
                    .collect();
                json!({
                    "data": data,
                    "model": "m",
                    "usage": {"prompt_tokens": 1, "total_tokens": 1},
                })
            })
        }

        let received = Received::default();
        let app = axum::Router::new()
            .fallback(embed)
            .with_state(received.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{address}"), received)
    }

    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn should_call_providers_at_the_configured_endpoint() {
        // given
        let (root, received) = mock_provider().await;
        let key_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(key_file.path(), "co-key\n").unwrap();
        let openai: EmbeddingConfig = serde_json::from_value(serde_json::json!({
            "provider": "openai",
            "model": "m",
            "endpoint": format!("{root}/v1/"),
            "api_key": {"source": "literal", "value": "sk-test"},
            "dimensions": 3,
        }))
        .unwrap();
        let cohere: EmbeddingConfig = serde_json::from_value(serde_json::json!({
            "provider": "cohere",
            "model": "m",
            "endpoint": root,
            "api_key": {"source": "file", "path": key_file.path()},
        }))
        .unwrap();
        let gemini: EmbeddingConfig = serde_json::from_value(serde_json::json!({
            "provider": "gemini",
            "model": "m",
            "endpoint": root,
            "api_key": {"source": "literal", "value": "g-key"},
            "dimensions": 3,
        }))
        .unwrap();
        let openai = Embedder::new(&openai).unwrap();
        let cohere = Embedder::new(&cohere).unwrap();
        let gemini = Embedder::new(&gemini).unwrap();

        // when
        let vectors = openai
            .embed(&["a", "bbb"], InputKind::Document)
            .await
            .unwrap();
        let query = cohere.embed(&["cc"], InputKind::Query).await.unwrap();
        let documents = gemini
            .embed(&["dddd", "e"], InputKind::Document)
            .await
            .unwrap();

        // then
        assert_eq!(&*vectors[0], &[1.0, 1.0, 0.0]);
        assert_eq!(&*vectors[1], &[3.0, 1.0, 0.0]);
        assert_eq!(&*query[0], &[2.0, 1.0, 0.0]);
        assert_eq!(&*documents[0], &[4.0, 1.0, 0.0]);
        assert_eq!(&*documents[1], &[1.0, 1.0, 0.0]);
        let received = received.lock().unwrap();
        assert_eq!(received[0].0, "/v1/embeddings");
        assert_eq!(received[0].1, "Bearer sk-test");
        assert_eq!(received[0].2["dimensions"], 3);
        assert_eq!(received[1].0, "/v1/embed");
        assert_eq!(received[1].1, "Bearer co-key");
        assert_eq!(received[1].2["input_type"], "search_query");
        assert_eq!(
            received[2].0,
            "/v1beta/models/m:batchEmbedContents?key=g-key"
        );
        assert_eq!(received[2].2["requests"][0]["output_dimensionality"], 3);
    }

    #[test]
    fn should_parse_config_and_redact_literal_keys() {
        let config: EmbeddingConfig = serde_yaml::from_str(
            "provider: gemini\nmodel: gemini-embedding-001\ndimensions: 512\n\
             api_key: { source: literal, value: sk-secret }\n",
        )
        .unwrap();
        assert_eq!(config.provider, EmbeddingProvider::Gemini);
        assert_eq!(config.batch_size, 96);
        assert_eq!(config.cache.capacity, DEFAULT_CACHE_CAPACITY);
        assert!(!format!("{config:?}").contains("sk-secret"));
        let embedder = Embedder::new(&config).unwrap();
        assert_eq!(embedder.model_id(), "gemini:gemini-embedding-001:512");
        assert!(
            serde_yaml::from_str::<EmbeddingConfig>("provider: anthropic\nmodel: x\n").is_err()
        );
        let rejected = [
            "provider: openai\nmodel: m\nregion: us-east-1\n",
            "provider: cohere\nmodel: embed-v4.0\ndimensions: 512\n",
            "provider: bedrock\nmodel: cohere.embed-english-v3\n",
            "provider: bedrock\nmodel: amazon.titan-embed-text-v2:0\ndimensions: 300\n",
            "provider: bedrock\nmodel: amazon.titan-embed-text-v2:0\nendpoint: http://x\n",
        ];
        for yaml in rejected {
            let config: EmbeddingConfig = serde_yaml::from_str(yaml).unwrap();
            assert!(Embedder::new(&config).is_err(), "{yaml}");
        }
    }

    #[tokio::test]
    async fn should_embed_with_titan_v2_at_its_default_size_on_bedrock() {
        let config: EmbeddingConfig = serde_yaml::from_str(
            "provider: bedrock\nmodel: amazon.titan-embed-text-v2:0\nregion: eu-west-1\n",
        )
        .unwrap();
        let embedder = Embedder::new(&config).unwrap();
        assert_eq!(embedder.model_id(), "bedrock:amazon.titan-embed-text-v2:0");
        assert_eq!(embedder.dimensions().await.unwrap(), 1024);
    }
}
