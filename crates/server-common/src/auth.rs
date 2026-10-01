//! Credential configuration and request authorization shared by the product
//! servers: Basic credentials, JWT bearer tokens and the internal secret.

use std::{
    collections::HashSet,
    env, fmt, fs,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::http::{HeaderMap, header::AUTHORIZATION};
use base64::{Engine, engine::general_purpose::STANDARD};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::Jwk};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot resolve secret: {0}")]
pub struct SecretError(String);

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum Secret {
    Literal { value: String },
    Env { name: String },
    File { path: String },
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Secret([REDACTED])")
    }
}

impl Secret {
    pub fn expose(&self) -> Result<String, SecretError> {
        let value = match self {
            Self::Literal { value } => value.clone(),
            Self::Env { name } => env::var(name)
                .map_err(|_| SecretError(format!("environment variable {name} is unset")))?,
            Self::File { path } => fs::read_to_string(path)
                .map_err(|error| SecretError(format!("cannot read {path}: {error}")))?,
        };
        let value = value.trim_end_matches(['\r', '\n']).to_owned();
        if value.is_empty() {
            return Err(SecretError("secret cannot be empty".to_owned()));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credential {
    Basic { username: String, password: Secret },
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Access {
    pub read: Vec<Credential>,
    pub write: Vec<Credential>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    pub unauthenticated: bool,
    pub jwt: Option<JwtConfig>,
    pub global: Access,
    pub internal: Option<Secret>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct JwtConfig {
    pub jwks: JwksSource,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub refresh_interval_seconds: u64,
    pub request_timeout_seconds: u64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            jwks: JwksSource::File {
                path: String::new(),
            },
            issuer: None,
            audience: None,
            refresh_interval_seconds: 300,
            request_timeout_seconds: 5,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum JwksSource {
    File { path: String },
    Url { url: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Read,
    Write,
}

impl Permission {
    fn allows(self, requested: Self) -> bool {
        self == Self::Write || self == requested
    }
}

#[derive(Clone)]
pub struct JwtAuthenticator {
    inner: Arc<JwtAuthenticatorInner>,
}

struct JwtAuthenticatorInner {
    config: JwtConfig,
    client: reqwest::Client,
    cache: RwLock<JwksCache>,
    refresh: Mutex<()>,
}

struct JwksCache {
    keys: Vec<JwtKey>,
    generation: u64,
    refreshed_at: Instant,
    unknown_kid_refreshed_at: Option<Instant>,
}

struct JwtKey {
    kid: String,
    algorithm: Algorithm,
    decoding_key: DecodingKey,
}

#[derive(Deserialize)]
struct Claims {
    exp: u64,
    #[serde(default)]
    nbf: Option<u64>,
    namespace: String,
    permission: Permission,
}

impl JwtAuthenticator {
    pub async fn open(config: JwtConfig) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .build()?;
        let keys = load_jwks(&client, &config.jwks).await?;
        Ok(Self {
            inner: Arc::new(JwtAuthenticatorInner {
                config,
                client,
                cache: RwLock::new(JwksCache {
                    keys,
                    generation: 1,
                    refreshed_at: Instant::now(),
                    unknown_kid_refreshed_at: None,
                }),
                refresh: Mutex::new(()),
            }),
        })
    }

    async fn authorize(&self, token: &str, namespace: &str, permission: Permission) -> bool {
        self.refresh_if_due().await;
        let Ok(header) = decode_header(token) else {
            return false;
        };
        let Some(kid) = header.kid.as_deref() else {
            return false;
        };
        let generation = self.inner.cache.read().await.generation;
        let mut key = self.key(kid, header.alg).await;
        if key.is_none() && matches!(self.inner.config.jwks, JwksSource::Url { .. }) {
            self.refresh_generation(generation, true).await;
            key = self.key(kid, header.alg).await;
        }
        let Some(key) = key else {
            return false;
        };
        let mut validation = Validation::new(header.alg);
        validation.validate_nbf = true;
        if let Some(issuer) = &self.inner.config.issuer {
            validation.set_issuer(&[issuer]);
            validation.required_spec_claims.insert("iss".to_owned());
        }
        if let Some(audience) = &self.inner.config.audience {
            validation.set_audience(&[audience]);
            validation.required_spec_claims.insert("aud".to_owned());
        } else {
            validation.validate_aud = false;
        }
        let Ok(data) = decode::<Claims>(token, &key, &validation) else {
            return false;
        };
        let _ = (data.claims.exp, data.claims.nbf);
        data.claims.permission.allows(permission)
            && Regex::new(&data.claims.namespace).is_ok_and(|pattern| pattern.is_match(namespace))
    }

    async fn key(&self, kid: &str, algorithm: Algorithm) -> Option<DecodingKey> {
        self.inner
            .cache
            .read()
            .await
            .keys
            .iter()
            .find(|key| key.kid == kid && key.algorithm == algorithm)
            .map(|key| key.decoding_key.clone())
    }

    async fn refresh_if_due(&self) {
        if !matches!(self.inner.config.jwks, JwksSource::Url { .. }) {
            return;
        }
        let cache = self.inner.cache.read().await;
        let generation = cache.generation;
        let due = cache.refreshed_at.elapsed()
            >= Duration::from_secs(self.inner.config.refresh_interval_seconds);
        drop(cache);
        if due {
            self.refresh_generation(generation, false).await;
        }
    }

    async fn refresh_generation(&self, generation: u64, unknown_kid: bool) {
        let _refresh = self.inner.refresh.lock().await;
        let cache = self.inner.cache.read().await;
        if cache.generation != generation
            || (!unknown_kid
                && cache.refreshed_at.elapsed()
                    < Duration::from_secs(self.inner.config.refresh_interval_seconds))
            || (unknown_kid
                && cache.unknown_kid_refreshed_at.is_some_and(|refreshed_at| {
                    refreshed_at.elapsed()
                        < Duration::from_secs(self.inner.config.refresh_interval_seconds)
                }))
        {
            return;
        }
        drop(cache);
        if unknown_kid {
            self.inner.cache.write().await.unknown_kid_refreshed_at = Some(Instant::now());
        }
        match load_jwks(&self.inner.client, &self.inner.config.jwks).await {
            Ok(keys) => {
                let mut cache = self.inner.cache.write().await;
                cache.keys = keys;
                cache.generation = cache.generation.wrapping_add(1);
                cache.refreshed_at = Instant::now();
                cache.unknown_kid_refreshed_at = Some(Instant::now());
            }
            Err(error) => {
                tracing::warn!(%error, "failed to refresh JWT JWKS");
            }
        }
    }
}

pub async fn authorize(
    headers: &HeaderMap,
    unauthenticated: bool,
    global: &Access,
    namespace_access: &Access,
    jwt: Option<&JwtAuthenticator>,
    namespace: &str,
    permission: Permission,
) -> bool {
    if unauthenticated {
        return true;
    }
    let global = credentials(global, permission);
    let namespace_credentials = credentials(namespace_access, permission);
    let Some(header) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    if let Some(token) = header.strip_prefix("Bearer ") {
        if token.is_empty() {
            return false;
        }
        return match jwt {
            Some(authenticator) => authenticator.authorize(token, namespace, permission).await,
            None => false,
        };
    }
    global
        .chain(namespace_credentials)
        .any(|credential| credential_matches(credential, header))
}

fn credentials(access: &Access, permission: Permission) -> impl Iterator<Item = &Credential> {
    access
        .read
        .iter()
        .filter(move |_| permission == Permission::Read)
        .chain(access.write.iter())
}

fn credential_matches(credential: &Credential, header: &str) -> bool {
    let Credential::Basic { username, password } = credential;
    let Some(encoded) = header.strip_prefix("Basic ") else {
        return false;
    };
    let Ok(decoded) = STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(decoded) = String::from_utf8(decoded) else {
        return false;
    };
    let Some((provided_user, provided_password)) = decoded.split_once(':') else {
        return false;
    };
    password.expose().is_ok_and(|expected| {
        secure_eq(provided_user, username) & secure_eq(provided_password, &expected)
    })
}

async fn load_jwks(client: &reqwest::Client, source: &JwksSource) -> anyhow::Result<Vec<JwtKey>> {
    let bytes = match source {
        JwksSource::File { path } => fs::read(path)?,
        JwksSource::Url { url } => client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?
            .to_vec(),
    };
    parse_jwks(&bytes)
}

fn parse_jwks(bytes: &[u8]) -> anyhow::Result<Vec<JwtKey>> {
    let document: Value = serde_json::from_slice(bytes)?;
    let values = document
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("JWKS must contain a keys array"))?;
    if values.is_empty() {
        anyhow::bail!("JWKS cannot be empty");
    }
    let mut kids = HashSet::new();
    values
        .iter()
        .map(|value| {
            let kid = value
                .get("kid")
                .and_then(Value::as_str)
                .filter(|kid| !kid.is_empty())
                .ok_or_else(|| anyhow::anyhow!("every JWK must have a non-empty kid"))?;
            if !kids.insert(kid.to_owned()) {
                anyhow::bail!("duplicate JWK kid");
            }
            let algorithm = value
                .get("alg")
                .and_then(Value::as_str)
                .and_then(parse_algorithm)
                .ok_or_else(|| anyhow::anyhow!("every JWK must have a supported alg"))?;
            let jwk: Jwk = serde_json::from_value(value.clone())?;
            let decoding_key = DecodingKey::from_jwk(&jwk)?;
            Ok(JwtKey {
                kid: kid.to_owned(),
                algorithm,
                decoding_key,
            })
        })
        .collect()
}

fn parse_algorithm(value: &str) -> Option<Algorithm> {
    match value {
        "HS256" => Some(Algorithm::HS256),
        "HS384" => Some(Algorithm::HS384),
        "HS512" => Some(Algorithm::HS512),
        "ES256" => Some(Algorithm::ES256),
        "ES384" => Some(Algorithm::ES384),
        "RS256" => Some(Algorithm::RS256),
        "RS384" => Some(Algorithm::RS384),
        "RS512" => Some(Algorithm::RS512),
        "PS256" => Some(Algorithm::PS256),
        "PS384" => Some(Algorithm::PS384),
        "PS512" => Some(Algorithm::PS512),
        "EdDSA" => Some(Algorithm::EdDSA),
        _ => None,
    }
}

pub use crate::internal_rpc::secure_eq;

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use axum::http::HeaderValue;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::{Map, json};

    use super::*;

    const KID: &str = "test-hs256";
    const SECRET: &[u8] = b"metrics-test-only-hs256-signing-key";

    async fn authenticator(issuer: Option<&str>, audience: Option<&str>) -> JwtAuthenticator {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jwks.json");
        let encoded = URL_SAFE_NO_PAD.encode(SECRET);
        fs::write(
            &path,
            json!({"keys": [{
                "kty": "oct",
                "kid": KID,
                "alg": "HS256",
                "use": "sig",
                "k": encoded
            }]})
            .to_string(),
        )
        .unwrap();
        JwtAuthenticator::open(JwtConfig {
            jwks: JwksSource::File {
                path: path.display().to_string(),
            },
            issuer: issuer.map(ToOwned::to_owned),
            audience: audience.map(ToOwned::to_owned),
            ..JwtConfig::default()
        })
        .await
        .unwrap()
    }

    fn token(
        kid: Option<&str>,
        namespace: &str,
        permission: &str,
        expires_at: u64,
        issuer: Option<&str>,
        audience: Option<&str>,
    ) -> String {
        let mut claims = Map::from_iter([
            ("exp".to_owned(), json!(expires_at)),
            ("namespace".to_owned(), json!(namespace)),
            ("permission".to_owned(), json!(permission)),
        ]);
        if let Some(issuer) = issuer {
            claims.insert("iss".to_owned(), json!(issuer));
        }
        if let Some(audience) = audience {
            claims.insert("aud".to_owned(), json!(audience));
        }
        let mut header = Header::new(Algorithm::HS256);
        header.kid = kid.map(ToOwned::to_owned);
        encode(&header, &claims, &EncodingKey::from_secret(SECRET)).unwrap()
    }

    fn token_not_before(not_before: u64) -> String {
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(KID.to_owned());
        encode(
            &header,
            &json!({
                "exp": future(),
                "nbf": not_before,
                "namespace": ".*",
                "permission": "read"
            }),
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap()
    }

    fn bearer(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {value}")).unwrap(),
        );
        headers
    }

    fn basic(username: &str, password: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!(
                "Basic {}",
                STANDARD.encode(format!("{username}:{password}"))
            ))
            .unwrap(),
        );
        headers
    }

    fn future() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300
    }

    #[tokio::test]
    async fn local_jwks_authorizes_namespace_regex_and_permission_hierarchy() {
        let jwt = authenticator(None, None).await;
        let read = token(Some(KID), r"^tenant-[0-9]+$", "read", future(), None, None);
        assert!(
            authorize(
                &bearer(&read),
                false,
                &Access::default(),
                &Access::default(),
                Some(&jwt),
                "tenant-42",
                Permission::Read,
            )
            .await
        );
        assert!(
            !authorize(
                &bearer(&read),
                false,
                &Access::default(),
                &Access::default(),
                Some(&jwt),
                "other",
                Permission::Read,
            )
            .await
        );
        assert!(
            !authorize(
                &bearer(&read),
                false,
                &Access::default(),
                &Access::default(),
                Some(&jwt),
                "tenant-42",
                Permission::Write,
            )
            .await
        );
        let write = token(Some(KID), "^tenant-42$", "write", future(), None, None);
        assert!(
            authorize(
                &bearer(&write),
                false,
                &Access::default(),
                &Access::default(),
                Some(&jwt),
                "tenant-42",
                Permission::Read,
            )
            .await
        );
    }

    #[tokio::test]
    async fn jwt_rejects_expired_unknown_kid_and_malformed_tokens() {
        let jwt = authenticator(None, None).await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for token in [
            token(Some(KID), ".*", "read", now - 120, None, None),
            token(Some("unknown"), ".*", "read", future(), None, None),
            token(None, ".*", "read", future(), None, None),
            token(Some(KID), ".*", "admin", future(), None, None),
            token_not_before(now + 120),
            "not-a-jwt".to_owned(),
        ] {
            assert!(
                !authorize(
                    &bearer(&token),
                    false,
                    &Access::default(),
                    &Access::default(),
                    Some(&jwt),
                    "tenant",
                    Permission::Read,
                )
                .await
            );
        }
    }

    #[tokio::test]
    async fn jwt_enforces_configured_issuer_and_audience() {
        let jwt = authenticator(Some("metrics-issuer"), Some("metrics-api")).await;
        for (issuer, audience, expected) in [
            (Some("metrics-issuer"), Some("metrics-api"), true),
            (Some("other"), Some("metrics-api"), false),
            (Some("metrics-issuer"), Some("other"), false),
            (None, Some("metrics-api"), false),
        ] {
            let value = token(Some(KID), "^tenant$", "read", future(), issuer, audience);
            assert_eq!(
                authorize(
                    &bearer(&value),
                    false,
                    &Access::default(),
                    &Access::default(),
                    Some(&jwt),
                    "tenant",
                    Permission::Read,
                )
                .await,
                expected
            );
        }
    }

    #[tokio::test]
    async fn basic_credentials_remain_scoped_and_fallback_from_jwt() {
        let jwt = authenticator(None, None).await;
        let global = Access {
            read: vec![Credential::Basic {
                username: "global-reader".to_owned(),
                password: Secret::Literal {
                    value: "global-password".to_owned(),
                },
            }],
            write: vec![],
        };
        let namespace = Access {
            read: vec![],
            write: vec![Credential::Basic {
                username: "tenant-writer".to_owned(),
                password: Secret::Literal {
                    value: "tenant-password".to_owned(),
                },
            }],
        };
        assert!(
            authorize(
                &basic("global-reader", "global-password"),
                false,
                &global,
                &namespace,
                Some(&jwt),
                "tenant",
                Permission::Read,
            )
            .await
        );
        assert!(
            authorize(
                &basic("tenant-writer", "tenant-password"),
                false,
                &global,
                &namespace,
                Some(&jwt),
                "tenant",
                Permission::Write,
            )
            .await
        );
        assert!(
            authorize(
                &basic("tenant-writer", "tenant-password"),
                false,
                &global,
                &namespace,
                Some(&jwt),
                "tenant",
                Permission::Read,
            )
            .await
        );
        assert!(
            !authorize(
                &basic("global-reader", "global-password"),
                false,
                &global,
                &namespace,
                Some(&jwt),
                "tenant",
                Permission::Write,
            )
            .await
        );
    }

    #[tokio::test]
    async fn anonymous_access_requires_explicit_opt_in() {
        let headers = HeaderMap::new();
        assert!(
            !authorize(
                &headers,
                false,
                &Access::default(),
                &Access::default(),
                None,
                "tenant",
                Permission::Read,
            )
            .await
        );
        assert!(
            authorize(
                &headers,
                true,
                &Access::default(),
                &Access::default(),
                None,
                "tenant",
                Permission::Read,
            )
            .await
        );
    }

    #[test]
    fn secret_sources_and_redaction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        fs::write(&path, "from-file\n").unwrap();
        let secret = Secret::File {
            path: path.display().to_string(),
        };
        assert_eq!(secret.expose().unwrap(), "from-file");
        assert!(!format!("{secret:?}").contains("from-file"));
        let literal = Secret::Literal {
            value: "literal".to_owned(),
        };
        assert_eq!(literal.expose().unwrap(), "literal");
        assert!(!format!("{literal:?}").contains("literal"));
    }
}
