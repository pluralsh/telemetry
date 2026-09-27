use std::{
    collections::HashSet,
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::http::{HeaderMap, header::AUTHORIZATION};
use base64::{Engine, engine::general_purpose::STANDARD};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::Jwk};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, RwLock};

use crate::config::{Access, Credential, JwksSource, JwtConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Read,
    Write,
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
        data.claims.permission == permission
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
    global: &Access,
    namespace_access: &Access,
    jwt: Option<&JwtAuthenticator>,
    namespace: &str,
    permission: Permission,
) -> bool {
    let global = credentials(global, permission);
    let namespace_credentials = credentials(namespace_access, permission);
    if global.is_empty() && namespace_credentials.is_empty() && jwt.is_none() {
        return true;
    }
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
        .iter()
        .chain(namespace_credentials.iter())
        .any(|credential| credential_matches(credential, header))
}

fn credentials(access: &Access, permission: Permission) -> &[Credential] {
    match permission {
        Permission::Read => &access.read,
        Permission::Write => &access.write,
    }
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

pub fn secure_eq(left: &str, right: &str) -> bool {
    let left = blake3::hash(left.as_bytes());
    let right = blake3::hash(right.as_bytes());
    bool::from(left.as_bytes().ct_eq(right.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use axum::http::HeaderValue;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::{Map, json};

    use super::*;
    use crate::config::Secret;

    const KID: &str = "test-hs256";
    const SECRET: &[u8] = b"meter-test-only-hs256-signing-key";

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
    async fn local_jwks_authorizes_namespace_regex_and_exact_permission() {
        let jwt = authenticator(None, None).await;
        let read = token(Some(KID), r"^tenant-[0-9]+$", "read", future(), None, None);
        assert!(
            authorize(
                &bearer(&read),
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
            !authorize(
                &bearer(&write),
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
        let jwt = authenticator(Some("meter-issuer"), Some("meter-api")).await;
        for (issuer, audience, expected) in [
            (Some("meter-issuer"), Some("meter-api"), true),
            (Some("other"), Some("meter-api"), false),
            (Some("meter-issuer"), Some("other"), false),
            (None, Some("meter-api"), false),
        ] {
            let value = token(Some(KID), "^tenant$", "read", future(), issuer, audience);
            assert_eq!(
                authorize(
                    &bearer(&value),
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
                &global,
                &namespace,
                Some(&jwt),
                "tenant",
                Permission::Write,
            )
            .await
        );
        assert!(
            !authorize(
                &basic("tenant-writer", "tenant-password"),
                &global,
                &namespace,
                Some(&jwt),
                "tenant",
                Permission::Read,
            )
            .await
        );
    }
}
