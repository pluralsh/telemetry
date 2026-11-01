//! Namespace authentication for the cluster API.
//!
//! The `auth` and `namespaces` sections of the cluster config follow the
//! other Telemetry servers: Basic credentials per namespace and globally,
//! JWTs whose `namespace` claim matches, and an internal token replicas
//! present on requests they forward to each other. Without an `auth`
//! section every request is allowed.
//!
//! With `auth`, a namespace has to be declared under `namespaces` (the
//! operator declares one per NamespaceAuthentication) unless
//! `auth.unauthenticated` is set. Read access covers queries, rows,
//! metadata and schema reads; write access also covers creating, deleting
//! and configuring the namespace and writing to it. Shard listing and
//! splitting need global credentials. Both sections are reloaded from the
//! config file while the server runs.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use axum::http::HeaderMap;
use serde::Deserialize;
use server_common::auth::{Access, AuthConfig, JwtAuthenticator, Permission, authorize, secure_eq};
use server_common::reload::{self, Live};
use tokio_util::sync::CancellationToken;

use super::cluster::FORWARDED_HEADER;
use super::error::ApiError;
use crate::Error;
use crate::VectorCluster;
use crate::cluster::{AttributeSchema, DeclaredSchemas};

/// Carries the internal token on requests one replica forwards to another.
pub const INTERNAL_TOKEN_HEADER: &str = "x-vector-internal-token";

/// The access settings of a cluster config file.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AccessConfig {
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub namespaces: Vec<NamespaceAccess>,
}

/// A declared namespace, the credentials that may use it, and the
/// attribute schema it is kept merged with.
#[derive(Debug, Clone, Deserialize)]
pub struct NamespaceAccess {
    pub name: String,
    #[serde(default)]
    pub auth: Access,
    #[serde(default)]
    pub schema: BTreeMap<String, AttributeSchema>,
}

impl AccessConfig {
    pub fn from_yaml(raw: &str) -> Result<Self, String> {
        serde_yaml::from_str(raw).map_err(|e| format!("invalid access config: {e}"))
    }

    /// The schemas declared for namespaces, by namespace name.
    pub fn schemas(&self) -> DeclaredSchemas {
        let mut schemas = DeclaredSchemas::new();
        for namespace in self.namespaces.iter().filter(|ns| !ns.schema.is_empty()) {
            schemas
                .entry(namespace.name.clone())
                .or_default()
                .extend(namespace.schema.clone());
        }
        schemas
    }
}

#[derive(Default)]
struct Policy {
    enforced: bool,
    unauthenticated: bool,
    global: Access,
    namespaces: HashMap<String, Access>,
}

impl Policy {
    fn new(config: &AccessConfig) -> Self {
        let Some(auth) = &config.auth else {
            return Self::default();
        };
        Self {
            enforced: true,
            unauthenticated: auth.unauthenticated,
            global: auth.global.clone(),
            namespaces: config
                .namespaces
                .iter()
                .map(|ns| (ns.name.clone(), ns.auth.clone()))
                .collect(),
        }
    }
}

/// Decides which requests the cluster API serves.
pub struct Guard {
    policy: Live<Policy>,
    jwt: Option<JwtAuthenticator>,
    internal: Option<String>,
}

impl Guard {
    /// Allows every request.
    pub fn open() -> Self {
        Self {
            policy: Live::new(Policy::default()),
            jwt: None,
            internal: None,
        }
    }

    pub async fn new(config: &AccessConfig) -> anyhow::Result<Self> {
        let auth = config.auth.as_ref();
        let jwt = match auth.and_then(|a| a.jwt.clone()) {
            Some(jwt) => Some(JwtAuthenticator::open(jwt).await?),
            None => None,
        };
        let internal = match auth {
            Some(auth) => auth.internal_token()?,
            None => None,
        };
        Ok(Self {
            policy: Live::new(Policy::new(config)),
            jwt,
            internal,
        })
    }

    /// The token this replica puts on requests it forwards.
    pub(crate) fn internal_token(&self) -> Option<&str> {
        self.internal.as_deref()
    }

    /// Applies the namespaces, credentials and namespace schemas of each new
    /// revision of the config file at `path`, the schemas to `cluster`. The
    /// JWT and internal token settings, like the rest of the file, apply on
    /// the next restart.
    pub fn watch(
        self: &Arc<Self>,
        path: PathBuf,
        running: serde_json::Value,
        cluster: Arc<VectorCluster>,
    ) {
        let guard = Arc::clone(self);
        reload::spawn_config_watcher(
            "vector",
            path,
            reload::POLL_INTERVAL,
            CancellationToken::new(),
            move |raw| {
                let guard = Arc::clone(&guard);
                let cluster = Arc::clone(&cluster);
                let running = running.clone();
                async move {
                    let next: serde_json::Value =
                        serde_yaml::from_str(&raw).map_err(|e| e.to_string())?;
                    let config = AccessConfig::from_yaml(&raw)?;
                    if config.auth.is_some() != guard.policy.load().enforced {
                        return Ok(vec!["auth".into()]);
                    }
                    guard.policy.store(Policy::new(&config));
                    cluster.declare_schemas(config.schemas());
                    Ok(reload::restart_required(&running, &next))
                }
            },
        );
    }

    /// Whether the request was forwarded by another replica, which already
    /// authorized it. A forwarded request without the internal token is
    /// rejected.
    fn trusted(&self, headers: &HeaderMap) -> Result<bool, ApiError> {
        if !headers.contains_key(FORWARDED_HEADER) {
            return Ok(false);
        }
        self.internal(headers)?;
        Ok(true)
    }

    /// Admits replica-to-replica requests.
    pub(crate) fn internal(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        if !self.policy.load().enforced {
            return Ok(());
        }
        let presented = headers
            .get(INTERNAL_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok());
        match (presented, self.internal.as_deref()) {
            (Some(presented), Some(expected)) if secure_eq(presented, expected) => Ok(()),
            _ => Err(unauthorized("missing or invalid internal token")),
        }
    }

    /// Admits an operation needing `permission` on namespace `ns`.
    pub(crate) async fn namespace(
        &self,
        headers: &HeaderMap,
        ns: &str,
        permission: Permission,
    ) -> Result<(), ApiError> {
        if self.trusted(headers)? {
            return Ok(());
        }
        let policy = self.policy.load();
        if !policy.enforced {
            return Ok(());
        }
        let access = policy.namespaces.get(ns);
        if access.is_none() && !policy.unauthenticated {
            return Err(ApiError(Error::NotFound(format!(
                "unknown namespace '{ns}'"
            ))));
        }
        let none = Access::default();
        let access = access.unwrap_or(&none);
        if authorize(
            headers,
            policy.unauthenticated,
            &policy.global,
            access,
            self.jwt.as_ref(),
            ns,
            permission,
        )
        .await
        {
            Ok(())
        } else {
            Err(unauthorized(&format!(
                "not authorized for namespace '{ns}'"
            )))
        }
    }

    /// Whether a caller may see namespace `ns` in a listing.
    pub(crate) async fn visible(&self, headers: &HeaderMap, ns: &str) -> bool {
        self.namespace(headers, ns, Permission::Read).await.is_ok()
    }

    /// Admits a cluster-wide operation, which needs global credentials.
    pub(crate) async fn admin(
        &self,
        headers: &HeaderMap,
        permission: Permission,
    ) -> Result<(), ApiError> {
        if self.trusted(headers)? {
            return Ok(());
        }
        let policy = self.policy.load();
        if !policy.enforced {
            return Ok(());
        }
        if authorize(
            headers,
            policy.unauthenticated,
            &policy.global,
            &Access::default(),
            self.jwt.as_ref(),
            "",
            permission,
        )
        .await
        {
            Ok(())
        } else {
            Err(unauthorized("global credentials required"))
        }
    }
}

fn unauthorized(message: &str) -> ApiError {
    ApiError(Error::Unauthorized(message.into()))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;
    use axum::http::header::AUTHORIZATION;
    use base64::Engine;

    use super::*;

    const CONFIG: &str = r#"
storage:
  type: InMemory
auth:
  unauthenticated: false
  global:
    write:
      - {type: basic, username: admin, password: {source: literal, value: root}}
  internal: {source: literal, value: peer-token}
namespaces:
  - name: docs
    auth:
      read:
        - {type: basic, username: reader, password: {source: literal, value: r}}
      write:
        - {type: basic, username: writer, password: {source: literal, value: w}}
  - name: open
"#;

    fn basic(user: &str, password: &str) -> HeaderMap {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );
        headers
    }

    async fn guard(raw: &str) -> Guard {
        Guard::new(&AccessConfig::from_yaml(raw).unwrap())
            .await
            .unwrap()
    }

    fn status(result: Result<(), ApiError>) -> u16 {
        use axum::response::IntoResponse;
        match result {
            Ok(()) => 200,
            Err(e) => e.into_response().status().as_u16(),
        }
    }

    #[tokio::test]
    async fn should_allow_everything_without_an_auth_section() {
        let guard = guard("storage:\n  type: InMemory\n").await;
        let none = HeaderMap::new();
        assert_eq!(
            status(guard.namespace(&none, "any", Permission::Write).await),
            200
        );
        assert_eq!(status(guard.admin(&none, Permission::Write).await), 200);
        let mut forwarded = HeaderMap::new();
        forwarded.insert(FORWARDED_HEADER, HeaderValue::from_static("1"));
        assert_eq!(status(guard.internal(&forwarded)), 200);
    }

    #[tokio::test]
    async fn should_enforce_namespace_credentials_and_permissions() {
        let guard = guard(CONFIG).await;
        let reader = basic("reader", "r");
        let writer = basic("writer", "w");
        let admin = basic("admin", "root");
        let check = |headers: HeaderMap, ns: &'static str, permission| {
            let guard = &guard;
            async move { status(guard.namespace(&headers, ns, permission).await) }
        };

        assert_eq!(check(reader.clone(), "docs", Permission::Read).await, 200);
        assert_eq!(check(reader.clone(), "docs", Permission::Write).await, 401);
        assert_eq!(check(writer.clone(), "docs", Permission::Write).await, 200);
        assert_eq!(check(writer.clone(), "docs", Permission::Read).await, 200);
        assert_eq!(
            check(basic("reader", "x"), "docs", Permission::Read).await,
            401
        );
        assert_eq!(check(HeaderMap::new(), "docs", Permission::Read).await, 401);
        // Credentials of one namespace don't reach another.
        assert_eq!(check(writer.clone(), "open", Permission::Read).await, 401);
        assert_eq!(check(admin.clone(), "open", Permission::Write).await, 200);
        // Undeclared namespaces don't exist, even for global credentials.
        assert_eq!(check(admin.clone(), "other", Permission::Read).await, 404);

        assert_eq!(status(guard.admin(&admin, Permission::Write).await), 200);
        assert_eq!(status(guard.admin(&writer, Permission::Read).await), 401);
        assert!(guard.visible(&reader, "docs").await);
        assert!(!guard.visible(&reader, "open").await);
    }

    #[tokio::test]
    async fn should_trust_forwarded_requests_only_with_the_internal_token() {
        let guard = guard(CONFIG).await;
        let mut forwarded = HeaderMap::new();
        forwarded.insert(FORWARDED_HEADER, HeaderValue::from_static("1"));
        assert_eq!(
            status(guard.namespace(&forwarded, "docs", Permission::Write).await),
            401
        );
        forwarded.insert(INTERNAL_TOKEN_HEADER, HeaderValue::from_static("wrong"));
        assert_eq!(
            status(guard.namespace(&forwarded, "docs", Permission::Write).await),
            401
        );
        forwarded.insert(
            INTERNAL_TOKEN_HEADER,
            HeaderValue::from_static("peer-token"),
        );
        assert_eq!(
            status(guard.namespace(&forwarded, "docs", Permission::Write).await),
            200
        );
        assert_eq!(
            status(guard.admin(&forwarded, Permission::Write).await),
            200
        );
        assert_eq!(guard.internal_token(), Some("peer-token"));
    }

    #[tokio::test]
    async fn should_allow_undeclared_namespaces_when_unauthenticated() {
        let guard = guard(&CONFIG.replace("unauthenticated: false", "unauthenticated: true")).await;
        let none = HeaderMap::new();
        assert_eq!(
            status(guard.namespace(&none, "other", Permission::Write).await),
            200
        );
        assert_eq!(status(guard.admin(&none, Permission::Write).await), 200);
    }

    #[test]
    fn should_collect_the_schemas_declared_for_namespaces() {
        // given
        let raw = r#"
namespaces:
  - name: docs
    schema:
      body: { type: string, full_text_search: true }
  - name: docs
    schema:
      year: { type: int }
  - name: open
"#;

        // when
        let schemas = AccessConfig::from_yaml(raw).unwrap().schemas();

        // then
        assert_eq!(schemas.keys().collect::<Vec<_>>(), ["docs"]);
        assert!(schemas["docs"]["body"].full_text_search);
        assert_eq!(
            schemas["docs"]["year"].attribute_type,
            crate::cluster::AttributeType::Int
        );
    }

    #[tokio::test]
    async fn should_apply_reloaded_namespaces_and_credentials() {
        let guard = guard(CONFIG).await;
        let running: serde_json::Value = serde_yaml::from_str(CONFIG).unwrap();
        let policy = |raw: &str| Policy::new(&AccessConfig::from_yaml(raw).unwrap());

        let next = CONFIG.replace("name: open", "name: added");
        guard.policy.store(policy(&next));
        let admin = basic("admin", "root");
        assert_eq!(
            status(guard.namespace(&admin, "added", Permission::Read).await),
            200
        );
        assert_eq!(
            status(guard.namespace(&admin, "open", Permission::Read).await),
            404
        );
        let next: serde_json::Value = serde_yaml::from_str(&next).unwrap();
        assert!(reload::restart_required(&running, &next).is_empty());
    }
}
