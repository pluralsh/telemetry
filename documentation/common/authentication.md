# Authentication and tenancy

Every public data route names a configured namespace. Unknown namespaces return
`404`. Read credentials authorize reads only; write credentials authorize both
reads and writes.

```text
request for namespace X
    ├── anonymous allowed? ───────────────► allow
    ├── matching global/namespace Basic? ► allow
    ├── valid JWT for X + permission? ───► allow
    └── otherwise ───────────────────────► 401/403
```

Anonymous access is denied by default. Empty credential lists do not make a
namespace public.

## Basic authentication

Basic credentials may be global or namespace-specific, and are assigned to
either `read` or `write`. The `write` permission includes `read`. Passwords can
come from a literal, environment
variable, or UTF-8 file. File values have trailing CR/LF removed and secrets
are redacted from debug output. Prefer mounted files or environment-backed
secrets over literals.

## JWT/JWKS

Bearer JWT verification is enabled by `auth.jwt`. JWKS can be loaded from a
file at startup or from an HTTP(S) URL. URL keys refresh lazily; an unknown
`kid` can trigger a rate-limited early refresh, and a failed refresh preserves
the last valid key set.

Required claims:

- `exp`: valid expiration.
- `namespace`: Rust regular expression matching the requested namespace. Use
  anchors, for example `^tenant-a$`, when exact matching is intended.
- `permission`: exactly `read` or `write`; `write` also authorizes reads.

`nbf` is checked when present. `iss` and `aud` become required when configured.
The JWT header must contain a known `kid` and an algorithm matching the JWK.

## Internal traffic and Kubernetes

`auth.internal` is a separate shared Bearer token for writer-to-writer gRPC; it
does not authorize public APIs. Configure the same value on every writer and
reader that can forward requests.

The operator can render Basic credentials and JWT settings from Kubernetes
Secrets. `NamespaceAuthentication` resources can target Meter, Line, and Track;
other credentials can be supplied through each datastore's main config.
