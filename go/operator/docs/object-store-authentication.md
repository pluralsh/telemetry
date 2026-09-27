# Object-store authentication

Object-store credentials are referenced from Secrets in the Meter namespace and
injected directly into managed Meter containers. They are not written to the
generated Meter configuration Secret.

The operator creates a ServiceAccount named after each Meter. Cloud identity
annotations can be configured declaratively:

```yaml
spec:
  serviceAccount:
    annotations:
      cloud-provider.example/identity: meter
```

## AWS S3

Omit credential references to use the standard AWS credential chain, including
IRSA, ECS, and EC2 metadata. Static credentials and S3-compatible endpoints can
be configured as follows:

```yaml
spec:
  config:
    storage:
      objectStore:
        type: Aws
        aws:
          region: us-east-1
          bucket: meter
          endpoint: https://s3.example.com
          virtualHostedStyle: false
          accessKeyIDSecretRef:
            name: meter-s3
            key: access-key-id
          secretAccessKeySecretRef:
            name: meter-s3
            key: secret-access-key
          sessionTokenSecretRef:
            name: meter-s3
            key: session-token
```

Set `allowHTTP: true` only when a trusted S3-compatible endpoint does not
support TLS.

For EKS IRSA, annotate the managed ServiceAccount and omit static credentials:

```yaml
spec:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/meter
```

EKS Pod Identity uses an external association with the Meter ServiceAccount and
does not require this annotation.

## Azure Blob Storage

Omit explicit authentication to use managed identity. Account-key
authentication uses:

```yaml
spec:
  config:
    storage:
      objectStore:
        type: Azure
        azure:
          account: telemetry
          container: meter
          accessKeySecretRef:
            name: meter-azure
            key: account-key
```

Replace `accessKeySecretRef` with exactly one of the following authentication
methods:

```yaml
# Shared access signature
sasTokenSecretRef:
  name: meter-azure
  key: sas-token

# Static OAuth bearer token
bearerTokenSecretRef:
  name: meter-azure
  key: bearer-token

# Service principal
clientSecret:
  clientID: 00000000-0000-0000-0000-000000000000
  tenantID: 00000000-0000-0000-0000-000000000000
  clientSecretKeyRef:
    name: meter-azure
    key: client-secret

# Workload identity; mount the projected token with writer/reader podTemplate
workloadIdentity:
  clientID: 00000000-0000-0000-0000-000000000000
  tenantID: 00000000-0000-0000-0000-000000000000
  tokenFile: /var/run/secrets/azure/tokens/azure-identity-token
```

`endpoint` and `allowHTTP` support Azurite and other custom endpoints.

Azure workload identity commonly uses
`serviceAccount.annotations.azure.workload.identity/client-id` together with
the workload identity webhook's required pod labels or an explicitly projected
token in `podTemplate`.

## Google Cloud Storage

Omit explicit authentication to use application default credentials, including
GKE workload identity. A service-account JSON Secret can be referenced with:

```yaml
spec:
  config:
    storage:
      objectStore:
        type: Gcp
        gcp:
          bucket: meter
          serviceAccountKeySecretRef:
            name: meter-gcp
            key: service-account.json
```

For a static OAuth token, replace `serviceAccountKeySecretRef` with
`bearerTokenSecretRef`. `baseURL` can override the API URL for an emulator.

For the GKE service-account linking flow, set:

```yaml
spec:
  serviceAccount:
    annotations:
      iam.gke.io/gcp-service-account: meter@my-project.iam.gserviceaccount.com
```

The underlying Google object-store client obtains GKE workload identity
credentials from the metadata server when explicit credentials are omitted.
