# API Reference

## Packages
- [telemetry.plural.sh/v1alpha1](#telemetrypluralshv1alpha1)


## telemetry.plural.sh/v1alpha1

Package v1alpha1 contains API Schema definitions for the telemetry v1alpha1 API group.

### Resource Types
- [Logs](#logs)
- [Metrics](#metrics)
- [NamespaceAuthentication](#namespaceauthentication)
- [PseudoFS](#pseudofs)
- [ShardMap](#shardmap)
- [Traces](#traces)



#### AWSObjectStoreSpec



AWSObjectStoreSpec configures an Amazon S3 or S3-compatible object store.



_Appears in:_
- [ObjectStoreSpec](#objectstorespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `region` _string_ |  |  | MinLength: 1 <br /> |
| `bucket` _string_ |  |  | MinLength: 1 <br /> |
| `endpoint` _string_ |  |  |  |
| `allowHTTP` _boolean_ |  |  |  |
| `virtualHostedStyle` _boolean_ |  |  |  |
| `accessKeyIDSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |
| `secretAccessKeySecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |
| `sessionTokenSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |


#### AccessSpec







_Appears in:_
- [AuthSpec](#authspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `read` _[BasicCredentialSpec](#basiccredentialspec) array_ |  |  |  |
| `write` _[BasicCredentialSpec](#basiccredentialspec) array_ |  |  |  |


#### AuthSpec







_Appears in:_
- [LogsConfigSpec](#logsconfigspec)
- [MetricsConfigSpec](#metricsconfigspec)
- [TracesConfigSpec](#tracesconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `unauthenticated` _boolean_ | Unauthenticated permits anonymous access to namespace HTTP APIs. | false |  |
| `global` _[AccessSpec](#accessspec)_ |  |  |  |
| `jwt` _[JWTSpec](#jwtspec)_ |  |  |  |
| `internalTokenSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |


#### AzureClientSecretAuthSpec







_Appears in:_
- [AzureObjectStoreSpec](#azureobjectstorespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `clientID` _string_ |  |  | MinLength: 1 <br /> |
| `tenantID` _string_ |  |  | MinLength: 1 <br /> |
| `clientSecretKeyRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |


#### AzureObjectStoreSpec







_Appears in:_
- [ObjectStoreSpec](#objectstorespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `account` _string_ |  |  | MinLength: 1 <br /> |
| `container` _string_ |  |  | MinLength: 1 <br /> |
| `endpoint` _string_ |  |  |  |
| `allowHTTP` _boolean_ |  |  |  |
| `accessKeySecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |
| `sasTokenSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |
| `bearerTokenSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |
| `clientSecret` _[AzureClientSecretAuthSpec](#azureclientsecretauthspec)_ |  |  |  |
| `workloadIdentity` _[AzureWorkloadIdentityAuthSpec](#azureworkloadidentityauthspec)_ |  |  |  |


#### AzureWorkloadIdentityAuthSpec







_Appears in:_
- [AzureObjectStoreSpec](#azureobjectstorespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `clientID` _string_ |  |  | MinLength: 1 <br /> |
| `tenantID` _string_ |  |  | MinLength: 1 <br /> |
| `tokenFile` _string_ |  |  | MinLength: 1 <br /> |


#### BasicCredentialSpec







_Appears in:_
- [AccessSpec](#accessspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `username` _string_ |  |  | MinLength: 1 <br /> |
| `passwordSecretKeyRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |


#### CacheSpec







_Appears in:_
- [StorageSpec](#storagespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _[CacheType](#cachetype)_ |  |  | Enum: [FoyerHybrid FoyerMemory] <br /> |
| `memoryCapacity` _integer_ |  |  |  |
| `diskCapacity` _integer_ |  |  |  |
| `diskPath` _string_ |  |  |  |
| `capacity` _integer_ |  |  |  |
| `shards` _integer_ |  |  |  |
| `writePolicy` _string_ |  |  |  |
| `flushers` _integer_ |  |  |  |
| `bufferPoolSize` _integer_ |  |  |  |
| `submitQueueSizeThreshold` _integer_ |  |  |  |


#### CacheType

_Underlying type:_ _string_





_Appears in:_
- [CacheSpec](#cachespec)

| Field | Description |
| --- | --- |
| `FoyerHybrid` |  |
| `FoyerMemory` |  |


#### CacheWarmerSpec







_Appears in:_
- [LogsConfigSpec](#logsconfigspec)
- [MetricsConfigSpec](#metricsconfigspec)
- [TracesConfigSpec](#tracesconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ |  | false |  |
| `warmRangeSeconds` _integer_ |  | 7200 | Minimum: 1 <br /> |
| `timeoutSeconds` _integer_ |  | 30 | Minimum: 1 <br /> |
| `concurrency` _integer_ |  | 2 | Minimum: 1 <br /> |
| `includePayloads` _boolean_ |  | false |  |


#### DataStoreReference







_Appears in:_
- [NamespaceAuthenticationSpec](#namespaceauthenticationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `kind` _string_ |  |  | Enum: [Metrics Logs Traces] <br /> |
| `name` _string_ |  |  | MinLength: 1 <br /> |


#### Durability

_Underlying type:_ _string_





_Appears in:_
- [WriteSpec](#writespec)

| Field | Description |
| --- | --- |
| `applied` |  |
| `written` |  |
| `durable` |  |


#### GCPObjectStoreSpec







_Appears in:_
- [ObjectStoreSpec](#objectstorespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucket` _string_ |  |  | MinLength: 1 <br /> |
| `baseURL` _string_ |  |  |  |
| `serviceAccountKeySecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |
| `bearerTokenSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |


#### HashRange







_Appears in:_
- [HashRangeAssignment](#hashrangeassignment)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `start` _string_ |  |  | Pattern: `^[0-9a-f]\{32\}$` <br /> |
| `end` _string_ |  |  | Pattern: `^[0-9a-f]\{32\}$` <br /> |


#### HashRangeAssignment







_Appears in:_
- [HashRangeMap](#hashrangemap)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `shard` _integer_ |  |  | Minimum: 0 <br /> |
| `range` _[HashRange](#hashrange)_ |  |  |  |


#### HashRangeMap







_Appears in:_
- [RoutingEpoch](#routingepoch)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `generation` _integer_ |  |  | Minimum: 1 <br /> |
| `assignments` _[HashRangeAssignment](#hashrangeassignment) array_ |  |  |  |


#### ImageSpec







_Appears in:_
- [LogsSpec](#logsspec)
- [MetricsSpec](#metricsspec)
- [PseudoFSSpec](#pseudofsspec)
- [TracesSpec](#tracesspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `repository` _string_ | Repository is the container image repository. It defaults to the product's<br />official GHCR repository. |  |  |
| `tag` _string_ | Tag is a deprecated alias for the enclosing resource's spec.version.<br />When both are set they must match, and spec.version takes precedence.<br />Deprecated: use spec.version. |  | Pattern: `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$` <br /> |
| `pullPolicy` _[PullPolicy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#pullpolicy-v1-core)_ | PullPolicy is the Kubernetes image pull policy. | IfNotPresent | Enum: [Always IfNotPresent Never] <br /> |


#### IngressMetadataSpec







_Appears in:_
- [IngressSpec](#ingressspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `annotations` _object (keys:string, values:string)_ |  |  |  |
| `labels` _object (keys:string, values:string)_ |  |  |  |


#### IngressSpec







_Appears in:_
- [LogsSpec](#logsspec)
- [MetricsSpec](#metricsspec)
- [TracesSpec](#tracesspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ |  |  |  |
| `hostname` _string_ |  |  |  |
| `ingressClass` _string_ |  |  |  |
| `pathPrefix` _string_ |  |  |  |
| `metadata` _[IngressMetadataSpec](#ingressmetadataspec)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `tls` _[IngressTLSSpec](#ingresstlsspec)_ |  |  |  |


#### IngressTLSSpec







_Appears in:_
- [IngressSpec](#ingressspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ |  |  |  |
| `secretName` _string_ |  |  |  |


#### JWKSSpec







_Appears in:_
- [JWTSpec](#jwtspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `url` _string_ |  |  |  |
| `secretKeyRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |


#### JWTSpec







_Appears in:_
- [AuthSpec](#authspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `jwks` _[JWKSSpec](#jwksspec)_ |  |  |  |
| `issuer` _string_ |  |  |  |
| `audience` _string_ |  |  |  |
| `refreshIntervalSeconds` _integer_ |  | 300 | Minimum: 1 <br /> |
| `requestTimeoutSeconds` _integer_ |  | 5 | Minimum: 1 <br /> |


#### Logs









| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `Logs` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[LogsSpec](#logsspec)_ |  |  |  |


#### LogsConfigSpec







_Appears in:_
- [LogsSpec](#logsspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storage` _[StorageSpec](#storagespec)_ |  | \{ path:logs \} |  |
| `segmentDurationSeconds` _integer_ |  | 3600 | Minimum: 1 <br /> |
| `retentionSeconds` _integer_ |  |  | Minimum: 1 <br /> |
| `page` _[LogsPageSpec](#logspagespec)_ |  |  |  |
| `write` _[WriteSpec](#writespec)_ |  |  |  |
| `sharding` _[ShardingSpec](#shardingspec)_ |  |  |  |
| `request` _[LogsRequestSpec](#logsrequestspec)_ |  |  |  |
| `cacheWarmer` _[CacheWarmerSpec](#cachewarmerspec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ |  |  |  |
| `namespaces` _string array_ |  | [default] |  |




#### LogsPageSpec







_Appears in:_
- [LogsConfigSpec](#logsconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `targetSizeBytes` _integer_ |  | 1048576 | Minimum: 1 <br /> |
| `maxRows` _integer_ |  | 8192 | Minimum: 1 <br /> |
| `rowsPerBlock` _integer_ |  | 256 | Minimum: 1 <br /> |


#### LogsRequestSpec







_Appears in:_
- [LogsConfigSpec](#logsconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `maxRequestBytes` _integer_ |  | 10485760 | Minimum: 1 <br /> |
| `maxQueryEntries` _integer_ |  | 5000 | Minimum: 1 <br /> |
| `maxQueryPages` _integer_ |  | 10000 | Minimum: 1 <br /> |
| `maxStructuredMetadataFields` _integer_ |  | 128 | Minimum: 1 <br /> |
| `queryConcurrency` _integer_ |  | 16 | Minimum: 1 <br /> |
| `maxInFlightQueryBytes` _integer_ |  | 134217728 | Minimum: 1 <br /> |


#### LogsSpec



LogsSpec defines the desired state of Logs.



_Appears in:_
- [Logs](#logs)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[LogsMode](#logsmode)_ |  | Standalone | Enum: [Standalone Sharded] <br /> |
| `version` _string_ | Version is the canonical Logs container image tag. It must be SemVer<br />without a leading "v". When omitted, deprecated image.tag is used, then<br />the operator's default version. |  | Pattern: `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$` <br /> |
| `image` _[ImageSpec](#imagespec)_ |  | \{ pullPolicy:IfNotPresent repository:ghcr.io/pluralsh/logs \} |  |
| `config` _[LogsConfigSpec](#logsconfigspec)_ |  |  |  |
| `writer` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `reader` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `service` _[ServiceSpec](#servicespec)_ |  | \{ grpcPort:9091 httpPort:3100 type:ClusterIP \} |  |
| `ingress` _[IngressSpec](#ingressspec)_ |  |  |  |
| `serviceAccount` _[ServiceAccountSpec](#serviceaccountspec)_ |  |  |  |




#### Metrics



Metrics is the Schema for the metrics API.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `Metrics` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[MetricsSpec](#metricsspec)_ |  |  |  |


#### MetricsConfigSpec







_Appears in:_
- [MetricsSpec](#metricsspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storage` _[StorageSpec](#storagespec)_ |  | \{ path:metrics \} |  |
| `readerCacheCapacity` _integer_ |  | 268435456 | Minimum: 1 <br /> |
| `cacheWarmer` _[CacheWarmerSpec](#cachewarmerspec)_ |  |  |  |
| `write` _[WriteSpec](#writespec)_ |  |  |  |
| `sharding` _[ShardingSpec](#shardingspec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ |  |  |  |
| `namespaces` _string array_ |  | [default] |  |




#### MetricsSpec



MetricsSpec defines the desired state of Metrics.



_Appears in:_
- [Metrics](#metrics)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[MetricsMode](#metricsmode)_ |  | Standalone | Enum: [Standalone Sharded] <br /> |
| `version` _string_ | Version is the canonical Metrics container image tag. It must be SemVer<br />without a leading "v". When omitted, deprecated image.tag is used, then<br />the operator's default version. |  | Pattern: `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$` <br /> |
| `image` _[ImageSpec](#imagespec)_ |  | \{ pullPolicy:IfNotPresent repository:ghcr.io/pluralsh/metrics \} |  |
| `config` _[MetricsConfigSpec](#metricsconfigspec)_ |  |  |  |
| `writer` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `reader` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `service` _[ServiceSpec](#servicespec)_ |  | \{ grpcPort:9090 httpPort:8080 type:ClusterIP \} |  |
| `ingress` _[IngressSpec](#ingressspec)_ |  |  |  |
| `serviceAccount` _[ServiceAccountSpec](#serviceaccountspec)_ |  |  |  |




#### NamespaceAuthentication



NamespaceAuthentication is the Schema for the namespaceauthentications API.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `NamespaceAuthentication` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[NamespaceAuthenticationSpec](#namespaceauthenticationspec)_ |  |  |  |


#### NamespaceAuthenticationSpec



NamespaceAuthenticationSpec defines the desired state of NamespaceAuthentication.



_Appears in:_
- [NamespaceAuthentication](#namespaceauthentication)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `dataStoreRef` _[DataStoreReference](#datastorereference)_ |  |  |  |
| `namespace` _string_ |  |  | MinLength: 1 <br /> |
| `username` _string_ |  |  | MinLength: 1 <br /> |
| `permission` _string_ |  |  | Enum: [read write] <br /> |
| `secretKeyRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |
| `usageReportingEndpoint` _string_ | UsageReportingEndpoint is a plaintext gRPC endpoint (host:port or<br />http://host:port) implementing Plural Console's PluralServer.MeterMetrics.<br />Ingested bytes for the namespace are buffered and periodically reported<br />to it. When several NamespaceAuthentications for the same datastore and<br />namespace set different endpoints, the lexicographically smallest wins. |  | MaxLength: 253 <br />Pattern: `^(http://)?[A-Za-z0-9.-]+(:[0-9]\{1,5\})?$` <br /> |




#### ObjectStoreSpec







_Appears in:_
- [StorageSpec](#storagespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _[ObjectStoreType](#objectstoretype)_ |  | Local | Enum: [InMemory Local Aws Azure Gcp] <br /> |
| `path` _string_ |  |  |  |
| `aws` _[AWSObjectStoreSpec](#awsobjectstorespec)_ |  |  |  |
| `azure` _[AzureObjectStoreSpec](#azureobjectstorespec)_ |  |  |  |
| `gcp` _[GCPObjectStoreSpec](#gcpobjectstorespec)_ |  |  |  |


#### ObjectStoreType

_Underlying type:_ _string_





_Appears in:_
- [ObjectStoreSpec](#objectstorespec)

| Field | Description |
| --- | --- |
| `InMemory` |  |
| `Local` |  |
| `Aws` |  |
| `Azure` |  |
| `Gcp` |  |


#### PersistentVolumeClaimRetentionPolicySpec







_Appears in:_
- [WorkloadSpec](#workloadspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `whenDeleted` _[PersistentVolumeClaimRetentionPolicyType](#persistentvolumeclaimretentionpolicytype)_ | WhenDeleted controls PVC retention when the StatefulSet is deleted.<br />When omitted, the object-store-dependent workload default is used. |  | Enum: [Retain Delete] <br /> |
| `whenScaled` _[PersistentVolumeClaimRetentionPolicyType](#persistentvolumeclaimretentionpolicytype)_ | WhenScaled controls PVC retention when StatefulSet replicas are reduced.<br />When omitted, the object-store-dependent workload default is used. |  | Enum: [Retain Delete] <br /> |


#### PersistentVolumeClaimRetentionPolicyType

_Underlying type:_ _string_





_Appears in:_
- [PersistentVolumeClaimRetentionPolicySpec](#persistentvolumeclaimretentionpolicyspec)

| Field | Description |
| --- | --- |
| `Retain` |  |
| `Delete` |  |




#### PseudoFS









| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `PseudoFS` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[PseudoFSSpec](#pseudofsspec)_ |  |  |  |


#### PseudoFSConfigSpec







_Appears in:_
- [PseudoFSSpec](#pseudofsspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storage` _[StorageSpec](#storagespec)_ |  | \{ path:pseudofs \} |  |
| `chunkSizeBytes` _integer_ |  | 1048576 | Minimum: 1 <br /> |
| `maxFileSizeBytes` _integer_ |  | 1073741824 | Minimum: 1 <br /> |
| `maxAppendGenerations` _integer_ |  | 64 | Minimum: 2 <br /> |
| `maxUnaryFileSizeBytes` _integer_ |  | 8388608 | Minimum: 1 <br /> |
| `maxDecodingMessageBytes` _integer_ |  | 16777216 | Minimum: 1 <br /> |
| `maxEncodingMessageBytes` _integer_ |  | 16777216 | Minimum: 1 <br /> |


#### PseudoFSServiceSpec







_Appears in:_
- [PseudoFSSpec](#pseudofsspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `grpcPort` _integer_ |  | 9093 | Maximum: 65535 <br />Minimum: 1 <br /> |
| `type` _[ServiceType](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#servicetype-v1-core)_ |  | ClusterIP | Enum: [ClusterIP NodePort LoadBalancer] <br /> |
| `annotations` _object (keys:string, values:string)_ |  |  |  |


#### PseudoFSSpec



PseudoFSSpec defines the desired state of PseudoFS.



_Appears in:_
- [PseudoFS](#pseudofs)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `version` _string_ | Version is the canonical PseudoFS container image tag. |  | Pattern: `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$` <br /> |
| `image` _[ImageSpec](#imagespec)_ |  | \{ pullPolicy:IfNotPresent repository:ghcr.io/pluralsh/pseudofs \} |  |
| `config` _[PseudoFSConfigSpec](#pseudofsconfigspec)_ |  |  |  |
| `workload` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `service` _[PseudoFSServiceSpec](#pseudofsservicespec)_ |  |  |  |
| `serviceAccount` _[ServiceAccountSpec](#serviceaccountspec)_ |  |  |  |




#### RoutingEpoch



RoutingEpoch is the hash routing for records timestamped at or after
EffectiveFromNs, until the next epoch begins. The first epoch starts at the
minimum int64 so every timestamp resolves to exactly one epoch.



_Appears in:_
- [ShardMapSpec](#shardmapspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `effective_from_ns` _integer_ |  |  |  |
| `routing` _[HashRangeMap](#hashrangemap)_ |  |  |  |


#### ServiceAccountSpec







_Appears in:_
- [LogsSpec](#logsspec)
- [MetricsSpec](#metricsspec)
- [PseudoFSSpec](#pseudofsspec)
- [TracesSpec](#tracesspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `annotations` _object (keys:string, values:string)_ |  |  |  |


#### ServiceSpec







_Appears in:_
- [LogsSpec](#logsspec)
- [MetricsSpec](#metricsspec)
- [TracesSpec](#tracesspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `httpPort` _integer_ |  |  |  |
| `grpcPort` _integer_ |  |  |  |
| `type` _[ServiceType](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#servicetype-v1-core)_ |  | ClusterIP | Enum: [ClusterIP NodePort LoadBalancer] <br /> |
| `annotations` _object (keys:string, values:string)_ |  |  |  |


#### ShardAssignment







_Appears in:_
- [ShardMapSpec](#shardmapspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `owner` _[ShardOwner](#shardowner)_ |  |  |  |
| `range` _[ShardRange](#shardrange)_ |  |  |  |
| `state` _string_ |  |  | Enum: [pending active draining released] <br /> |


#### ShardMap



ShardMap is the Schema for authoritative telemetry shard routing.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `ShardMap` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[ShardMapSpec](#shardmapspec)_ |  |  |  |


#### ShardMapSpec



ShardMapSpec is the authoritative routing and writer-ownership snapshot.
The elected Rust shard coordinator writes it with resourceVersion-based
compare-and-swap updates; all writer and reader replicas consume it by watch.



_Appears in:_
- [ShardMap](#shardmap)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `generation` _integer_ |  |  | Minimum: 1 <br /> |
| `shard_count` _integer_ | ShardCount is the number of physical SlateDB storage shards, equal to<br />the latest epoch's shard count. Desired count comes from writer<br />StatefulSet replicas; it only grows because shards are never merged. |  | Minimum: 1 <br /> |
| `epochs` _[RoutingEpoch](#routingepoch) array_ | Epochs are ordered by EffectiveFromNs with non-decreasing shard counts.<br />Scale-up appends an epoch at a future aligned cutover. |  | MinItems: 1 <br /> |
| `assignments` _[ShardAssignment](#shardassignment) array_ |  |  |  |


#### ShardOwner







_Appears in:_
- [ShardAssignment](#shardassignment)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `id` _string_ |  |  |  |
| `ordinal` _integer_ |  |  | Minimum: 0 <br /> |


#### ShardRange







_Appears in:_
- [ShardAssignment](#shardassignment)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `start` _integer_ |  |  | Minimum: 0 <br /> |
| `end` _integer_ |  |  | Minimum: 1 <br /> |


#### ShardingSpec







_Appears in:_
- [LogsConfigSpec](#logsconfigspec)
- [MetricsConfigSpec](#metricsconfigspec)
- [TracesConfigSpec](#tracesconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ioConcurrencyLimit` _integer_ | IOConcurrencyLimit bounds concurrent storage I/O operations per pod. | 128 | Minimum: 1 <br /> |
| `leaseDurationSeconds` _integer_ |  | 15 | Minimum: 1 <br /> |
| `renewIntervalSeconds` _integer_ |  | 5 | Minimum: 1 <br /> |


#### StorageSpec







_Appears in:_
- [LogsConfigSpec](#logsconfigspec)
- [MetricsConfigSpec](#metricsconfigspec)
- [PseudoFSConfigSpec](#pseudofsconfigspec)
- [TracesConfigSpec](#tracesconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `path` _string_ |  |  |  |
| `settingsPath` _string_ |  |  |  |
| `objectStore` _[ObjectStoreSpec](#objectstorespec)_ |  |  |  |
| `blockCache` _[CacheSpec](#cachespec)_ |  |  |  |
| `metaCache` _[CacheSpec](#cachespec)_ |  |  |  |


#### Traces









| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `Traces` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[TracesSpec](#tracesspec)_ |  |  |  |


#### TracesConfigSpec







_Appears in:_
- [TracesSpec](#tracesspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storage` _[StorageSpec](#storagespec)_ |  | \{ path:traces \} |  |
| `segmentDurationSeconds` _integer_ |  | 3600 | Minimum: 1 <br /> |
| `retentionSeconds` _integer_ |  |  | Minimum: 1 <br /> |
| `page` _[TracesPageSpec](#tracespagespec)_ |  |  |  |
| `write` _[WriteSpec](#writespec)_ |  |  |  |
| `sharding` _[ShardingSpec](#shardingspec)_ |  |  |  |
| `request` _[TracesRequestSpec](#tracesrequestspec)_ |  |  |  |
| `cacheWarmer` _[CacheWarmerSpec](#cachewarmerspec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ |  |  |  |
| `namespaces` _string array_ |  | [default] |  |




#### TracesPageSpec







_Appears in:_
- [TracesConfigSpec](#tracesconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `targetSizeBytes` _integer_ |  | 1048576 | Minimum: 1 <br /> |
| `maxSizeBytes` _integer_ |  | 4194304 | Minimum: 1 <br /> |
| `maxTraces` _integer_ |  | 1024 | Minimum: 1 <br /> |


#### TracesRequestSpec







_Appears in:_
- [TracesConfigSpec](#tracesconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `maxRequestBytes` _integer_ |  | 10485760 | Minimum: 1 <br /> |
| `requestConcurrency` _integer_ |  | 64 | Minimum: 1 <br /> |
| `maxCandidates` _integer_ |  | 10000 | Minimum: 1 <br /> |
| `maxSpansPerTrace` _integer_ |  | 100000 | Minimum: 1 <br /> |
| `queryConcurrency` _integer_ |  | 8 | Minimum: 1 <br /> |
| `maxQueryLimit` _integer_ |  | 1000 | Minimum: 1 <br /> |


#### TracesSpec



TracesSpec defines the desired state of Traces.



_Appears in:_
- [Traces](#traces)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[TracesMode](#tracesmode)_ |  | Standalone | Enum: [Standalone Sharded] <br /> |
| `version` _string_ | Version is the canonical Traces container image tag. It must be SemVer<br />without a leading "v". When omitted, deprecated image.tag is used, then<br />the operator's default version. |  | Pattern: `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$` <br /> |
| `image` _[ImageSpec](#imagespec)_ |  | \{ pullPolicy:IfNotPresent repository:ghcr.io/pluralsh/traces \} |  |
| `config` _[TracesConfigSpec](#tracesconfigspec)_ |  |  |  |
| `writer` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `reader` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `service` _[ServiceSpec](#servicespec)_ |  | \{ grpcPort:9092 httpPort:3200 type:ClusterIP \} |  |
| `ingress` _[IngressSpec](#ingressspec)_ |  |  |  |
| `serviceAccount` _[ServiceAccountSpec](#serviceaccountspec)_ |  |  |  |




#### VolumeSpec







_Appears in:_
- [WorkloadSpec](#workloadspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `emptyDir` _[EmptyDirVolumeSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#emptydirvolumesource-v1-core)_ |  |  |  |
| `persistentVolumeClaim` _[PersistentVolumeClaimSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#persistentvolumeclaimspec-v1-core)_ |  |  |  |


#### WorkloadSpec







_Appears in:_
- [LogsSpec](#logsspec)
- [MetricsSpec](#metricsspec)
- [PseudoFSSpec](#pseudofsspec)
- [TracesSpec](#tracesspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `replicas` _integer_ | Replicas defaults to one for sharded writers and two for sharded<br />readers. Standalone mode uses exactly one writer and no reader. |  | Minimum: 0 <br /> |
| `nodeSelector` _object (keys:string, values:string)_ | NodeSelector is merged with podTemplate.spec.nodeSelector. Values here<br />take precedence when the same key is configured in both places. |  |  |
| `tolerations` _[Toleration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#toleration-v1-core) array_ | Tolerations are merged with podTemplate.spec.tolerations. A first-class<br />toleration replaces a podTemplate toleration with the same key, operator,<br />and effect; otherwise it is appended. |  |  |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#resourcerequirements-v1-core)_ | Resources configures requests and limits for the datastore container.<br />Values here take precedence over podTemplate container resources. Missing<br />values default to 250m CPU and 512Mi memory requests and a 2Gi memory limit. |  |  |
| `podTemplate` _[PodTemplateSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#podtemplatespec-v1-core)_ | PodTemplate is the full Kubernetes pod template escape hatch. Operator<br />required fields and the first-class scheduling fields are merged into it. |  |  |
| `dataVolume` _[VolumeSpec](#volumespec)_ |  |  |  |
| `cacheVolume` _[VolumeSpec](#volumespec)_ |  |  |  |
| `persistentVolumeClaimRetentionPolicy` _[PersistentVolumeClaimRetentionPolicySpec](#persistentvolumeclaimretentionpolicyspec)_ | PersistentVolumeClaimRetentionPolicy controls whether StatefulSet PVCs<br />are retained or deleted when the workload is deleted or scaled down.<br />When omitted, remote and in-memory object stores default to Delete while<br />Local object stores default to Retain because the data PVC is authoritative. |  |  |


#### WriteSpec







_Appears in:_
- [LogsConfigSpec](#logsconfigspec)
- [MetricsConfigSpec](#metricsconfigspec)
- [TracesConfigSpec](#tracesconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `durability` _[Durability](#durability)_ |  | applied | Enum: [applied written durable] <br /> |
| `flushIntervalSeconds` _integer_ |  | 10 | Minimum: 0 <br /> |
| `bufferQueueCapacity` _integer_ |  | 10000 | Minimum: 1 <br /> |
| `bufferFlushIntervalMilliseconds` _integer_ |  | 10000 | Minimum: 1 <br /> |
| `bufferSizeThresholdBytes` _integer_ |  | 67108864 | Minimum: 1 <br /> |
| `remoteConcurrency` _integer_ |  | 16 | Minimum: 1 <br /> |
| `remoteRetries` _integer_ |  | 2 | Minimum: 0 <br /> |


#### WriterScalingStatus



WriterScalingStatus reports the operator-observed writer membership and the
authoritative Rust-managed ShardMap state. Nil shard fields mean the
ShardMap was absent or could not be trusted.



_Appears in:_
- [LogsStatus](#logsstatus)
- [MetricsStatus](#metricsstatus)
- [TracesStatus](#tracesstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `effectiveWriterReplicas` _integer_ |  |  |  |
| `readyWriterReplicas` _integer_ |  |  |  |
| `shardCount` _integer_ |  |  |  |
| `shardGeneration` _integer_ |  |  |  |
| `routingEpochs` _integer_ |  |  |  |


