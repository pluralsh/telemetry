# API Reference

## Packages
- [telemetry.plural.sh/v1alpha1](#telemetrypluralshv1alpha1)


## telemetry.plural.sh/v1alpha1

Package v1alpha1 contains API Schema definitions for the telemetry v1alpha1 API group.

### Resource Types
- [BucketCleaner](#bucketcleaner)
- [Logs](#logs)
- [Metrics](#metrics)
- [NamespaceAuthentication](#namespaceauthentication)
- [NamespaceSchema](#namespaceschema)
- [PseudoFS](#pseudofs)
- [ShardMap](#shardmap)
- [Traces](#traces)
- [Vector](#vector)
- [VectorTopology](#vectortopology)



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


#### AttributeSchema



AttributeSchema is the schema of one namespace attribute, as in the
datastore's schema API.



_Appears in:_
- [NamespaceSchemaSpec](#namespaceschemaspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _string_ | Type is fixed once the attribute exists. |  | Enum: [string int float bool []string []int] <br /> |
| `full_text_search` _boolean_ | FullTextSearch indexes a string attribute for BM25 ranking. |  |  |
| `embed` _boolean_ | Embed embeds a string attribute into the vector of rows written<br />without one. At most one attribute per namespace. |  |  |


#### AuthSpec







_Appears in:_
- [LogsConfigSpec](#logsconfigspec)
- [MetricsConfigSpec](#metricsconfigspec)
- [TracesConfigSpec](#tracesconfigspec)
- [VectorConfigSpec](#vectorconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `unauthenticated` _boolean_ | Unauthenticated permits anonymous access to namespace HTTP APIs. | false |  |
| `global` _[AccessSpec](#accessspec)_ |  |  |  |
| `jwt` _[JWTSpec](#jwtspec)_ |  |  |  |
| `internalTokenSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |


#### AutoscalingSpec



AutoscalingSpec scales reader replicas on resource utilization. While
enabled the HorizontalPodAutoscaler owns replica counts and the
workload's replicas field only seeds a new StatefulSet.



_Appears in:_
- [WorkloadSpec](#workloadspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ |  |  |  |
| `minReplicas` _integer_ | MinReplicas defaults to the workload's replicas, or 2. |  | Minimum: 1 <br /> |
| `maxReplicas` _integer_ |  |  | Minimum: 1 <br /> |
| `targetCPUUtilizationPercentage` _integer_ | TargetCPUUtilizationPercentage is average CPU usage relative to the<br />container's CPU request. Defaults to 70 when no memory target is set. |  | Minimum: 1 <br /> |
| `targetMemoryUtilizationPercentage` _integer_ | TargetMemoryUtilizationPercentage is average memory usage relative to<br />the container's memory request. |  | Minimum: 1 <br /> |
| `behavior` _[HorizontalPodAutoscalerBehavior](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#horizontalpodautoscalerbehavior-v2-autoscaling)_ | Behavior is passed through to the HorizontalPodAutoscaler. |  |  |


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


#### BucketCleaner



BucketCleaner verifies the operator can reach an object-store prefix and
deletes every object under it when the BucketCleaner is deleted.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `BucketCleaner` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketCleanerSpec](#bucketcleanerspec)_ |  |  |  |


#### BucketCleanerDataStoreReference







_Appears in:_
- [BucketCleanerSpec](#bucketcleanerspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `kind` _string_ |  |  | Enum: [Metrics Logs Traces Vector] <br /> |
| `name` _string_ |  |  | MinLength: 1 <br /> |


#### BucketCleanerSpec



BucketCleanerSpec defines the desired state of BucketCleaner.



_Appears in:_
- [BucketCleaner](#bucketcleaner)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `objectStore` _[ObjectStoreSpec](#objectstorespec)_ | ObjectStore is the bucket holding the tracked data, in the same form as<br />a datastore's spec.config.storage.objectStore. Credential Secrets are<br />read from the BucketCleaner's namespace by the operator itself; when<br />they are omitted the operator's own cloud identity is used. Defaults to<br />the dataStoreRef's spec.config.storage.objectStore. |  |  |
| `subPath` _string_ | SubPath is the key prefix within the bucket, typically a datastore's<br />spec.config.storage.path. It is treated as a directory: "logs" covers<br />"logs/..." but not "logs-archive/...". Every object under it is deleted<br />when the BucketCleaner is deleted. Defaults to the dataStoreRef's<br />spec.config.storage.path, or the product name when that is unset. |  | MinLength: 1 <br />Pattern: `[^/]` <br /> |
| `checkIntervalSeconds` _integer_ | CheckIntervalSeconds is how often bucket access is re-verified. | 300 | Minimum: 10 <br /> |
| `dataStoreRef` _[BucketCleanerDataStoreReference](#bucketcleanerdatastorereference)_ | DataStoreRef is the datastore, in the BucketCleaner's namespace, that<br />writes under subPath. Omitted objectStore and subPath settings are<br />taken from its storage while it exists. Deletion waits until the<br />datastore and its pods are gone, so no replica writes objects after the<br />prefix is emptied, and data is never deleted while the datastore exists. |  |  |




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
| `enabled` _boolean_ | Enabled runs a startup pass over recent SSTs. Defaults to true for<br />readers, which warm filters and indexes in the background; Logs<br />readers that skip filters warm only indexes. |  |  |
| `warmRangeSeconds` _integer_ | WarmRangeSeconds is the recent window whose metadata the startup pass<br />warms. Unset covers all live data, which retention bounds, and the<br />metadata budget bounds what is held. A set value bounds payloads too<br />unless PayloadWarmRangeSeconds is set. 7200, the default of earlier<br />CRDs that the API server stored in existing resources, is read as<br />unset. |  | Minimum: 1 <br /> |
| `payloadWarmRangeSeconds` _integer_ | PayloadWarmRangeSeconds is the recent window whose payloads the<br />startup pass warms when IncludePayloads is set. Unset uses<br />WarmRangeSeconds when that is set, otherwise all live data, which<br />PayloadBudgetPercent bounds newest first. Servers older than this<br />field reject it, so set it only once every reader runs a server that<br />knows it. |  | Minimum: 1 <br /> |
| `timeoutSeconds` _integer_ |  | 30 | Minimum: 1 <br /> |
| `concurrency` _integer_ |  | 2 | Minimum: 1 <br /> |
| `includePayloads` _boolean_ |  | false |  |
| `blockReadiness` _boolean_ | BlockReadiness holds readiness until the startup pass ends. Unset<br />keeps the server default, false. |  |  |
| `metadataBudgetPercent` _integer_ | MetadataBudgetPercent is the share of the metadata cache warming may<br />fill before it stops; the server default is 75. |  | Maximum: 100 <br />Minimum: 0 <br /> |
| `payloadBudgetPercent` _integer_ | PayloadBudgetPercent is the share of the data block cache that<br />payload warming may fill, newest first: of its disk tier when it has<br />one, otherwise of its memory. Warmed payloads go to the disk tier<br />only, so warming never fills memory. The server default is 50.<br />Servers older than this field reject it, so set it only once every<br />reader runs a server that knows it. |  | Maximum: 100 <br />Minimum: 0 <br /> |
| `continuous` _[ContinuousCacheWarmerSpec](#continuouscachewarmerspec)_ | Continuous warms SSTs that appear in reader manifests after startup. |  |  |


#### ContinuousCacheWarmerSpec







_Appears in:_
- [CacheWarmerSpec](#cachewarmerspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ |  | true |  |
| `intervalSeconds` _integer_ |  | 10 | Minimum: 1 <br /> |
| `warmRangeSeconds` _integer_ | WarmRangeSeconds is CacheWarmerSpec.WarmRangeSeconds for new SSTs. |  | Minimum: 1 <br /> |
| `payloadWarmRangeSeconds` _integer_ | PayloadWarmRangeSeconds is CacheWarmerSpec.PayloadWarmRangeSeconds<br />for new SSTs. |  | Minimum: 1 <br /> |
| `includePayloads` _boolean_ |  | true |  |


#### DataStoreReference







_Appears in:_
- [NamespaceAuthenticationSpec](#namespaceauthenticationspec)
- [NamespaceSchemaSpec](#namespaceschemaspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `kind` _string_ |  |  | Enum: [Metrics Logs Traces Vector] <br /> |
| `name` _string_ |  |  | MinLength: 1 <br /> |


#### DiskSpec



DiskSpec is a disk's provisioned performance, as its provider advertises it.



_Appears in:_
- [StorageSpec](#storagespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `throughputMiBps` _integer_ |  | 125 | Minimum: 1 <br /> |
| `iops` _integer_ |  | 3000 | Minimum: 1 <br /> |


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
- [VectorSpec](#vectorspec)

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
- [VectorSpec](#vectorspec)

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
| `retention` _string_ | Retention is how long data is kept, counted from ingestion, e.g. 14d or 2w.<br />When unset, the server default of 14 days applies. |  | Pattern: `^([0-9]+[wdhms])*0*[1-9][0-9]*[wdhms]([0-9]+[wdhms])*$` <br /> |
| `retentionSeconds` _integer_ | RetentionSeconds is deprecated in favor of retention. |  | Minimum: 1 <br /> |
| `page` _[LogsPageSpec](#logspagespec)_ |  |  |  |
| `write` _[WriteSpec](#writespec)_ |  |  |  |
| `sharding` _[ShardingSpec](#shardingspec)_ |  |  |  |
| `request` _[LogsRequestSpec](#logsrequestspec)_ |  |  |  |
| `elasticsearch` _[LogsElasticsearchSpec](#logselasticsearchspec)_ |  | \{  \} |  |
| `cacheWarmer` _[CacheWarmerSpec](#cachewarmerspec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ |  |  |  |
| `namespaces` _string array_ |  | [default] |  |


#### LogsElasticsearchSpec



LogsElasticsearchSpec maps Elasticsearch _bulk documents to log entries.



_Appears in:_
- [LogsConfigSpec](#logsconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `messageFields` _string array_ | MessageFields are tried in order; the first present becomes the log line. | [message log msg] | items:MinLength: 1 <br /> |
| `timeField` _string_ | TimeField holds the entry timestamp as RFC3339 or epoch milliseconds. | @timestamp | MinLength: 1 <br /> |
| `streamFields` _string array_ | StreamFields are document fields promoted to stream labels. Keep them<br />low-cardinality; all other fields become structured metadata. |  | items:MinLength: 1 <br /> |




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
| `maxRequestBytes` _integer_ | MaxRequestBytes caps a request body as received, before content decoding. | 33554432 | Minimum: 1 <br /> |
| `maxDecodedRequestBytes` _integer_ | MaxDecodedRequestBytes caps a write body after gzip or snappy decoding.<br />Must be at least maxRequestBytes. | 134217728 | Minimum: 1 <br /> |
| `maxQueryEntries` _integer_ |  | 5000 | Minimum: 1 <br /> |
| `maxQueryPages` _integer_ |  | 10000 | Minimum: 1 <br /> |
| `maxStructuredMetadataFields` _integer_ |  | 128 | Minimum: 1 <br /> |
| `queryConcurrency` _integer_ |  | 16 | Minimum: 1 <br /> |
| `maxInFlightQueryBytes` _integer_ | MaxInFlightQueryBytes bounds the decoded bytes all running queries hold<br />at once. When omitted, the operator sizes it from the memory limit. |  | Minimum: 1 <br /> |


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
| `retention` _string_ | Retention is how long samples are kept, counted from their timestamp, e.g. 14d or 2w.<br />When unset, the server default of 60 days applies. |  | Pattern: `^([0-9]+[wdhms])*0*[1-9][0-9]*[wdhms]([0-9]+[wdhms])*$` <br /> |
| `readerCacheCapacity` _integer_ | ReaderCacheCapacity sizes each shard's query cache: half of it, in<br />bytes, holds decoded series chunks. When omitted, the operator sizes it<br />from the container memory limit. |  | Minimum: 1 <br /> |
| `forwardIndexCacheCapacityBytes` _integer_ | ForwardIndexCacheCapacityBytes bounds forward-index entries and resolved<br />selectors. When omitted, the operator sizes it from the container<br />memory limit. |  | Minimum: 1 <br /> |
| `query` _[MetricsQuerySpec](#metricsqueryspec)_ | Query limits PromQL evaluation across the whole process. |  |  |
| `cacheWarmer` _[CacheWarmerSpec](#cachewarmerspec)_ |  |  |  |
| `write` _[WriteSpec](#writespec)_ |  |  |  |
| `request` _[MetricsRequestSpec](#metricsrequestspec)_ |  |  |  |
| `sharding` _[ShardingSpec](#shardingspec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ |  |  |  |
| `namespaces` _string array_ |  | [default] |  |




#### MetricsQuerySpec



MetricsQuerySpec limits the queries one Metrics process evaluates at once.



_Appears in:_
- [MetricsConfigSpec](#metricsconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `maxConcurrent` _integer_ | MaxConcurrent is how many queries evaluate at once; the rest wait.<br />Defaults to 32. |  | Minimum: 1 <br /> |
| `queueTimeoutSeconds` _integer_ | QueueTimeoutSeconds is how long a query waits to start before failing<br />with 503. Defaults to 30. |  | Minimum: 1 <br /> |
| `maxMemoryBytes` _integer_ | MaxMemoryBytes is the engine memory one query may reserve. Defaults to<br />half the memory budget, capped at 1 GiB, so one query cannot take all<br />of it. |  | Minimum: 1 <br /> |
| `memoryBudgetBytes` _integer_ | MemoryBudgetBytes is the engine memory all running queries together may<br />reserve. When omitted, the operator sizes it from the container memory<br />limit. |  | Minimum: 1 <br /> |
| `maxConcurrentShardReads` _integer_ | MaxConcurrentShardReads is how many shard reads a reader serves to other<br />reader groups at once. Defaults to 16. |  | Minimum: 1 <br /> |


#### MetricsRequestSpec







_Appears in:_
- [MetricsConfigSpec](#metricsconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `maxRequestBytes` _integer_ | MaxRequestBytes caps a request body as received, before content decoding. | 33554432 | Minimum: 1 <br /> |
| `maxDecodedRequestBytes` _integer_ | MaxDecodedRequestBytes caps a write body after gzip or snappy decoding.<br />Must be at least maxRequestBytes. | 134217728 | Minimum: 1 <br /> |


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
| `usageReportingEndpoint` _string_ | UsageReportingEndpoint is a plaintext gRPC endpoint (host:port or<br />http://host:port) implementing Plural Console's PluralServer.MeterMetrics.<br />Ingested bytes for the namespace are buffered and periodically reported<br />to it. When several NamespaceAuthentications for the same datastore and<br />namespace set different endpoints, the lexicographically smallest wins.<br />Vector doesn't report usage. |  | MaxLength: 253 <br />Pattern: `^(http://)?[A-Za-z0-9.-]+(:[0-9]\{1,5\})?$` <br /> |




#### NamespaceSchema



NamespaceSchema declares the attribute schema of a datastore namespace.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `NamespaceSchema` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[NamespaceSchemaSpec](#namespaceschemaspec)_ |  |  |  |


#### NamespaceSchemaSpec



NamespaceSchemaSpec defines the desired state of NamespaceSchema.



_Appears in:_
- [NamespaceSchema](#namespaceschema)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `dataStoreRef` _[DataStoreReference](#datastorereference)_ |  |  |  |
| `namespace` _string_ | Namespace is the datastore namespace the schema is for. Defaults to<br />the resource's name. |  | MinLength: 1 <br /> |
| `schema` _object (keys:string, values:[AttributeSchema](#attributeschema))_ | Schema maps attribute names to their schemas. Servers merge it into<br />the namespace whenever it changes and when the namespace is created:<br />attributes it names are replaced whole, others are left alone, and an<br />attribute's type can't change once set. Attributes removed from it<br />stay in the namespace. |  | MinProperties: 1 <br /> |




#### ObjectStoreSpec







_Appears in:_
- [BucketCleanerSpec](#bucketcleanerspec)
- [BucketCleanerStatus](#bucketcleanerstatus)
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


#### ProductMode

_Underlying type:_ _string_





_Appears in:_
- [VectorSpec](#vectorspec)

| Field | Description |
| --- | --- |
| `Standalone` |  |
| `Sharded` |  |


#### PseudoFS









| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `PseudoFS` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[PseudoFSSpec](#pseudofsspec)_ |  |  |  |


#### PseudoFSBlobsSpec



PseudoFSBlobsSpec configures file content stored outside the metastore.
Blobs are kept on the data volume until uploaded and then cached there, and
are uploaded to the storage object store with the same credentials, under
"<storage.path>/blobs".



_Appears in:_
- [PseudoFSConfigSpec](#pseudofsconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `cacheCapacityBytes` _integer_ | CacheCapacityBytes bounds blob usage of the data volume, including<br />blobs awaiting upload. Keep it well below the volume size. | 4294967296 | Minimum: 1 <br /> |
| `uploadConcurrency` _integer_ |  | 4 | Minimum: 1 <br /> |
| `gcGraceSeconds` _integer_ | GCGraceSeconds is how long unreferenced blobs are kept so in-flight<br />reads of replaced files can finish. | 300 | Minimum: 0 <br /> |


#### PseudoFSConfigSpec







_Appears in:_
- [PseudoFSSpec](#pseudofsspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storage` _[StorageSpec](#storagespec)_ | Storage.Path is the filesystem's root prefix in the object store: the<br />metastore is kept under "<path>/metastore" and blobs under<br />"<path>/blobs". | \{ path:pseudofs \} |  |
| `chunkSizeBytes` _integer_ |  | 1048576 | Minimum: 1 <br /> |
| `maxFileSizeBytes` _integer_ |  | 1073741824 | Minimum: 1 <br /> |
| `maxAppendGenerations` _integer_ |  | 64 | Minimum: 2 <br /> |
| `maxUnaryFileSizeBytes` _integer_ |  | 8388608 | Minimum: 1 <br /> |
| `maxDecodingMessageBytes` _integer_ |  | 16777216 | Minimum: 1 <br /> |
| `maxEncodingMessageBytes` _integer_ |  | 16777216 | Minimum: 1 <br /> |
| `inlineThresholdBytes` _integer_ | InlineThresholdBytes is the largest write stored in the metastore<br />rather than as a blob. Inline bytes pass through the metastore's WAL and<br />compactions, so raising this trades metastore write load for fewer blobs. | 16384 | Maximum: 1.6777216e+07 <br />Minimum: 0 <br /> |
| `blobs` _[PseudoFSBlobsSpec](#pseudofsblobsspec)_ |  | \{  \} |  |
| `search` _[PseudoFSSearchSpec](#pseudofssearchspec)_ |  | \{  \} |  |


#### PseudoFSSearchSpec



PseudoFSSearchSpec bounds Glob and Grep requests. Requests that leave a
limit unset use its default; larger requested values are capped.



_Appears in:_
- [PseudoFSConfigSpec](#pseudofsconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `defaultGlobResults` _integer_ |  | 1000 | Minimum: 1 <br /> |
| `maxGlobResults` _integer_ |  | 10000 | Minimum: 1 <br /> |
| `defaultGrepMatches` _integer_ |  | 100 | Minimum: 1 <br /> |
| `maxGrepMatches` _integer_ |  | 1000 | Minimum: 1 <br /> |
| `maxGrepContextLines` _integer_ |  | 100 | Minimum: 0 <br /> |
| `defaultGrepFileSizeBytes` _integer_ |  | 16777216 | Minimum: 1 <br /> |
| `maxGrepFileSizeBytes` _integer_ |  | 67108864 | Minimum: 1 <br /> |
| `grepConcurrency` _integer_ | GrepConcurrency is how many files one Grep request searches at once. | 8 | Minimum: 1 <br /> |
| `maxScannedEntries` _integer_ | MaxScannedEntries is how many directory entries one Glob or Grep walk<br />may read before failing with RESOURCE_EXHAUSTED. | 1000000 | Minimum: 1 <br /> |
| `maxGrepSeconds` _integer_ | MaxGrepSeconds is how long one Grep request may search before failing<br />with RESOURCE_EXHAUSTED after the matches already sent. | 30 | Minimum: 1 <br /> |


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




#### ReadGroupsSpec



ReadGroupsSpec splits sharded readers into one group per storage shard.
Each group is a StatefulSet that opens only its own shard; a query sent
to any reader reads its own shard in process and every other shard from
that shard's group over gRPC. Replicas, resources, and autoscaling apply
to each group.



_Appears in:_
- [WorkloadSpec](#workloadspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ | Enabled defaults to true. When false, every reader opens every shard. | true |  |
| `hedgeAfterMilliseconds` _integer_ | HedgeAfterMilliseconds starts a second copy of a unary shard read on<br />another replica of its group when the first has not answered in time.<br />Streaming reads are never hedged. Zero disables hedging. | 250 | Minimum: 0 <br /> |
| `requestTimeoutSeconds` _integer_ | RequestTimeoutSeconds bounds one shard read. | 120 | Minimum: 1 <br /> |


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
- [VectorSpec](#vectorspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `annotations` _object (keys:string, values:string)_ |  |  |  |


#### ServiceSpec







_Appears in:_
- [LogsSpec](#logsspec)
- [MetricsSpec](#metricsspec)
- [TracesSpec](#tracesspec)
- [VectorSpec](#vectorspec)

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
| `ioConcurrencyLimit` _integer_ | IOConcurrencyLimit bounds concurrent storage I/O operations per pod. When<br />omitted, the operator infers it from the component's memory request at 96<br />permits per GiB, with a minimum of 128. |  | Minimum: 1 <br /> |
| `leaseDurationSeconds` _integer_ |  | 15 | Minimum: 1 <br /> |
| `renewIntervalSeconds` _integer_ |  | 5 | Minimum: 1 <br /> |


#### StorageSpec







_Appears in:_
- [LogsConfigSpec](#logsconfigspec)
- [MetricsConfigSpec](#metricsconfigspec)
- [PseudoFSConfigSpec](#pseudofsconfigspec)
- [TracesConfigSpec](#tracesconfigspec)
- [VectorConfigSpec](#vectorconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `path` _string_ |  |  |  |
| `settingsPath` _string_ |  |  |  |
| `objectStore` _[ObjectStoreSpec](#objectstorespec)_ |  |  |  |
| `blockCache` _[CacheSpec](#cachespec)_ |  |  |  |
| `metaCache` _[CacheSpec](#cachespec)_ |  |  |  |
| `disk` _[DiskSpec](#diskspec)_ | Disk describes the performance specs of the disks used for block and meta caches. This is used to<br />optimize caches and defaults to common block store specs as a reasonable baseline. |  |  |


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
| `retention` _string_ | Retention is how long data is kept, counted from ingestion, e.g. 14d or 2w.<br />When unset, the server default of 14 days applies. |  | Pattern: `^([0-9]+[wdhms])*0*[1-9][0-9]*[wdhms]([0-9]+[wdhms])*$` <br /> |
| `retentionSeconds` _integer_ | RetentionSeconds is deprecated in favor of retention. |  | Minimum: 1 <br /> |
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
| `maxRequestBytes` _integer_ | MaxRequestBytes caps a request body as received, before content decoding. | 33554432 | Minimum: 1 <br /> |
| `maxDecodedRequestBytes` _integer_ | MaxDecodedRequestBytes caps a write body after gzip decoding and a<br />decoded OTLP or Jaeger gRPC message. Must be at least maxRequestBytes. | 134217728 | Minimum: 1 <br /> |
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




#### Vector









| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `Vector` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[VectorSpec](#vectorspec)_ |  |  |  |


#### VectorConfigSpec







_Appears in:_
- [VectorSpec](#vectorspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storage` _[StorageSpec](#storagespec)_ |  | \{ path:vector \} |  |
| `flushIntervalSeconds` _integer_ | FlushIntervalSeconds is how often namespaces flush buffered writes. | 1 | Minimum: 1 <br /> |
| `readerRefreshIntervalSeconds` _integer_ | ReaderRefreshIntervalSeconds is how often readers pick up their<br />writer's changes, which bounds the staleness of eventual reads. | 1 | Minimum: 1 <br /> |
| `leaseDurationSeconds` _integer_ | LeaseDurationSeconds is the lifetime of shard and coordinator leases:<br />how long a shard stays unserved after its writer dies. | 15 | Minimum: 3 <br /> |
| `index` _[VectorIndexSpec](#vectorindexspec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ | Auth configures credentials, as for the other datastores. Unless<br />unauthenticated is set, only declared namespaces exist: those of<br />namespaces and of NamespaceAuthentications referencing this Vector.<br />Namespace read access covers queries, rows and metadata; write access<br />also covers creating, configuring, deleting and writing to the<br />namespace. Shard listing needs global credentials.<br />Credential changes apply without restarting pods. |  |  |
| `namespaces` _string array_ | Namespaces are declared without namespace credentials, for global<br />credentials or JWTs to use. |  |  |
| `embedding` _[VectorEmbeddingSpec](#vectorembeddingspec)_ | Embedding configures the model that embeds namespaces' embed<br />attributes and query texts. Changing it restarts the pods; namespaces<br />keep the model they first embedded with. |  |  |


#### VectorEmbeddingSpec



VectorEmbeddingSpec is an embedding model served by a provider API.



_Appears in:_
- [VectorConfigSpec](#vectorconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `provider` _string_ | Provider is the API: openai (or an OpenAI-compatible endpoint, such<br />as Azure OpenAI, vLLM or Ollama), cohere, gemini, or bedrock (Amazon<br />Bedrock's Titan Text Embeddings v2). |  | Enum: [openai cohere gemini bedrock] <br /> |
| `model` _string_ | Model is the provider's model name, e.g. text-embedding-3-small or<br />amazon.titan-embed-text-v2:0. |  | MinLength: 1 <br /> |
| `apiKeySecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ | APIKeySecretRef holds the provider API key. Rotating the Secret<br />applies without restarting pods. Bedrock instead uses the pods' AWS<br />credentials: an IAM role through the service account, or a Bedrock<br />API key in AWS_BEARER_TOKEN_BEDROCK. |  |  |
| `endpoint` _string_ | Endpoint replaces the provider's API root, e.g.<br />http://ollama.ai.svc:11434/v1. |  |  |
| `region` _string_ | Region is the AWS region of the Bedrock runtime. Without it, the<br />region of the pods' AWS config (AWS_REGION). |  | MinLength: 1 <br /> |
| `dimensions` _integer_ | Dimensions asks models that can shorten their vectors for this many<br />dimensions: OpenAI's text-embedding-3 models, Gemini, and Titan v2<br />(256, 512 or 1024). |  | Maximum: 65535 <br />Minimum: 1 <br /> |
| `batchSize` _integer_ | BatchSize is the most texts sent in one provider request. The server<br />default is 96. |  | Minimum: 1 <br /> |
| `timeoutSeconds` _integer_ | TimeoutSeconds bounds each provider request. The server default is<br />30. |  | Minimum: 1 <br /> |
| `cacheSize` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#quantity-resource-api)_ | CacheSize is the memory each replica spends caching embeddings, so<br />repeated texts and queries skip the provider. 0 disables the cache.<br />Without it, 4% of what each component's memory limit leaves after<br />headroom and reservations, and at least 16Mi. |  |  |


#### VectorIndexSpec







_Appears in:_
- [VectorConfigSpec](#vectorconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `splitThresholdVectors` _integer_ | SplitThresholdVectors is the posting-list size at which a centroid<br />splits. The server default is 150. |  | Minimum: 2 <br /> |
| `mergeThresholdVectors` _integer_ | MergeThresholdVectors is the posting-list size below which a centroid<br />merges into its neighbours. The server default is 50. |  | Minimum: 1 <br /> |


#### VectorNamespaceRange



VectorNamespaceRange is the namespace ids [start, end) a shard holds, as
16 hex digits. An absent end extends to the end of the id space.



_Appears in:_
- [VectorShard](#vectorshard)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `start` _string_ |  |  | Pattern: `^[0-9a-f]\{16\}$` <br /> |
| `end` _string_ |  |  | Pattern: `^[0-9a-f]\{16\}$` <br /> |


#### VectorShard







_Appears in:_
- [VectorTopologySpec](#vectortopologyspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `id` _integer_ |  |  | Minimum: 0 <br /> |
| `state` _string_ |  |  | Enum: [active retired] <br /> |
| `range` _[VectorNamespaceRange](#vectornamespacerange)_ |  |  |  |
| `owner` _string_ | Owner is the writer pod that owns the shard. Each writer owns one<br />active shard. |  |  |
| `split_to` _string_ | SplitTo is a writer pod without a shard that the owner is splitting<br />this shard for: the writer takes the upper half of the range. |  |  |
| `parent` _integer_ | Parent is the shard this one was split from. |  |  |
| `children` _integer array_ | Children are the shards this one was split into. |  |  |
| `created_at_ms` _integer_ |  |  |  |


#### VectorSpec



VectorSpec defines the desired state of Vector.

In Sharded mode each writer replica owns one shard, held with a Lease, and
reader groups serve eventually consistent queries, one group per active
shard by default. Adding writers splits shards for them: the elected
writer picks the widest shard for each new writer once its pod is ready,
and the shard's owner splits it with SlateDB clones, handing half its
namespaces to the new writer without copying data. Writers can't be
removed, since shards don't merge; the operator keeps the writer
StatefulSet at one replica per shard. Any replica accepts any request and
forwards it to the replica that can serve it.



_Appears in:_
- [Vector](#vector)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[ProductMode](#productmode)_ |  | Standalone | Enum: [Standalone Sharded] <br /> |
| `version` _string_ | Version is the Vector container image tag. |  | Pattern: `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$` <br /> |
| `image` _[ImageSpec](#imagespec)_ |  | \{ pullPolicy:IfNotPresent repository:ghcr.io/pluralsh/vector \} |  |
| `config` _[VectorConfigSpec](#vectorconfigspec)_ |  |  |  |
| `writer` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `reader` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `service` _[ServiceSpec](#servicespec)_ |  | \{ httpPort:8080 type:ClusterIP \} |  |
| `ingress` _[IngressSpec](#ingressspec)_ | Ingress exposes the API, which any replica serves, at<br />\{pathPrefix\}/api on the hostname. With a pathPrefix the servers serve<br />the API under it too, so the Ingress needs no rewrite and the<br />hostname can be shared with other services. Changing pathPrefix<br />restarts the pods. |  |  |
| `serviceAccount` _[ServiceAccountSpec](#serviceaccountspec)_ |  |  |  |




#### VectorTopology



VectorTopology is the Schema for a Vector cluster's shard topology.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `VectorTopology` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[VectorTopologySpec](#vectortopologyspec)_ |  |  |  |


#### VectorTopologySpec



VectorTopologySpec is the authoritative shard layout of a Vector cluster.
The elected writer and the writer splitting a shard change it with
resourceVersion-based compare-and-swap; every replica follows it by watch.



_Appears in:_
- [VectorTopology](#vectortopology)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `generation` _integer_ |  |  | Minimum: 1 <br /> |
| `next_shard_id` _integer_ |  |  | Minimum: 1 <br /> |
| `shards` _[VectorShard](#vectorshard) array_ |  |  |  |


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
- [VectorSpec](#vectorspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `replicas` _integer_ | Replicas defaults to one for sharded writers and two for sharded<br />readers. Standalone mode uses exactly one writer and no reader. |  | Minimum: 0 <br /> |
| `nodeSelector` _object (keys:string, values:string)_ | NodeSelector is merged with podTemplate.spec.nodeSelector. Values here<br />take precedence when the same key is configured in both places. |  |  |
| `tolerations` _[Toleration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#toleration-v1-core) array_ | Tolerations are merged with podTemplate.spec.tolerations. A first-class<br />toleration replaces a podTemplate toleration with the same key, operator,<br />and effect; otherwise it is appended. |  |  |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#resourcerequirements-v1-core)_ | Resources configures requests and limits for the datastore container.<br />Values here take precedence over podTemplate container resources. Missing<br />values default to a 250m CPU request and a 2Gi memory limit (8Gi for<br />Metrics, Logs and Traces writers and standalone servers, which roll up<br />closed days), with memory requested at its limit unless a request is<br />set. Those writers need at least a 6Gi limit and are not rolled out with<br />less. The operator holds back a quarter of the memory limit (at least<br />768Mi, at most half) as headroom and sizes the caches and query memory<br />budgets from the rest. |  |  |
| `podTemplate` _[PodTemplateSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#podtemplatespec-v1-core)_ | PodTemplate is the full Kubernetes pod template escape hatch. Operator<br />required fields and the first-class scheduling fields are merged into it. |  |  |
| `dataVolume` _[VolumeSpec](#volumespec)_ |  |  |  |
| `cacheVolume` _[VolumeSpec](#volumespec)_ |  |  |  |
| `persistentVolumeClaimRetentionPolicy` _[PersistentVolumeClaimRetentionPolicySpec](#persistentvolumeclaimretentionpolicyspec)_ | PersistentVolumeClaimRetentionPolicy controls whether StatefulSet PVCs<br />are retained or deleted when the workload is deleted or scaled down.<br />When omitted, remote and in-memory object stores default to Delete while<br />Local object stores default to Retain because the data PVC is authoritative. |  |  |
| `readGroups` _[ReadGroupsSpec](#readgroupsspec)_ | ReadGroups configures per-shard reader groups. Readers only; ignored<br />for writers and in standalone mode. |  |  |
| `autoscaling` _[AutoscalingSpec](#autoscalingspec)_ | Autoscaling adds a HorizontalPodAutoscaler per reader StatefulSet, one<br />per reader group. Readers only; ignored for writers and in standalone<br />mode. |  |  |


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
| `bufferFlushIntervalMilliseconds` _integer_ |  | 30000 | Minimum: 1 <br /> |
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


