# API Reference

## Packages
- [telemetry.plural.sh/v1alpha1](#telemetrypluralshv1alpha1)


## telemetry.plural.sh/v1alpha1

Package v1alpha1 contains API Schema definitions for the telemetry v1alpha1 API group.

### Resource Types
- [Line](#line)
- [Meter](#meter)
- [NamespaceAuthentication](#namespaceauthentication)
- [Track](#track)



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
- [LineConfigSpec](#lineconfigspec)
- [MeterConfigSpec](#meterconfigspec)
- [TrackConfigSpec](#trackconfigspec)

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


#### DataStoreReference







_Appears in:_
- [NamespaceAuthenticationSpec](#namespaceauthenticationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `kind` _string_ |  |  | Enum: [Meter Line] <br /> |
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


#### ImageSpec







_Appears in:_
- [LineSpec](#linespec)
- [MeterSpec](#meterspec)
- [TrackSpec](#trackspec)

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
- [LineSpec](#linespec)
- [MeterSpec](#meterspec)
- [TrackSpec](#trackspec)

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


#### Line









| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `Line` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[LineSpec](#linespec)_ |  |  |  |


#### LineCacheSpec







_Appears in:_
- [LineConfigSpec](#lineconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `queryEntries` _integer_ |  | 256 | Minimum: 0 <br /> |


#### LineConfigSpec







_Appears in:_
- [LineSpec](#linespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storage` _[StorageSpec](#storagespec)_ |  | \{ path:line \} |  |
| `segmentDurationSeconds` _integer_ |  | 3600 | Minimum: 1 <br /> |
| `retentionSeconds` _integer_ |  |  | Minimum: 1 <br /> |
| `page` _[LinePageSpec](#linepagespec)_ |  |  |  |
| `visibilityIntervalSeconds` _integer_ |  | 1 | Minimum: 1 <br /> |
| `write` _[WriteSpec](#writespec)_ |  |  |  |
| `sharding` _[ShardingSpec](#shardingspec)_ |  |  |  |
| `request` _[LineRequestSpec](#linerequestspec)_ |  |  |  |
| `cache` _[LineCacheSpec](#linecachespec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ |  |  |  |
| `namespaces` _string array_ |  | [default] |  |




#### LinePageSpec







_Appears in:_
- [LineConfigSpec](#lineconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `targetSizeBytes` _integer_ |  | 1048576 | Minimum: 1 <br /> |
| `maxRows` _integer_ |  | 8192 | Minimum: 1 <br /> |
| `maxAgeSeconds` _integer_ |  | 5 | Minimum: 1 <br /> |
| `rowsPerBlock` _integer_ |  | 256 | Minimum: 1 <br /> |


#### LineRequestSpec







_Appears in:_
- [LineConfigSpec](#lineconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `maxRequestBytes` _integer_ |  | 10485760 | Minimum: 1 <br /> |
| `maxQueryEntries` _integer_ |  | 5000 | Minimum: 1 <br /> |
| `maxQueryPages` _integer_ |  | 10000 | Minimum: 1 <br /> |
| `maxStructuredMetadataFields` _integer_ |  | 128 | Minimum: 1 <br /> |
| `queryConcurrency` _integer_ |  | 16 | Minimum: 1 <br /> |
| `maxInFlightQueryBytes` _integer_ |  | 134217728 | Minimum: 1 <br /> |


#### LineSpec



LineSpec defines the desired state of Line.



_Appears in:_
- [Line](#line)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[LineMode](#linemode)_ |  | Standalone | Enum: [Standalone Sharded] <br /> |
| `version` _string_ | Version is the canonical Line container image tag. It must be SemVer<br />without a leading "v". When omitted, deprecated image.tag is used, then<br />the operator's default version. |  | Pattern: `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$` <br /> |
| `image` _[ImageSpec](#imagespec)_ |  | \{ pullPolicy:IfNotPresent repository:ghcr.io/pluralsh/line \} |  |
| `config` _[LineConfigSpec](#lineconfigspec)_ |  |  |  |
| `writer` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `reader` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `service` _[ServiceSpec](#servicespec)_ |  | \{ grpcPort:9091 httpPort:3100 type:ClusterIP \} |  |
| `ingress` _[IngressSpec](#ingressspec)_ |  |  |  |
| `serviceAccount` _[ServiceAccountSpec](#serviceaccountspec)_ |  |  |  |




#### Meter



Meter is the Schema for the meters API.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `Meter` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[MeterSpec](#meterspec)_ |  |  |  |


#### MeterConfigSpec







_Appears in:_
- [MeterSpec](#meterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storage` _[StorageSpec](#storagespec)_ |  | \{ path:meter \} |  |
| `readerCacheCapacity` _integer_ |  | 268435456 | Minimum: 1 <br /> |
| `write` _[WriteSpec](#writespec)_ |  |  |  |
| `sharding` _[ShardingSpec](#shardingspec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ |  |  |  |
| `namespaces` _string array_ |  | [default] |  |




#### MeterSpec



MeterSpec defines the desired state of Meter.



_Appears in:_
- [Meter](#meter)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[MeterMode](#metermode)_ |  | Standalone | Enum: [Standalone Sharded] <br /> |
| `version` _string_ | Version is the canonical Meter container image tag. It must be SemVer<br />without a leading "v". When omitted, deprecated image.tag is used, then<br />the operator's default version. |  | Pattern: `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$` <br /> |
| `image` _[ImageSpec](#imagespec)_ |  | \{ pullPolicy:IfNotPresent repository:ghcr.io/pluralsh/meter \} |  |
| `config` _[MeterConfigSpec](#meterconfigspec)_ |  |  |  |
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




#### ServiceAccountSpec







_Appears in:_
- [LineSpec](#linespec)
- [MeterSpec](#meterspec)
- [TrackSpec](#trackspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `annotations` _object (keys:string, values:string)_ |  |  |  |


#### ServiceSpec







_Appears in:_
- [LineSpec](#linespec)
- [MeterSpec](#meterspec)
- [TrackSpec](#trackspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `httpPort` _integer_ |  |  |  |
| `grpcPort` _integer_ |  |  |  |
| `type` _[ServiceType](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#servicetype-v1-core)_ |  | ClusterIP | Enum: [ClusterIP NodePort LoadBalancer] <br /> |
| `annotations` _object (keys:string, values:string)_ |  |  |  |


#### ShardingSpec







_Appears in:_
- [LineConfigSpec](#lineconfigspec)
- [MeterConfigSpec](#meterconfigspec)
- [TrackConfigSpec](#trackconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `virtualShards` _integer_ |  | 8 | Minimum: 1 <br /> |
| `ioConcurrencyMultiplier` _integer_ |  | 8 | Minimum: 1 <br /> |
| `leaseDurationSeconds` _integer_ |  | 15 | Minimum: 1 <br /> |
| `renewIntervalSeconds` _integer_ |  | 5 | Minimum: 1 <br /> |


#### StorageSpec







_Appears in:_
- [LineConfigSpec](#lineconfigspec)
- [MeterConfigSpec](#meterconfigspec)
- [TrackConfigSpec](#trackconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `path` _string_ |  |  |  |
| `settingsPath` _string_ |  |  |  |
| `objectStore` _[ObjectStoreSpec](#objectstorespec)_ |  |  |  |
| `blockCache` _[CacheSpec](#cachespec)_ |  |  |  |
| `metaCache` _[CacheSpec](#cachespec)_ |  |  |  |


#### Track









| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `telemetry.plural.sh/v1alpha1` | | |
| `kind` _string_ | `Track` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[TrackSpec](#trackspec)_ |  |  |  |


#### TrackConfigSpec







_Appears in:_
- [TrackSpec](#trackspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `storage` _[StorageSpec](#storagespec)_ |  | \{ path:track \} |  |
| `segmentDurationSeconds` _integer_ |  | 3600 | Minimum: 1 <br /> |
| `retentionSeconds` _integer_ |  |  | Minimum: 1 <br /> |
| `page` _[TrackPageSpec](#trackpagespec)_ |  |  |  |
| `write` _[WriteSpec](#writespec)_ |  |  |  |
| `sharding` _[ShardingSpec](#shardingspec)_ |  |  |  |
| `request` _[TrackRequestSpec](#trackrequestspec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ |  |  |  |
| `namespaces` _string array_ |  | [default] |  |




#### TrackPageSpec







_Appears in:_
- [TrackConfigSpec](#trackconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `targetSizeBytes` _integer_ |  | 1048576 | Minimum: 1 <br /> |
| `maxSizeBytes` _integer_ |  | 4194304 | Minimum: 1 <br /> |
| `maxTraces` _integer_ |  | 1024 | Minimum: 1 <br /> |


#### TrackRequestSpec







_Appears in:_
- [TrackConfigSpec](#trackconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `maxRequestBytes` _integer_ |  | 10485760 | Minimum: 1 <br /> |
| `requestConcurrency` _integer_ |  | 64 | Minimum: 1 <br /> |
| `maxCandidates` _integer_ |  | 10000 | Minimum: 1 <br /> |
| `maxSpansPerTrace` _integer_ |  | 100000 | Minimum: 1 <br /> |
| `queryConcurrency` _integer_ |  | 8 | Minimum: 1 <br /> |
| `maxQueryLimit` _integer_ |  | 1000 | Minimum: 1 <br /> |


#### TrackSpec



TrackSpec defines the desired state of Track.



_Appears in:_
- [Track](#track)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[TrackMode](#trackmode)_ |  | Standalone | Enum: [Standalone Sharded] <br /> |
| `version` _string_ | Version is the canonical Track container image tag. It must be SemVer<br />without a leading "v". When omitted, deprecated image.tag is used, then<br />the operator's default version. |  | Pattern: `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0\|[1-9][0-9]*)\|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$` <br /> |
| `image` _[ImageSpec](#imagespec)_ |  | \{ pullPolicy:IfNotPresent repository:ghcr.io/pluralsh/track \} |  |
| `config` _[TrackConfigSpec](#trackconfigspec)_ |  |  |  |
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
- [LineSpec](#linespec)
- [MeterSpec](#meterspec)
- [TrackSpec](#trackspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `replicas` _integer_ | Replicas defaults to three for sharded writers and two for sharded<br />readers. Standalone mode uses exactly one writer and no reader. |  | Minimum: 0 <br /> |
| `nodeSelector` _object (keys:string, values:string)_ | NodeSelector is merged with podTemplate.spec.nodeSelector. Values here<br />take precedence when the same key is configured in both places. |  |  |
| `tolerations` _[Toleration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#toleration-v1-core) array_ | Tolerations are merged with podTemplate.spec.tolerations. A first-class<br />toleration replaces a podTemplate toleration with the same key, operator,<br />and effect; otherwise it is appended. |  |  |
| `podTemplate` _[PodTemplateSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#podtemplatespec-v1-core)_ | PodTemplate is the full Kubernetes pod template escape hatch. Operator<br />required fields and the first-class scheduling fields are merged into it. |  |  |
| `dataVolume` _[VolumeSpec](#volumespec)_ |  |  |  |
| `cacheVolume` _[VolumeSpec](#volumespec)_ |  |  |  |


#### WriteSpec







_Appears in:_
- [LineConfigSpec](#lineconfigspec)
- [MeterConfigSpec](#meterconfigspec)
- [TrackConfigSpec](#trackconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `durability` _[Durability](#durability)_ |  | written | Enum: [applied written durable] <br /> |
| `flushIntervalSeconds` _integer_ |  | 60 | Minimum: 0 <br /> |
| `remoteConcurrency` _integer_ |  | 16 | Minimum: 1 <br /> |
| `remoteRetries` _integer_ |  | 2 | Minimum: 0 <br /> |


