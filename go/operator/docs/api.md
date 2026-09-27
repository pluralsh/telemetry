# API Reference

## Packages
- [telemetry.plural.sh/v1alpha1](#telemetrypluralshv1alpha1)


## telemetry.plural.sh/v1alpha1

Package v1alpha1 contains API Schema definitions for the telemetry v1alpha1 API group.

### Resource Types
- [Meter](#meter)
- [NamespaceAuthentication](#namespaceauthentication)



#### AccessSpec







_Appears in:_
- [AuthSpec](#authspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `read` _[BasicCredentialSpec](#basiccredentialspec) array_ |  |  |  |
| `write` _[BasicCredentialSpec](#basiccredentialspec) array_ |  |  |  |


#### AuthSpec







_Appears in:_
- [MeterConfigSpec](#meterconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `global` _[AccessSpec](#accessspec)_ |  |  |  |
| `jwt` _[JWTSpec](#jwtspec)_ |  |  |  |
| `internalTokenSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ |  |  |  |


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
| `kind` _string_ |  |  | Enum: [Meter] <br /> |
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


#### ImageSpec







_Appears in:_
- [MeterSpec](#meterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `repository` _string_ |  | ghcr.io/pluralsh/meter |  |
| `tag` _string_ |  |  |  |
| `pullPolicy` _[PullPolicy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#pullpolicy-v1-core)_ |  | IfNotPresent | Enum: [Always IfNotPresent Never] <br /> |


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
| `storage` _[StorageSpec](#storagespec)_ |  |  |  |
| `readerCacheCapacity` _integer_ |  | 268435456 | Minimum: 1 <br /> |
| `write` _[WriteSpec](#writespec)_ |  |  |  |
| `sharding` _[ShardingSpec](#shardingspec)_ |  |  |  |
| `auth` _[AuthSpec](#authspec)_ |  |  |  |
| `namespaces` _string array_ |  | [default] |  |


#### MeterMode

_Underlying type:_ _string_





_Appears in:_
- [MeterSpec](#meterspec)

| Field | Description |
| --- | --- |
| `Standalone` |  |
| `Sharded` |  |


#### MeterSpec



MeterSpec defines the desired state of Meter.



_Appears in:_
- [Meter](#meter)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[MeterMode](#metermode)_ |  | Standalone | Enum: [Standalone Sharded] <br /> |
| `image` _[ImageSpec](#imagespec)_ |  |  |  |
| `config` _[MeterConfigSpec](#meterconfigspec)_ |  |  |  |
| `writer` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `reader` _[WorkloadSpec](#workloadspec)_ |  |  |  |
| `service` _[ServiceSpec](#servicespec)_ |  |  |  |




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
| `type` _[ObjectStoreType](#objectstoretype)_ |  | Local | Enum: [InMemory Local Aws] <br /> |
| `path` _string_ |  |  |  |
| `region` _string_ |  |  |  |
| `bucket` _string_ |  |  |  |


#### ObjectStoreType

_Underlying type:_ _string_





_Appears in:_
- [ObjectStoreSpec](#objectstorespec)

| Field | Description |
| --- | --- |
| `InMemory` |  |
| `Local` |  |
| `Aws` |  |


#### ServiceSpec







_Appears in:_
- [MeterSpec](#meterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `httpPort` _integer_ |  | 8080 |  |
| `grpcPort` _integer_ |  | 9090 |  |
| `type` _[ServiceType](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#servicetype-v1-core)_ |  | ClusterIP | Enum: [ClusterIP NodePort LoadBalancer] <br /> |
| `annotations` _object (keys:string, values:string)_ |  |  |  |


#### ShardingSpec







_Appears in:_
- [MeterConfigSpec](#meterconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `virtualShards` _integer_ |  | 64 | Minimum: 1 <br /> |
| `leaseDurationSeconds` _integer_ |  | 15 | Minimum: 1 <br /> |
| `renewIntervalSeconds` _integer_ |  | 5 | Minimum: 1 <br /> |
| `watchPollIntervalSeconds` _integer_ |  | 2 | Minimum: 1 <br /> |


#### StorageSpec







_Appears in:_
- [MeterConfigSpec](#meterconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `path` _string_ |  | meter |  |
| `settingsPath` _string_ |  |  |  |
| `objectStore` _[ObjectStoreSpec](#objectstorespec)_ |  |  |  |
| `blockCache` _[CacheSpec](#cachespec)_ |  |  |  |
| `metaCache` _[CacheSpec](#cachespec)_ |  |  |  |


#### VolumeSpec



VolumeSpec configures one of the fixed data or cache workload volumes.



_Appears in:_
- [WorkloadSpec](#workloadspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `emptyDir` _[EmptyDirVolumeSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#emptydirvolumesource-v1-core)_ |  |  |  |
| `persistentVolumeClaim` _[PersistentVolumeClaimSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#persistentvolumeclaimspec-v1-core)_ |  |  |  |


#### WorkloadSpec







_Appears in:_
- [MeterSpec](#meterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `replicas` _integer_ |  |  | Minimum: 0 <br /> |
| `podTemplate` _[PodTemplateSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#podtemplatespec-v1-core)_ |  |  |  |
| `dataVolume` _[VolumeSpec](#volumespec)_ |  |  |  |
| `cacheVolume` _[VolumeSpec](#volumespec)_ |  |  |  |


#### WriteSpec







_Appears in:_
- [MeterConfigSpec](#meterconfigspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `durability` _[Durability](#durability)_ |  | written | Enum: [applied written durable] <br /> |
| `flushIntervalSeconds` _integer_ |  | 60 | Minimum: 0 <br /> |
| `remoteConcurrency` _integer_ |  | 16 | Minimum: 1 <br /> |
| `remoteRetries` _integer_ |  | 2 | Minimum: 0 <br /> |


