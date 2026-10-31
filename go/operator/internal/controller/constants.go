package controller

import (
	"time"

	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

type component = resources.Component

const (
	componentNone       = resources.ComponentNone
	componentStandalone = resources.ComponentStandalone
	componentWriter     = resources.ComponentWriter
	componentReader     = resources.ComponentReader

	permissionRead   = "read"
	permissionWrite  = "write"
	dataStoreMetrics = "Metrics"
	dataStoreLogs    = "Logs"
	dataStoreTraces  = "Traces"
	dataStoreVector  = "Vector"

	namespaceAuthMetricsIndex = "telemetry.plural.sh/namespace-auth-metrics"
	metricsSecretIndex        = "telemetry.plural.sh/metrics-secret"
	namespaceAuthSecretIndex  = "telemetry.plural.sh/namespace-auth-secret"

	conditionReady = "Ready"

	reasonReady                = "Ready"
	reasonProgressing          = "Progressing"
	reasonReconcileFailed      = "ReconcileFailed"
	reasonStorageResizing      = "StorageResizing"
	reasonStorageResizeBlocked = "StorageResizeBlocked"
	reasonMemoryLimitTooLow    = "MemoryLimitTooLow"
	// reasonWriterScaleDownRejected: a Vector's writers each own a shard,
	// and shards don't merge.
	reasonWriterScaleDownRejected = "WriterScaleDownRejected"
	messageWorkloadsReady         = "all workloads are ready"

	suffixConfig        = "config"
	suffixHeadless      = "headless"
	suffixInternalToken = "internal-token"
	suffixSharding      = "sharding"

	configHashAnnotation = resources.ConfigHashAnnotation
	tokenKey             = resources.TokenKey

	storageResizeRequeue = 2 * time.Second
)

func configKey(component resources.Component) string {
	return resources.ConfigKey(component)
}
