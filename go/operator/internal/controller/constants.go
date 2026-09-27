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

	permissionRead  = "read"
	permissionWrite = "write"
	dataStoreMeter  = "Meter"
	dataStoreLine   = "Line"

	namespaceAuthMeterIndex  = "telemetry.plural.sh/namespace-auth-meter"
	meterSecretIndex         = "telemetry.plural.sh/meter-secret"
	namespaceAuthSecretIndex = "telemetry.plural.sh/namespace-auth-secret"

	conditionReady = "Ready"

	reasonReady                = "Ready"
	reasonProgressing          = "Progressing"
	reasonReconcileFailed      = "ReconcileFailed"
	reasonStorageResizing      = "StorageResizing"
	reasonStorageResizeBlocked = "StorageResizeBlocked"

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
