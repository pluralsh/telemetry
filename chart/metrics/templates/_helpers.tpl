{{- define "metrics.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "metrics.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "metrics.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "metrics.labels" -}}
helm.sh/chart: {{ include "metrics.chart" . }}
app.kubernetes.io/name: {{ include "metrics.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "metrics.selectorLabels" -}}
app.kubernetes.io/name: {{ include "metrics.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "metrics.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "metrics.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "metrics.writerName" -}}
{{- printf "%s-writer" (include "metrics.fullname" . | trunc 56 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.readerName" -}}
{{- printf "%s-reader" (include "metrics.fullname" . | trunc 56 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.headlessName" -}}
{{- printf "%s-writer-headless" (include "metrics.fullname" . | trunc 47 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.standaloneHeadlessName" -}}
{{- printf "%s-headless" (include "metrics.fullname" . | trunc 54 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.readerHeadlessName" -}}
{{- printf "%s-reader-headless" (include "metrics.fullname" . | trunc 47 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.standaloneConfigName" -}}
{{- printf "%s-config" (include "metrics.fullname" . | trunc 56 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.writerConfigName" -}}
{{- printf "%s-writer-config" (include "metrics.fullname" . | trunc 49 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.readerConfigName" -}}
{{- printf "%s-reader-config" (include "metrics.fullname" . | trunc 49 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.assignmentName" -}}
{{- printf "%s-writer-shard-map" (include "metrics.fullname" . | trunc 46 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.coordinatorLeaseName" -}}
{{- printf "%s-writer-shard-coordinator" (include "metrics.fullname" . | trunc 38 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.shardLeasePrefix" -}}
{{- printf "%s-writer-shard" (include "metrics.fullname" . | trunc 32 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.internalSecretName" -}}
{{- if .Values.internalToken.existingSecret -}}
{{- .Values.internalToken.existingSecret -}}
{{- else -}}
{{- printf "%s-internal-token" (include "metrics.fullname" . | trunc 48 | trimSuffix "-") -}}
{{- end -}}
{{- end }}

{{- define "metrics.shardingRoleName" -}}
{{- printf "%s-sharding" (include "metrics.fullname" . | trunc 54 | trimSuffix "-") -}}
{{- end }}

{{- define "metrics.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end }}

{{- define "metrics.validate" -}}
{{- if and (eq .Values.mode "sharded") (ge (int .Values.sharding.renewIntervalSeconds) (int .Values.sharding.leaseDurationSeconds)) -}}
{{- fail "sharding.renewIntervalSeconds must be less than sharding.leaseDurationSeconds" -}}
{{- end -}}
{{- end }}
