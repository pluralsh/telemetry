{{- define "meter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "meter.fullname" -}}
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

{{- define "meter.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "meter.labels" -}}
helm.sh/chart: {{ include "meter.chart" . }}
app.kubernetes.io/name: {{ include "meter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "meter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "meter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "meter.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "meter.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "meter.writerName" -}}
{{- printf "%s-writer" (include "meter.fullname" . | trunc 56 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.readerName" -}}
{{- printf "%s-reader" (include "meter.fullname" . | trunc 56 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.headlessName" -}}
{{- printf "%s-writer-headless" (include "meter.fullname" . | trunc 47 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.standaloneHeadlessName" -}}
{{- printf "%s-headless" (include "meter.fullname" . | trunc 54 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.readerHeadlessName" -}}
{{- printf "%s-reader-headless" (include "meter.fullname" . | trunc 47 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.standaloneConfigName" -}}
{{- printf "%s-config" (include "meter.fullname" . | trunc 56 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.writerConfigName" -}}
{{- printf "%s-writer-config" (include "meter.fullname" . | trunc 49 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.readerConfigName" -}}
{{- printf "%s-reader-config" (include "meter.fullname" . | trunc 49 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.assignmentName" -}}
{{- printf "%s-writer-shard-assignments" (include "meter.fullname" . | trunc 38 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.coordinatorLeaseName" -}}
{{- printf "%s-writer-shard-coordinator" (include "meter.fullname" . | trunc 38 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.shardLeasePrefix" -}}
{{- printf "%s-writer-shard" (include "meter.fullname" . | trunc 32 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.internalSecretName" -}}
{{- if .Values.internalToken.existingSecret -}}
{{- .Values.internalToken.existingSecret -}}
{{- else -}}
{{- printf "%s-internal-token" (include "meter.fullname" . | trunc 48 | trimSuffix "-") -}}
{{- end -}}
{{- end }}

{{- define "meter.shardingRoleName" -}}
{{- printf "%s-sharding" (include "meter.fullname" . | trunc 54 | trimSuffix "-") -}}
{{- end }}

{{- define "meter.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end }}

{{- define "meter.validate" -}}
{{- if and (eq .Values.mode "sharded") (ge (int .Values.sharding.renewIntervalSeconds) (int .Values.sharding.leaseDurationSeconds)) -}}
{{- fail "sharding.renewIntervalSeconds must be less than sharding.leaseDurationSeconds" -}}
{{- end -}}
{{- end }}
