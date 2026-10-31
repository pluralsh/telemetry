{{- define "telemetry-stack.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "telemetry-stack.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "telemetry-stack.labels" -}}
helm.sh/chart: {{ include "telemetry-stack.chart" . }}
app.kubernetes.io/name: {{ include "telemetry-stack.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* A product's storage: its spec.config.storage merged over global.storage. */}}
{{- define "telemetry-stack.storage" -}}
{{- $config := default dict (get (default dict .product.spec) "config") }}
{{- toYaml (mergeOverwrite (deepCopy .root.Values.global.storage) (deepCopy (default dict (get $config "storage")))) }}
{{- end }}
