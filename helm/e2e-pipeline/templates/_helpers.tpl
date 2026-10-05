{{/*
Common labels
*/}}
{{- define "e2e-pipeline.labels" -}}
app.kubernetes.io/part-of: e2e-pipeline
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/*
Image reference
*/}}
{{- define "e2e-pipeline.image" -}}
{{- if .global.imageRegistry -}}
{{ .global.imageRegistry }}/{{ .repository }}:{{ .tag }}
{{- else -}}
{{ .repository }}:{{ .tag }}
{{- end -}}
{{- end }}

{{/*
Namespace
*/}}
{{- define "e2e-pipeline.namespace" -}}
{{ .Values.namespace | default "pipeline" }}
{{- end }}
