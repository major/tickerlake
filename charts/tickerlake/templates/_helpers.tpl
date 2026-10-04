{{/*
Expand the name of the chart.
*/}}
{{- define "tickerlake.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to
this (by the DNS naming spec). If the release name contains the chart name it
is used as a full name.
*/}}
{{- define "tickerlake.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "tickerlake.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "tickerlake.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tickerlake.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "tickerlake.labels" -}}
helm.sh/chart: {{ include "tickerlake.chart" . }}
{{ include "tickerlake.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Name of the ServiceAccount to use.
*/}}
{{- define "tickerlake.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "tickerlake.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding the Massive API key.
*/}}
{{- define "tickerlake.massiveSecret" -}}
{{- default (printf "%s-massive" (include "tickerlake.fullname" .)) .Values.massive.existingSecret }}
{{- end }}

{{/*
Name of the Secret holding the database DSN, selected by database.mode.
*/}}
{{- define "tickerlake.databaseSecret" -}}
{{- if eq .Values.database.mode "cnpg" -}}
{{- printf "%s-app" (default .Release.Name .Values.database.cnpg.clusterName) -}}
{{- else if eq .Values.database.mode "embedded" -}}
{{- printf "%s-postgres" (include "tickerlake.fullname" .) -}}
{{- else if eq .Values.database.mode "external" -}}
{{- if .Values.database.existingSecret.name -}}
{{- .Values.database.existingSecret.name -}}
{{- else if .Values.database.url -}}
{{- printf "%s-database" (include "tickerlake.fullname" .) -}}
{{- else -}}
{{- fail "database.mode=external requires database.existingSecret.name or database.url" -}}
{{- end -}}
{{- else -}}
{{- fail (printf "database.mode must be one of cnpg, embedded, external (got %q)" .Values.database.mode) -}}
{{- end -}}
{{- end }}

{{/*
Key within the database Secret that holds the DSN, selected by database.mode.
*/}}
{{- define "tickerlake.databaseSecretKey" -}}
{{- if eq .Values.database.mode "cnpg" -}}
{{- default "uri" .Values.database.cnpg.uriKey -}}
{{- else if eq .Values.database.mode "embedded" -}}
uri
{{- else -}}
{{- default "DATABASE_URL" .Values.database.existingSecret.key -}}
{{- end -}}
{{- end }}

{{/*
Environment entries for the tickerlake container: MASSIVE_API_KEY and
DATABASE_URL, both sourced from Secrets via secretKeyRef.
*/}}
{{- define "tickerlake.env" -}}
- name: MASSIVE_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "tickerlake.massiveSecret" . }}
      key: {{ .Values.massive.secretKey | quote }}
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "tickerlake.databaseSecret" . }}
      key: {{ include "tickerlake.databaseSecretKey" . | quote }}
{{- end }}

{{/*
Selector labels for the embedded Postgres StatefulSet and Services. The name
label uses the fullname so that the embedded workload is clearly identified.
*/}}
{{- define "tickerlake.embeddedPostgres.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tickerlake.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: embedded-postgres
{{- end }}

{{/*
Common labels for embedded Postgres resources.
*/}}
{{- define "tickerlake.embeddedPostgres.labels" -}}
helm.sh/chart: {{ include "tickerlake.chart" . }}
{{ include "tickerlake.embeddedPostgres.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
