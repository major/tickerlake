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
Name of the Secret holding the database DSN. CloudNativePG generates a
<cluster>-app Secret of type kubernetes.io/basic-auth that carries the
DSN under the "uri" key by default.
*/}}
{{- define "tickerlake.databaseSecret" -}}
{{- printf "%s-app" (default .Release.Name .Values.database.clusterName) -}}
{{- end }}

{{/*
Key within the database Secret that holds the DSN.
*/}}
{{- define "tickerlake.databaseSecretKey" -}}
{{- default "uri" .Values.database.uriKey -}}
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
