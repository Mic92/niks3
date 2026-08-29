{{- define "niks3.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "niks3.fullname" -}}
{{- if contains .Chart.Name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "niks3.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{ include "niks3.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "niks3.selectorLabels" -}}
app.kubernetes.io/name: {{ include "niks3.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "niks3.serverSelectorLabels" -}}
{{ include "niks3.selectorLabels" . }}
app.kubernetes.io/component: server
{{- end }}

{{- define "niks3.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "niks3.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "niks3.image" -}}
{{ .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end }}

{{- define "niks3.dbSecretName" -}}
{{- if not (or .Values.database.existingSecret .Values.database.url) }}
{{- fail "set database.existingSecret or database.url" }}
{{- end }}
{{- default (printf "%s-db" (include "niks3.fullname" .)) .Values.database.existingSecret }}
{{- end }}

{{- define "niks3.tokenSecretName" -}}
{{- if not (or .Values.auth.existingSecret .Values.auth.token) }}
{{- fail "set auth.existingSecret or auth.token" }}
{{- end }}
{{- default (printf "%s-token" (include "niks3.fullname" .)) .Values.auth.existingSecret }}
{{- end }}

{{/* OIDC providers map incl. the synthesized Kubernetes one; empty when unused. */}}
{{- define "niks3.oidcProviders" -}}
{{- $providers := deepCopy (.Values.auth.oidcProviders | default dict) }}
{{- with .Values.auth.workloadIdentity }}
{{- if .enabled }}
{{- $subjects := list }}
{{- range (required "auth.workloadIdentity.allowedServiceAccounts must not be empty" .allowedServiceAccounts) }}
{{- $subjects = append $subjects (printf "system:serviceaccount:%s" .) }}
{{- end }}
{{- $_ := set $providers "kubernetes" (dict
  "issuer" .issuer
  "jwks_url" "https://kubernetes.default.svc/openid/v1/jwks"
  "audience" .audience
  "bound_subject" $subjects
  "scopes" .scopes
  "ca_file" "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
  "bearer_token_file" "/var/run/secrets/kubernetes.io/serviceaccount/token") }}
{{- end }}
{{- end }}
{{- if $providers }}{{ toJson (dict "providers" $providers) }}{{- end }}
{{- end }}

{{- define "niks3.s3SecretName" -}}
{{- if not (or .Values.s3.existingSecret .Values.s3.accessKey) }}
{{- fail "set s3.useIAM, s3.existingSecret or s3.accessKey/secretKey" }}
{{- end }}
{{- default (printf "%s-s3" (include "niks3.fullname" .)) .Values.s3.existingSecret }}
{{- end }}
