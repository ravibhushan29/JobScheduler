{{/* vim: set filetype=mustache: */}}

{{/* Chart name. */}}
{{- define "job-scheduler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name. Truncated so "<fullname>-worker-long" stays
within the 63-character label limit.
*/}}
{{- define "job-scheduler.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 51 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 51 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 51 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "job-scheduler.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels on every resource. */}}
{{- define "job-scheduler.labels" -}}
helm.sh/chart: {{ include "job-scheduler.chart" . }}
{{ include "job-scheduler.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: job-scheduler
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* Labels that identify this release's pods. */}}
{{- define "job-scheduler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "job-scheduler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Component labels. Call with (dict "root" $ "name" "api"). */}}
{{- define "job-scheduler.componentLabels" -}}
{{ include "job-scheduler.labels" .root }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{- define "job-scheduler.componentSelectorLabels" -}}
{{ include "job-scheduler.selectorLabels" .root }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{/* Resource name of a component, e.g. "<fullname>-worker-long". */}}
{{- define "job-scheduler.componentName" -}}
{{- printf "%s-%s" (include "job-scheduler.fullname" .root) .name -}}
{{- end -}}

{{/* metadata.annotations from commonAnnotations; include without nindent. */}}
{{- define "job-scheduler.commonAnnotations" -}}
{{- with .Values.commonAnnotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
{{- end }}
{{- end -}}

{{- define "job-scheduler.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "job-scheduler.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "job-scheduler.configMapName" -}}
{{- printf "%s-config" (include "job-scheduler.fullname" .) -}}
{{- end -}}

{{- define "job-scheduler.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{- .Values.secrets.existingSecret -}}
{{- else -}}
{{- printf "%s-secrets" (include "job-scheduler.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "job-scheduler.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
{{- end -}}

{{/* envFrom + env shared by the migrate init container and app containers. */}}
{{- define "job-scheduler.env" -}}
{{- $root := .root -}}
envFrom:
  - configMapRef:
      name: {{ include "job-scheduler.configMapName" $root }}
  - secretRef:
      name: {{ include "job-scheduler.secretName" $root }}
{{- $env := concat ($root.Values.extraEnv | default list) (.component.extraEnv | default list) }}
{{- with $env }}
env:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- define "job-scheduler.volumeMounts" -}}
volumeMounts:
  - name: tmp
    mountPath: /tmp
  {{- with .Values.extraVolumeMounts }}
  {{- toYaml . | nindent 2 }}
  {{- end }}
{{- end -}}

{{/*
One Deployment. Call with
  (dict "root" $ "key" "workerLong" "name" "worker-long" "migrate" true)
"migrate" adds the migrate init container (every component except Flower,
as in deploy/k8s).
*/}}
{{- define "job-scheduler.deployment" -}}
{{- $root := .root -}}
{{- $c := index $root.Values .key -}}
{{- $name := .name -}}
{{- $ctx := dict "root" $root "name" $name -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "job-scheduler.componentName" $ctx }}
  namespace: {{ $root.Release.Namespace }}
  labels:
    {{- include "job-scheduler.componentLabels" $ctx | nindent 4 }}
  {{- include "job-scheduler.commonAnnotations" $root }}
spec:
  {{- if not $c.hpa.enabled }}
  replicas: {{ $c.replicaCount }}
  {{- end }}
  revisionHistoryLimit: {{ $c.revisionHistoryLimit }}
  {{- with $c.strategy }}
  strategy:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "job-scheduler.componentSelectorLabels" $ctx | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "job-scheduler.componentSelectorLabels" $ctx | nindent 8 }}
        {{- with (merge (dict) ($c.podLabels | default dict) ($root.Values.podLabels | default dict)) }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      annotations:
        checksum/config: {{ include (print $root.Template.BasePath "/configmap.yaml") $root | sha256sum }}
        {{- if not $root.Values.secrets.existingSecret }}
        checksum/secret: {{ include (print $root.Template.BasePath "/secret.yaml") $root | sha256sum }}
        {{- end }}
        {{- with (merge (dict) ($c.podAnnotations | default dict) ($root.Values.podAnnotations | default dict)) }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
    spec:
      serviceAccountName: {{ include "job-scheduler.serviceAccountName" $root }}
      automountServiceAccountToken: false
      {{- with $root.Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $root.Values.priorityClassName }}
      priorityClassName: {{ . }}
      {{- end }}
      securityContext:
        {{- toYaml ($c.podSecurityContext | default $root.Values.podSecurityContext) | nindent 8 }}
      {{- if $c.topologySpreadConstraints }}
      topologySpreadConstraints:
        {{- toYaml $c.topologySpreadConstraints | nindent 8 }}
      {{- else if $c.topologySpread }}
      topologySpreadConstraints:
        {{- range $root.Values.topologySpread.topologyKeys }}
        - maxSkew: {{ $root.Values.topologySpread.maxSkew }}
          topologyKey: {{ . }}
          whenUnsatisfiable: {{ $root.Values.topologySpread.whenUnsatisfiable }}
          labelSelector:
            matchLabels:
              {{- include "job-scheduler.componentSelectorLabels" $ctx | nindent 14 }}
        {{- end }}
      {{- end }}
      {{- with ($c.nodeSelector | default $root.Values.nodeSelector) }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with ($c.tolerations | default $root.Values.tolerations) }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with ($c.affinity | default $root.Values.affinity) }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- if .migrate }}
      initContainers:
        # Idempotent and lock-protected, so every pod may run it safely.
        - name: migrate
          image: {{ include "job-scheduler.image" $root }}
          imagePullPolicy: {{ $root.Values.image.pullPolicy }}
          command: ["python", "-m", "app.migrate"]
          {{- include "job-scheduler.env" (dict "root" $root "component" $c) | nindent 10 }}
          securityContext:
            {{- toYaml $root.Values.securityContext | nindent 12 }}
          resources:
            {{- toYaml $root.Values.migrate.resources | nindent 12 }}
          {{- include "job-scheduler.volumeMounts" $root | nindent 10 }}
      {{- end }}
      containers:
        - name: {{ $name }}
          image: {{ include "job-scheduler.image" $root }}
          imagePullPolicy: {{ $root.Values.image.pullPolicy }}
          command:
            {{- toYaml $c.command | nindent 12 }}
          {{- with $c.args }}
          args:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- if $c.containerPort }}
          ports:
            - name: http
              containerPort: {{ $c.containerPort }}
              protocol: TCP
          {{- end }}
          {{- include "job-scheduler.env" (dict "root" $root "component" $c) | nindent 10 }}
          securityContext:
            {{- toYaml $root.Values.securityContext | nindent 12 }}
          resources:
            {{- toYaml $c.resources | nindent 12 }}
          {{- with $c.startupProbe }}
          startupProbe:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with $c.livenessProbe }}
          livenessProbe:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with $c.readinessProbe }}
          readinessProbe:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with $c.lifecycle }}
          lifecycle:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- include "job-scheduler.volumeMounts" $root | nindent 10 }}
      terminationGracePeriodSeconds: {{ $c.terminationGracePeriodSeconds }}
      volumes:
        - name: tmp
          emptyDir: {}
        {{- with $root.Values.extraVolumes }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
{{- end -}}

{{/* The components, in a fixed order: key, resource name, runs migrate. */}}
{{- define "job-scheduler.components" -}}
- { key: api, name: api, migrate: true }
- { key: worker, name: worker, migrate: true }
- { key: workerLong, name: worker-long, migrate: true }
- { key: scheduler, name: scheduler, migrate: true }
- { key: flower, name: flower, migrate: false }
{{- end -}}
