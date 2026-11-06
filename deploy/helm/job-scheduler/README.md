# job-scheduler Helm chart

Deploys the job scheduler on Kubernetes: the API, two worker pools
(`default` and `long` queues), the scheduler and Flower.

It renders the same resources and security settings as the kustomize
manifests in [`deploy/k8s/`](../../k8s/). Pick one of the two; don't use both
in the same namespace.

**Postgres and Redis are not included.** Use managed, highly available
services and point the chart's Secret at them. See
[chapter 9](../../../docs/09-deployment.md#93-high-availability-the-full-picture).

Needs Kubernetes 1.30+ and Helm 3.8+ (or Helm 4).

## What you get

| Resource | Default |
|---|---|
| Deployment + Service + PDB + HPA `<release>-api` | 3 pods (HPA 3–10), PDB keeps 2 up, ClusterIP port 80 |
| Deployment + PDB + HPA `<release>-worker` | 3 pods (HPA 3–20), `default` queue |
| Deployment + PDB + HPA `<release>-worker-long` | 2 pods (HPA 2–10), `long` queue |
| Deployment + PDB `<release>-scheduler` | 2 pods, PDB keeps 1 up |
| Deployment + Service `<release>-flower` | 1 pod, internal only |
| ConfigMap `<release>-config` | Non-secret settings (`config`) |
| Secret `<release>-secrets` | Only when `secrets.existingSecret` is empty |
| ServiceAccount | No API token mounted |
| NetworkPolicies | Deny all incoming traffic except to the API |
| Ingress, ServiceMonitor, PrometheusRule | Off by default |

`<release>` means the release fullname. With `helm install job-scheduler ...`
it is just `job-scheduler`, so the API Service is `job-scheduler-api`.

Every pod except Flower runs the `migrate` init container
(`python -m app.migrate`) before the app starts. `AUTO_MIGRATE` is always
`"false"` in the ConfigMap. Migrations are lock-protected, so many pods can
start at once.

Pods run as non-root (UID 1000) with a read-only root filesystem, no
capabilities, no privilege escalation and the `RuntimeDefault` seccomp
profile. This meets the *restricted* Pod Security Standard.

## Install

```bash
# 1. Namespace, with the restricted Pod Security level
kubectl create namespace job-scheduler
kubectl label namespace job-scheduler pod-security.kubernetes.io/enforce=restricted

# 2. The Secret (better: from your secret manager)
kubectl -n job-scheduler create secret generic job-scheduler-secrets \
  --from-literal=DATABASE_URL='postgresql://jobs:…@db.internal:5432/jobs?sslmode=require' \
  --from-literal=CELERY_BROKER_URL='rediss://:…@redis.internal:6379/0' \
  --from-literal=API_KEYS='…' \
  --from-literal=FLOWER_BASIC_AUTH='admin:…' \
  --from-literal=WEBHOOK_SIGNING_SECRET='…'

# 3. The chart
helm install job-scheduler deploy/helm/job-scheduler \
  --namespace job-scheduler \
  --set image.repository=registry.example.com/job-scheduler \
  --set image.tag=5.0.0 \
  --set secrets.existingSecret=job-scheduler-secrets

kubectl -n job-scheduler get pods -w
```

Check it works:

```bash
kubectl -n job-scheduler port-forward svc/job-scheduler-api 8000:80 &
API_URL=http://localhost:8000 API_KEY=… python scripts/smoke_test.py
```

Keep your settings in a values file rather than many `--set` flags:
`helm install job-scheduler deploy/helm/job-scheduler -n job-scheduler -f my-values.yaml`.

## Upgrade

```bash
helm upgrade job-scheduler deploy/helm/job-scheduler -n job-scheduler -f my-values.yaml
```

- A change to `config` (or to a chart-created Secret) changes a checksum
  annotation on the pods, so they roll automatically.
- A change to an **existing** Secret does not. Restart the pods yourself:
  `kubectl -n job-scheduler rollout restart deployment -l app.kubernetes.io/instance=job-scheduler`.
- New pods run `migrate` first, so the database is upgraded before new code
  serves traffic.

See what an upgrade will change first with
[helm-diff](https://github.com/databus23/helm-diff):
`helm diff upgrade job-scheduler deploy/helm/job-scheduler -n job-scheduler -f my-values.yaml`.

## Uninstall

```bash
helm uninstall job-scheduler -n job-scheduler
```

This removes everything the chart created. It does not touch your database,
Redis, an existing Secret or the namespace.

## Values

Each component (`api`, `worker`, `workerLong`, `scheduler`, `flower`) has the
same keys. `values.schema.json` checks them, so a typo fails at install time.

### Image and identity

| Key | Default | Description |
|---|---|---|
| `image.repository` | `job-scheduler` | Image name, e.g. `ghcr.io/acme/job-scheduler`. |
| `image.tag` | `""` | Defaults to the chart `appVersion` (`5.0.0`). |
| `image.digest` | `""` | `sha256:…`; wins over the tag. |
| `image.pullPolicy` | `IfNotPresent` | |
| `imagePullSecrets` | `[]` | e.g. `[{name: regcred}]`. |
| `nameOverride` / `fullnameOverride` | `""` | Change the resource name prefix. |
| `serviceAccount.create` | `true` | |
| `serviceAccount.name` | `""` | Defaults to the fullname. |
| `serviceAccount.annotations` | `{}` | e.g. for workload identity. |
| `commonLabels` / `commonAnnotations` | `{}` | Added to every resource. |

### Settings and secrets

| Key | Default | Description |
|---|---|---|
| `config` | see `values.yaml` | Map of non-secret env vars for the ConfigMap. Same defaults as `deploy/k8s/configmap.yaml`. `AUTO_MIGRATE` is not allowed (fixed to `"false"`). |
| `extraEnv` | `[]` | Extra env vars (Kubernetes `EnvVar` objects) for every container, including `migrate`. |
| `secrets.existingSecret` | `""` | Use a Secret you manage. The chart then creates none. |
| `secrets.values.API_KEYS` | `""` | Comma-separated API keys. |
| `secrets.values.DATABASE_URL` | `""` | Postgres URL. |
| `secrets.values.CELERY_BROKER_URL` | `""` | Redis URL; wins over `REDIS_PASSWORD`. |
| `secrets.values.REDIS_PASSWORD` | `""` | Used with `config.REDIS_HOST` / `REDIS_PORT`. |
| `secrets.values.FLOWER_BASIC_AUTH` | `""` | `user:password` for Flower. |
| `secrets.values.WEBHOOK_SIGNING_SECRET` | `""` | For app versions that sign webhooks. |

Empty `secrets.values` are left out of the Secret. Values set here are stored
in the Helm release (a Secret in the namespace). For production, prefer
`existingSecret`.

### Pod defaults

| Key | Default | Description |
|---|---|---|
| `podSecurityContext` | non-root, UID/GID/fsGroup 1000, `RuntimeDefault` seccomp | Pod-level security. |
| `securityContext` | no privilege escalation, read-only root FS, drop `ALL` | Every container, including `migrate`. |
| `topologySpread.maxSkew` | `1` | |
| `topologySpread.whenUnsatisfiable` | `ScheduleAnyway` | |
| `topologySpread.topologyKeys` | hostname, zone | One constraint per key. |
| `podAnnotations` / `podLabels` | `{}` | Merged with the component's own. |
| `nodeSelector` / `tolerations` / `affinity` | empty | Used when a component sets none. |
| `priorityClassName` | `""` | |
| `extraVolumes` / `extraVolumeMounts` | `[]` | A writable `/tmp` is always mounted. |
| `migrate.resources` | 50m / 128Mi, limit 256Mi | The `migrate` init container. |

### Per component

Replace `<c>` with `api`, `worker`, `workerLong`, `scheduler` or `flower`.

| Key | Description |
|---|---|
| `<c>.enabled` | Deploy this component. |
| `<c>.replicaCount` | Pod count when the HPA is off. With the HPA on, `replicas` is left to the HPA. |
| `<c>.revisionHistoryLimit`, `<c>.strategy` | Deployment rollout settings. |
| `<c>.command`, `<c>.args` | Container command, and extra args after it. Worker concurrency is in `command`. |
| `<c>.containerPort` | `api` and `flower` only. |
| `<c>.extraEnv` | Env vars for this component only. |
| `<c>.resources` | Requests and limits. |
| `<c>.startupProbe`, `livenessProbe`, `readinessProbe` | Kubernetes probes; `{}` turns one off. |
| `<c>.lifecycle` | e.g. the API's `preStop` sleep. |
| `<c>.terminationGracePeriodSeconds` | Workers: 330 s, longer than the task soft time limit. |
| `<c>.topologySpread` | Spread across nodes and zones (off for Flower). |
| `<c>.topologySpreadConstraints` | Your own list; replaces the generated one. |
| `<c>.podSecurityContext` | Replaces the default when set (Flower's has no `fsGroup`, as in `deploy/k8s`). |
| `<c>.podAnnotations`, `podLabels`, `nodeSelector`, `tolerations`, `affinity` | Per-component scheduling and metadata. |
| `<c>.service.type`, `.port`, `.annotations` | `api` (port 80) and `flower` (port 5555). |
| `<c>.hpa.enabled`, `minReplicas`, `maxReplicas`, `targetCPUUtilizationPercentage`, `behavior` | CPU autoscaler. On for `api`, `worker`, `workerLong`. |
| `<c>.pdb.enabled`, `minAvailable` / `maxUnavailable` | Disruption budget. Set the other key to `null` when switching. |

Defaults per component:

| | api | worker | workerLong | scheduler | flower |
|---|---|---|---|---|---|
| replicas | 3 | 3 | 2 | 2 | 1 |
| HPA | 3–10 | 3–20 | 2–10 | off | off |
| PDB | minAvailable 2 | maxUnavailable 1 | maxUnavailable 1 | minAvailable 1 | off |
| rollout | 0 down / 1 extra | 1 / 1 | 1 / 1 | 0 / 1 | default |
| requests | 250m / 256Mi | 500m / 512Mi | 500m / 512Mi | 100m / 192Mi | 50m / 128Mi |
| memory limit | 512Mi | 1Gi | 1Gi | 384Mi | 256Mi |

### Networking

| Key | Default | Description |
|---|---|---|
| `ingress.enabled` | `false` | Ingress for the API Service. |
| `ingress.className` | `""` | e.g. `nginx`. |
| `ingress.host` | `jobs.example.com` | |
| `ingress.path` / `pathType` | `/` / `Prefix` | |
| `ingress.annotations` | `{}` | e.g. `cert-manager.io/cluster-issuer`. |
| `ingress.tls.enabled` / `secretName` | `false` / `""` | TLS certificate Secret. |
| `networkPolicy.enabled` | `true` | Deny incoming traffic to this release's pods, except the API port. |
| `networkPolicy.apiIngressFrom` | `[]` | Who may reach the API (`NetworkPolicyPeer` list). Empty means anyone. |

### Monitoring (needs the Prometheus Operator CRDs)

The API serves Prometheus metrics at `/metrics` (5.0+). The ServiceMonitor
scrapes it; the PrometheusRule can carry the standard alerts. To turn on both
with the ready-made rules, add
[`examples/monitoring-values.yaml`](examples/monitoring-values.yaml) as an extra
`-f` file. A Grafana dashboard is in `deploy/monitoring/grafana/dashboards/`.

| Key | Default | Description |
|---|---|---|
| `metrics.serviceMonitor.enabled` | `false` | Scrape the API Service. |
| `metrics.serviceMonitor.path` | `/metrics` | |
| `metrics.serviceMonitor.interval` / `scrapeTimeout` | `30s` / `10s` | |
| `metrics.serviceMonitor.labels` | `{}` | e.g. `release: kube-prometheus-stack`, so Prometheus picks it up. |
| `metrics.serviceMonitor.relabelings` / `metricRelabelings` | `[]` | |
| `metrics.prometheusRule.enabled` | `false` | Create a PrometheusRule. |
| `metrics.prometheusRule.labels` | `{}` | |
| `metrics.prometheusRule.rules` | `[]` | Prometheus rules, rendered into one group. |

## Examples

### External managed Postgres and Redis (recommended)

Create the Secret from your secret manager (External Secrets Operator,
Sealed Secrets, Vault). Then:

```yaml
# my-values.yaml
image:
  repository: registry.example.com/job-scheduler
  tag: "5.0.0"

secrets:
  existingSecret: job-scheduler-secrets   # DATABASE_URL, CELERY_BROKER_URL, API_KEYS, ...

config:
  DB_POOL_SIZE: "10"
```

Example values in that Secret:

- `DATABASE_URL`: `postgresql://jobs:…@mydb.abc123.eu-west-1.rds.amazonaws.com:5432/jobs?sslmode=require`
- `CELERY_BROKER_URL`: `rediss://:…@master.myredis.abc123.euw1.cache.amazonaws.com:6379/0`

### Redis with a host and password instead of a URL

```yaml
config:
  REDIS_HOST: redis.internal
  REDIS_PORT: "6379"
secrets:
  values:
    REDIS_PASSWORD: "…"
    DATABASE_URL: "postgresql://jobs:…@db.internal:5432/jobs?sslmode=require"
    API_KEYS: "…"
    FLOWER_BASIC_AUTH: "admin:…"
```

### Redis Sentinel

```yaml
config:
  REDIS_SENTINEL_MASTER: mymaster
# and in the Secret:
#   CELERY_BROKER_URL=sentinel://:pw@s1:26379;sentinel://:pw@s2:26379
```

### Public ingress with TLS, monitoring

```yaml
ingress:
  enabled: true
  className: nginx
  host: jobs.example.com
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt
  tls:
    enabled: true
    secretName: jobs-example-com-tls

networkPolicy:
  apiIngressFrom:
    - namespaceSelector:
        matchLabels: { kubernetes.io/metadata.name: ingress-nginx }
    - namespaceSelector:
        matchLabels: { kubernetes.io/metadata.name: monitoring }   # Prometheus

metrics:
  serviceMonitor:
    enabled: true
    labels: { release: kube-prometheus-stack }
  prometheusRule:
    enabled: true
    labels: { release: kube-prometheus-stack }
    rules:
      - alert: JobSchedulerApiDown
        expr: up{job="job-scheduler-api"} == 0
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: "API pod {{ $labels.pod }} is down"
```

If you limit `networkPolicy.apiIngressFrom`, include Prometheus's namespace,
or the ServiceMonitor can't scrape.

### More workers, more concurrency

```yaml
worker:
  command: ["celery", "-A", "app.celery_app", "worker", "-Q", "default", "--concurrency=8", "--loglevel=info"]
  resources:
    requests: { cpu: "1", memory: 1Gi }
    limits: { memory: 2Gi }
  hpa:
    maxReplicas: 40
```

## How it differs from `deploy/k8s`

The pods, probes, commands, security settings and budgets are the same. A
few things differ on purpose, because this is a Helm chart:

| | kustomize | Helm chart | Why |
|---|---|---|---|
| Names | `api`, `worker`, … | `<release>-api`, `<release>-worker`, … | Several releases can share a namespace. |
| Labels / selectors | `app.kubernetes.io/name: api` | `name: job-scheduler`, `instance: <release>`, `component: api` | Standard Helm labels. |
| Namespace | Created, labelled `restricted` | Not created; label it yourself | Helm should not own the namespace it installs into. |
| Secret | Never created | Created from `secrets.values` unless `existingSecret` is set | Optional convenience. |
| `replicas` with an HPA | Set | Left to the HPA | Stops `helm upgrade` from resetting the HPA's count. |
| `default-deny-ingress` | All pods in the namespace | This release's pods | Doesn't affect other apps in the namespace. |
| Image tag | `4.0.0` | `appVersion` (`5.0.0`) | Follows the chart. |
| Pod checksum annotations | None | `checksum/config`, `checksum/secret` | Config changes roll the pods. |

## Checking the chart

```bash
helm lint deploy/helm/job-scheduler
helm lint deploy/helm/job-scheduler -f deploy/helm/job-scheduler/ci/full-values.yaml
helm template t deploy/helm/job-scheduler -f deploy/helm/job-scheduler/ci/full-values.yaml \
  | kubeconform -strict -summary \
      -schema-location default \
      -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'
```

`ci/full-values.yaml` turns on ingress, the ServiceMonitor, the
PrometheusRule and an existing Secret.
