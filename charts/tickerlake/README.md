# tickerlake

A Helm chart that runs the [tickerlake](https://github.com/major/tickerlake) US
equity market ETL as a Kubernetes CronJob. The chart connects to a Postgres
database owned by an existing CloudNativePG (CNPG) Cluster.

The job runs the `update` subcommand on a schedule (22:30 UTC Monday to Friday
by default, after the US market close) and refreshes the trailing revision
window. You can override the subcommand to `backfill` for a first-time load.

## Prerequisites

- Kubernetes 1.29 or newer (the chart uses `spec.timeZone` on the CronJob).
- Helm 3.8 or newer.
- The [CloudNativePG](https://cloudnative-pg.io/) operator installed, plus a
  `Cluster` resource in the release namespace. The chart does **not** render
  the Cluster; it only consumes the `<cluster>-app` Secret that CNPG manages.
- A container image that contains the tickerlake CLI (Python 3.14). The chart
  does **not** ship or pin an image, so you **must provide your own** image via
  `image.repository` and `image.tag`.
- A StorageClass that supports `ReadWriteOnce` PersistentVolumeClaims (the chart
  creates a small output PVC).
- A Massive API key. For production, put it in a pre-created Secret and point
  `massive.existingSecret` at it.

## Usage

1. Install the CloudNativePG operator if you have not already:

   ```bash
   helm repo add cnpg https://cloudnative-pg.github.io/charts
   helm repo update
   helm upgrade --install cnpg cnpg/cloudnative-pg \
     --namespace cnpg-system --create-namespace
   ```

2. Create a CloudNativePG `Cluster` (adjust storage, replicas, and resources):

   ```yaml
   # tickerlake-cluster.yaml
   apiVersion: postgresql.cnpg.io/v1
   kind: Cluster
   metadata:
     name: tickerlake-db
   spec:
     instances: 3
     storage:
       size: 10Gi
     bootstrap:
       initdb:
         database: tickerlake
         owner: tickerlake
   ```

   ```bash
   kubectl apply -f tickerlake-cluster.yaml
   kubectl wait --for=condition=Ready cluster/tickerlake-db --timeout=10m
   ```

   Place the Cluster in the same namespace as the chart release, unless you
   replicate the `<cluster>-app` Secret into the release namespace.

3. Create the Massive API key Secret:

   ```bash
   kubectl create secret generic tickerlake-massive \
     --from-literal=MASSIVE_API_KEY=your-key-here
   ```

4. Install the chart. `database.clusterName` defaults to the release name, so
   set it explicitly if your Cluster has a different name:

   ```bash
   helm install tickerlake ./charts/tickerlake \
     --set image.repository=ghcr.io/you/tickerlake \
     --set image.tag=0.1.0 \
     --set massive.existingSecret=tickerlake-massive \
     --set database.clusterName=tickerlake-db
   ```

   The CronJob reads `DATABASE_URL` from the `<cluster>-app` Secret that
   CloudNativePG manages (key `uri` by default).

5. Check the CronJob and run it once, outside the schedule:

   ```bash
   kubectl get cronjob
   kubectl create job --from=cronjob/tickerlake tickerlake-manual
   kubectl logs -l job-name=tickerlake-manual -f
   ```

## Configuration reference

The full set of values is documented inline in `values.yaml` with `# --`
comments. To regenerate a values reference table automatically, install
[helm-docs](https://github.com/norwoodj/helm-docs) and run:

```bash
helm-docs charts/tickerlake
```

Key values:

| Value | Description | Default |
| --- | --- | --- |
| `image.repository` | tickerlake container image repository (required) | `""` |
| `image.tag` | Image tag; falls back to `.Chart.AppVersion` | `""` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `cronjob.schedule` | Cron schedule | `30 22 * * 1-5` |
| `cronjob.timezone` | Cron time zone | `UTC` |
| `cronjob.command` | Subcommand to run (`update`, `backfill`, `info`, `compact`) | `update` |
| `cronjob.outputDir` | Output directory inside the pod | `/data` |
| `cronjob.concurrencyPolicy` | `Allow`, `Forbid`, or `Replace` | `Forbid` |
| `cronjob.restartPolicy` | `OnFailure` or `Never` | `OnFailure` |
| `massive.existingSecret` | Pre-created Secret with the API key | `""` |
| `massive.secretKey` | Key inside that Secret | `MASSIVE_API_KEY` |
| `massive.apiKey` | Literal key; testing only | `""` |
| `database.clusterName` | CloudNativePG Cluster name | release name |
| `database.uriKey` | DSN key in the `<cluster>-app` Secret | `uri` |
| `persistence.size` | Output PVC size | `1Gi` |
| `serviceAccount.create` | Create a ServiceAccount | `true` |

### Security notes

- The literal `massive.apiKey` value creates a Secret in the release and is
  intended for testing only. Use `massive.existingSecret` in production.
- The CronJob container runs as a non-root user with a read-only root
  filesystem. A writable `emptyDir` is mounted at `/tmp`, and the output
  directory is a PVC.
- The CronJob connects directly to the Cluster's `-rw` Service on port 5432.
  Do not route it through a PgBouncer `Pooler` in transaction-pooling mode:
  tickerlake uses session-level advisory locks (`pg_advisory_lock`) to
  serialize writers, and advisory locks do not survive transaction pooling.

## Uninstall

```bash
helm uninstall tickerlake
```

The output PVC created by the chart (`<release>-data`) is **not** deleted by
`helm uninstall`. Delete it explicitly if you no longer need the data:

```bash
kubectl delete pvc -l app.kubernetes.io/instance=tickerlake
```

Uninstalling the chart leaves the CloudNativePG `Cluster` CR and its data
untouched. Remove those separately if desired.