# tickerlake

A Helm chart that runs the [tickerlake](https://github.com/major/tickerlake) US
equity market ETL as a Kubernetes CronJob. The job runs the `update` subcommand
on a schedule (22:30 UTC Monday to Friday by default, after the US market
close) and refreshes the trailing revision window. You can override the
subcommand to `backfill` for a first-time load. The chart can talk to an
existing CloudNativePG Cluster, an optional single-node Postgres StatefulSet it
renders itself, or any Postgres reachable through a user-provided Secret.

## Prerequisites

- Kubernetes 1.29 or newer (the chart uses `spec.timeZone` on the CronJob).
- Helm 3.8 or newer.
- A container image that contains the tickerlake CLI (Python 3.14). The chart
  does **not** ship or pin an image, so you **must provide your own** image via
  `image.repository` and `image.tag`.
- A StorageClass that supports `ReadWriteOnce` PersistentVolumeClaims (the chart
  always creates a small output PVC).
- For `database.mode=cnpg`: the CloudNativePG operator and CRDs installed, plus
  an existing `Cluster` resource.
- A Massive API key. For production, put it in a pre-created Secret and point
  `massive.existingSecret` at it.

## Usage

### Recommended: existing CloudNativePG cluster

This is the recommended mode. CloudNativePG gives you high availability,
backups, and point-in-time recovery, none of which the embedded mode offers.

1. Install the CloudNativePG operator if you have not already:

   ```bash
   helm repo add cnpg https://cloudnative-pg.github.io/charts
   helm repo update
   helm upgrade --install cnpg cnpg/cloudnative-pg \
     --namespace cnpg-system --create-namespace
   ```

2. Create a small CloudNativePG `Cluster` (adjust storage and resources):

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

3. Create the Massive API key Secret:

   ```bash
   kubectl create secret generic tickerlake-massive \
     --from-literal=MASSIVE_API_KEY=your-key-here
   ```

4. Install the chart. `database.cnpg.clusterName` defaults to the release name,
   so set it explicitly if your Cluster has a different name:

   ```bash
   helm install tickerlake ./charts/tickerlake \
     --set image.repository=ghcr.io/you/tickerlake \
     --set image.tag=0.1.0 \
     --set massive.existingSecret=tickerlake-massive \
     --set database.mode=cnpg \
     --set database.cnpg.clusterName=tickerlake-db
   ```

   The CronJob reads `DATABASE_URL` from the `<cluster>-app` Secret that
   CloudNativePG manages (key `uri` by default).

5. Check the CronJob and run it once, outside the schedule:

   ```bash
   kubectl get cronjob
   kubectl create job --from=cronjob/tickerlake tickerlake-manual
   kubectl logs -l job-name=tickerlake-manual -f
   ```

### Standalone: embedded Postgres

This mode renders a single-node Postgres StatefulSet inside the release. It is
useful for evaluation, demos, or single-node clusters where you do not run the
CloudNativePG operator.

```bash
helm install tickerlake ./charts/tickerlake \
  --set image.repository=ghcr.io/you/tickerlake \
  --set image.tag=0.1.0 \
  --set massive.existingSecret=tickerlake-massive \
  --set database.mode=embedded
```

The chart creates a `<release>-postgres` Secret of type
`kubernetes.io/basic-auth` holding `username`, the password, and a ready-to-use
`uri`. If `database.embedded.postgresPassword` is empty the chart generates a
32-character password on first install and reuses it on later upgrades.

> **Why this is less recommended**
>
> - No high availability. A single Postgres pod means downtime during node
>   maintenance or pod rescheduling.
> - No continuous backups or point-in-time recovery. Durability is limited to
>   the PVC and whatever snapshot policy your storage layer provides.
> - Scaling and failover are manual. You own upgrades, major version changes,
>   and disaster recovery.
>
> Use CloudNativePG (or another managed Postgres) for anything that matters.

### Bring your own Postgres

Point the chart at a Secret you manage:

```bash
helm install tickerlake ./charts/tickerlake \
  --set image.repository=ghcr.io/you/tickerlake \
  --set image.tag=0.1.0 \
  --set massive.existingSecret=tickerlake-massive \
  --set database.mode=external \
  --set database.existingSecret.name=tickerlake-db \
  --set database.existingSecret.key=DATABASE_URL
```

## Configuration

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
| `cronjob.command` | Subcommand to run | `update` |
| `cronjob.outputDir` | Output directory inside the pod | `/data` |
| `cronjob.concurrencyPolicy` | `Allow`, `Forbid`, or `Replace` | `Forbid` |
| `cronjob.restartPolicy` | `OnFailure` or `Never` | `OnFailure` |
| `massive.existingSecret` | Pre-created Secret with the API key | `""` |
| `massive.secretKey` | Key inside that Secret | `MASSIVE_API_KEY` |
| `massive.apiKey` | Literal key; testing only | `""` |
| `database.mode` | `cnpg`, `embedded`, or `external` | `cnpg` |
| `database.cnpg.clusterName` | CloudNativePG Cluster name | release name |
| `database.cnpg.uriKey` | DSN key in the `<cluster>-app` Secret | `uri` |
| `database.existingSecret.name` | User-managed DSN Secret (external) | `""` |
| `database.existingSecret.key` | DSN key in that Secret | `DATABASE_URL` |
| `database.embedded.storageSize` | Embedded Postgres PVC size | `10Gi` |
| `persistence.size` | Output PVC size | `1Gi` |
| `serviceAccount.create` | Create a ServiceAccount | `true` |

### Security notes

- The literal `massive.apiKey` value creates a Secret in the release and is
  intended for testing only. Use `massive.existingSecret` in production.
- The literal `database.url` value is likewise testing-only. Use
  `database.existingSecret` in production.
- The CronJob container runs as a non-root user with a read-only root
  filesystem. A writable `emptyDir` is mounted at `/tmp`, and the output
  directory is a PVC.

## Uninstall

```bash
helm uninstall tickerlake
```

The output PVC created by the chart (`<release>-data`) and, in embedded mode,
the Postgres data PVC created by the StatefulSet `volumeClaimTemplates` are
**not** deleted by `helm uninstall`. Delete them explicitly if you no longer
need the data:

```bash
kubectl delete pvc -l app.kubernetes.io/instance=tickerlake
```

In CloudNativePG mode, uninstalling the chart leaves the `Cluster` CR and its
data untouched. Remove those separately if desired.
