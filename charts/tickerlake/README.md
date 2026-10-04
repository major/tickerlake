# tickerlake

A Helm chart that runs the [tickerlake](https://github.com/major/tickerlake) US
equity market ETL as a Kubernetes CronJob. The chart connects to a Postgres
database owned by an existing CloudNativePG (CNPG) Cluster.

The job runs the `update` subcommand on a schedule (8:00 PM America/New_York
Monday to Friday by default, after the US market close) and refreshes the
trailing revision window. You can override the subcommand to `backfill` for a
first-time load.

## Prerequisites

- Kubernetes 1.29 or newer (the chart uses `spec.timeZone` on the CronJob).
- Helm 3.8 or newer.
- The [CloudNativePG](https://cloudnative-pg.io/) operator installed, plus a
  `Cluster` resource in the release namespace. The chart does **not** render
  the Cluster; it only consumes the `<cluster>-app` Secret that CNPG manages.
- A container image that contains the tickerlake CLI (Python 3.14). The chart
  defaults `image.repository` to `ghcr.io/major/tickerlake`. Build it with
  `make docker-build` from the repo root, or override
  `--set image.repository=...` to use your own image.
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

## Building the image

The chart defaults `image.repository` to `ghcr.io/major/tickerlake`, which is
built from the `Containerfile` at the repo root (Red Hat UBI 9 Python 3.14,
multi-stage, runs as non-root UID 1000). Build it locally with:

```bash
make docker-build
```

Override the registry and tag with `IMAGE_REPO` and `IMAGE_TAG`:

```bash
make docker-build IMAGE_REPO=ghcr.io/you/tickerlake IMAGE_TAG=0.1.0
```

`make docker-push` pushes both the pinned tag and `latest`.

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
| `image.repository` | tickerlake container image repository | `ghcr.io/major/tickerlake` |
| `image.tag` | Image tag; falls back to `.Chart.AppVersion` | `""` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `cronjob.schedule` | Cron schedule | `0 20 * * 1-5` |
| `cronjob.timezone` | Cron time zone | `America/New_York` |
| `cronjob.command` | Subcommand to run (`update`, `backfill`) | `update` |
| `cronjob.concurrencyPolicy` | `Allow`, `Forbid`, or `Replace` | `Forbid` |
| `cronjob.restartPolicy` | `OnFailure` or `Never` | `OnFailure` |
| `massive.existingSecret` | Pre-created Secret with the API key | `""` |
| `massive.secretKey` | Key inside that Secret | `MASSIVE_API_KEY` |
| `massive.apiKey` | Literal key; testing only | `""` |
| `database.clusterName` | CloudNativePG Cluster name | release name |
| `database.uriKey` | DSN key in the `<cluster>-app` Secret | `uri` |
| `serviceAccount.create` | Create a ServiceAccount | `true` |

### Security notes

- The literal `massive.apiKey` value creates a Secret in the release and is
  intended for testing only. Use `massive.existingSecret` in production.
- The CronJob container runs as a non-root user with a read-only root
  filesystem. A writable `emptyDir` is mounted at `/tmp` for scratch space.
- The CronJob connects directly to the Cluster's `-rw` Service on port 5432.
  Do not route it through a PgBouncer `Pooler` in transaction-pooling mode:
  tickerlake uses session-level advisory locks (`pg_advisory_lock`) to
  serialize writers, and advisory locks do not survive transaction pooling.

## Uninstall

```bash
helm uninstall tickerlake
```

The chart does not create any PersistentVolumeClaims, so there is no chart-owned
storage to clean up. Uninstalling the chart leaves the CloudNativePG `Cluster`
CR and its data untouched. Remove those separately if desired.