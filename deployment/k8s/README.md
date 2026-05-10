# Kubernetes Dev Environment

This directory contains Kubernetes manifests for running the Critical Infrastructure Data Frameworks ingestion engine in a local development cluster (minikube or kind). The stack mirrors the services provided by `deployment/docker/docker-compose.yml` — the same Vault and MinIO backing services, the same `cidf-engine` container image, and the same non-secret configuration — but expressed as Kubernetes resources rather than Compose services. All manifests are compatible with standard `kubectl` tooling; no Helm or Kustomize installation is required for the dev workflow described here.

---

## Contents

```
deployment/k8s/
└── dev/
    ├── namespace.yaml        # Namespace: cidf-dev
    ├── serviceaccount.yaml   # ServiceAccount: cidf-engine
    ├── configmap.yaml        # ConfigMap: cidf-engine-config
    ├── secret.yaml           # Secret skeleton: cidf-engine-secrets (reference only — no real values)
    ├── deployment.yaml       # Deployment: cidf-engine (image: cidf/ingestion-engine:0.1.0-SNAPSHOT)
    └── service.yaml          # Service: cidf-engine (ClusterIP, port 8080 placeholder)
```

---

## Section 1 — Prerequisites and Local Cluster Setup

### Prerequisites

| Tool | Minimum version | Notes |
|------|-----------------|-------|
| `kubectl` | 1.28 | Must be configured to reach your local cluster. |
| minikube | 1.33 | **OR** kind (see below). Not both required. |
| kind | 0.23 | Alternative to minikube for a lighter-weight cluster. |
| Docker | Engine 24+ | Required by both minikube (docker driver) and kind. |

### Option A — minikube

Start a cluster with enough resources for the JVM-based ingestion engine:

```bash
minikube start --cpus=4 --memory=4g --driver=docker
```

Then make the local Docker daemon visible inside minikube so that the locally-built image `cidf/ingestion-engine:0.1.0-SNAPSHOT` is found without a registry push:

```bash
eval $(minikube docker-env)
```

> **Why `minikube docker-env`?**  The ingestion engine image is built locally with `docker build` or `sbt docker:publishLocal`. Without this step, the minikube cluster cannot find the image because it has its own internal Docker daemon separate from your host daemon. Running `eval $(minikube docker-env)` (or `minikube docker-env | source` in zsh) redirects the `docker` CLI in your current shell to point at minikube's daemon. Build the image in that shell and minikube can pull it with `imagePullPolicy: IfNotPresent`.

Confirm the cluster is reachable:

```bash
kubectl cluster-info
```

### Option B — kind

Create a named cluster:

```bash
kind create cluster --name cidf-dev
kubectl cluster-info --context kind-cidf-dev
```

With kind, Docker image state is not shared between your host and the cluster. You must explicitly load the local image into the cluster after every build:

```bash
kind load docker-image cidf/ingestion-engine:0.1.0-SNAPSHOT --name cidf-dev
```

---

## Section 2 — Apply and Verify

Apply the manifests in the order shown below. Steps 1 and 2 must be completed before the Deployment can start, because the Deployment references both the ConfigMap (`cidf-engine-config`) and the Secret (`cidf-engine-secrets`) by name — if either is missing, the pod will fail to schedule.

### Step 1 — Create the namespace first

The namespace must exist before any other resources can be created in it:

```bash
kubectl apply -f deployment/k8s/dev/namespace.yaml
```

Expected output:
```
namespace/cidf-dev created
```

### Step 2 — Populate the Secret with real dev values

The `secret.yaml` in this directory is a **reference skeleton only** — all its values are `cmVwbGFjZS1tZQ==` (base64 for `replace-me`). Never edit that file with real credentials. Instead, create or update the Secret directly using `kubectl`:

```bash
kubectl create secret generic cidf-engine-secrets \
  --from-literal=VAULT_TOKEN=<your-dev-vault-token> \
  --from-literal=AWS_ACCESS_KEY_ID=<your-minio-user> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<your-minio-password> \
  --namespace cidf-dev \
  --dry-run=client -o yaml | kubectl apply -f -
```

Replace the angle-bracket placeholders with the same dev credentials you used in `deployment/docker/.env`:
- `VAULT_TOKEN` — the value of `VAULT_DEV_ROOT_TOKEN` from your `.env`
- `AWS_ACCESS_KEY_ID` — the value of `MINIO_ROOT_USER` from your `.env`
- `AWS_SECRET_ACCESS_KEY` — the value of `MINIO_ROOT_PASSWORD` from your `.env`

> **The `--dry-run=client -o yaml | kubectl apply -f -` pattern** renders the resource manifest client-side and pipes it to `kubectl apply`, which uses server-side create-or-patch semantics. This makes the command idempotent — safe to re-run whenever you need to rotate the dev credentials without deleting and recreating the Secret.

### Step 3 — Apply all remaining manifests

```bash
kubectl apply -f deployment/k8s/dev/
```

Expected output (order may vary; the Secret will show `configured` because it was already created in Step 2):

```
namespace/cidf-dev configured
serviceaccount/cidf-engine created
configmap/cidf-engine-config created
secret/cidf-engine-secrets configured
deployment.apps/cidf-engine created
service/cidf-engine created
```

### Step 4 — Verify all resources are present

```bash
kubectl get all -n cidf-dev
kubectl get configmap,secret -n cidf-dev
```

You should see the Deployment, ReplicaSet, Pod, and Service listed by `get all`, and `cidf-engine-config` (ConfigMap) plus `cidf-engine-secrets` (Secret) listed by the second command.

### Step 5 — Check Deployment rollout status

```bash
kubectl rollout status deployment/cidf-engine -n cidf-dev
kubectl describe deployment cidf-engine -n cidf-dev
```

### Step 6 — Check pod logs

```bash
kubectl logs -l app.kubernetes.io/name=cidf-engine -n cidf-dev
```

### Expected pod status at v0.1.0-SNAPSHOT

**The pod will enter `CrashLoopBackOff`. This is expected and is not a misconfiguration.**

The ingestion engine JAR does not yet have a `Main-Class` manifest attribute. Running `java -jar /app/engine.jar` exits immediately with:

```
no main manifest attribute, in /app/engine.jar
```

Kubernetes restarts the container, which exits again, producing the `CrashLoopBackOff` cycle. The Deployment is present at this version to validate that the security context, resource limits, probes, and volume configuration are correct before the Main class lands in v0.1.0.

To inspect the container environment while it is briefly running (between crash cycles), exec into it:

```bash
kubectl exec -it deploy/cidf-engine -n cidf-dev -- /bin/sh
```

---

## Section 3 — Configuration Override Guide

### Populating the Secret

The `secret.yaml` file defines the shape of the Secret (key names) and is safe to commit because all values are `replace-me` placeholders. **Never edit it with real credentials.** Use `kubectl create secret` (shown in Section 2, Step 2) or one of the production patterns documented in Section 4.

To verify the Secret's key names without decoding values:

```bash
kubectl get secret cidf-engine-secrets -n cidf-dev -o jsonpath='{.data}' | python3 -m json.tool
```

The output should show three keys: `VAULT_TOKEN`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

### Overriding ConfigMap values

The ConfigMap `cidf-engine-config` ships with hostnames that match the docker-compose internal network (`vault` and `minio`). When running Vault and MinIO as Kubernetes Services in the same cluster, override the ConfigMap to use cluster-local DNS names:

```bash
kubectl create configmap cidf-engine-config \
  --from-literal=VAULT_ADDR=http://vault.cidf-dev.svc.cluster.local:8200 \
  --from-literal=AWS_S3_ENDPOINT=http://minio.cidf-dev.svc.cluster.local:9000 \
  --from-literal=AWS_S3_PATH_STYLE_ACCESS=true \
  --namespace cidf-dev \
  --dry-run=client -o yaml | kubectl apply -f -
```

After updating the ConfigMap, restart the Deployment so the pod picks up the new values:

```bash
kubectl rollout restart deployment/cidf-engine -n cidf-dev
```

### Mounting a local pipeline configuration

The Deployment's `pipeline-configs` volume is an `emptyDir` placeholder. The mount path `/app/configs` is reserved but carries no files at this version. The two options below replace the placeholder with real config content.

#### Option A — minikube hostPath (local dev convenience)

Use `minikube mount` to expose the repository's `examples/configs/` directory into the cluster, then point the volume at the mount path.

In a separate terminal, start the mount (keep it running):

```bash
minikube mount /path/to/repo/examples/configs:/mnt/configs
```

Edit `deployment/k8s/dev/deployment.yaml` and replace the `pipeline-configs` volume definition:

```yaml
volumes:
  - name: pipeline-configs
    hostPath:
      path: /mnt/configs
      type: Directory
```

Apply the change:

```bash
kubectl apply -f deployment/k8s/dev/deployment.yaml
```

#### Option B — ConfigMap volume (portable, for small config files)

Package the YAML configs into a ConfigMap:

```bash
kubectl create configmap cidf-pipeline-configs \
  --from-file=examples/configs/ \
  --namespace cidf-dev
```

Then edit `deployment/k8s/dev/deployment.yaml` and replace the `pipeline-configs` emptyDir with:

```yaml
volumes:
  - name: pipeline-configs
    configMap:
      name: cidf-pipeline-configs
```

Apply the change:

```bash
kubectl apply -f deployment/k8s/dev/deployment.yaml
```

> **ConfigMap size limit:** Kubernetes ConfigMaps have a hard limit of 1 MiB of data. If the total size of your pipeline config files exceeds this limit, use a PersistentVolumeClaim or a hostPath mount instead.

---

## Section 4 — Production Considerations

> **Phase 1 scope:** The manifests in `deployment/k8s/dev/` are for local development only. The patterns documented in this section are guidance for production readiness. **None are delivered in Phase 1.**

### Helm chart

Package the manifests as a Helm chart with a `values.yaml` for environment-specific overrides: image tag, resource limits, Vault address, replica count, namespace. Helm enables versioned releases, clean upgrades, and diff-before-apply previews (`helm diff`). The `dev/` manifests are the natural source of truth for an initial Helm chart.

### Secret management

The current approach — a Kubernetes Secret populated via `kubectl create secret` — is acceptable for local development but should not be used in production. Two preferred alternatives:

**Vault Agent Injector**
Deploy the Vault Agent sidecar alongside the engine pod. The agent authenticates to Vault using Kubernetes Auth and injects secrets as files under `/vault/secrets/` in the container at startup. The engine reads credentials from these files rather than from environment variables. The `cidf-engine-secrets` Kubernetes Secret and the `VAULT_TOKEN` environment variable can both be removed from the Deployment once this pattern is in place — the agent handles authentication.

**External Secrets Operator (ESO)**
An `ExternalSecret` custom resource instructs ESO to fetch a secret from Vault (or AWS Secrets Manager, GCP Secret Manager) and materialise it as a native Kubernetes Secret. The native Secret is kept in sync automatically. This pattern is preferred for GitOps workflows where the cluster state is fully declarative. The `ExternalSecret` manifest is safe to commit; it describes where the secret lives, not what it contains.

### Network policy

Add a `NetworkPolicy` resource to the `cidf-dev` namespace using a default-deny-all posture, then allow only the specific egress ports the engine requires:

- Vault API: port 8200 (TCP) to the Vault Service
- MinIO S3 API: port 9000 (TCP) to the MinIO Service
- Postgres JDBC: port 5432 (TCP) to the source database Service

No ingress is required — the engine is a batch workload with no inbound connections (the port-8080 Service in `service.yaml` is a placeholder for a future metrics endpoint).

### RBAC

Create a `Role` scoped to `cidf-dev` that grants the `cidf-engine` ServiceAccount read access to ConfigMaps and Secrets in that namespace, then bind it with a `RoleBinding`. The ServiceAccount already sets `automountServiceAccountToken: false`, which ensures no Kubernetes API token is mounted in the pod by default. Add only the permissions the engine actually needs at runtime — apply least-privilege to the Role as well.

### Horizontal scaling

The ingestion engine is a batch workload. Use a Kubernetes `Job` (for one-shot runs) or `CronJob` (for scheduled runs) rather than a `Deployment` in production. The `Deployment` is retained in the dev manifests for convenience — it keeps a pod running so you can `exec` into it and inspect the container environment without triggering a one-shot job run.

### Resource tuning

The dev resource limits (`250m`–`1000m` CPU, `512Mi`–`2Gi` memory) are conservative starting points sized for a single-node laptop cluster. Profile the engine under representative production batch loads and right-size limits accordingly — JVM workloads can have spiky GC behaviour that makes memory limits particularly important to set correctly. Consider adding a `LimitRange` to the namespace to enforce minimum and maximum resource constraints on all pods.

---

## Troubleshooting

| Symptom | Likely cause | Resolution |
|---------|-------------|------------|
| Pod stuck in `CrashLoopBackOff` | No `Main-Class` in JAR (v0.1.0-SNAPSHOT) | Expected. See Section 2, Step 6 note. Exec into the pod to inspect the environment. |
| Pod stuck in `Pending` | Secret or ConfigMap missing | Ensure Steps 1 and 2 in Section 2 were completed before `kubectl apply -f deployment/k8s/dev/`. |
| `ImagePullBackOff` with minikube | Image not in minikube's daemon | Re-run `eval $(minikube docker-env)` in your build shell, then rebuild the image. |
| `ImagePullBackOff` with kind | Image not loaded into kind cluster | Run `kind load docker-image cidf/ingestion-engine:0.1.0-SNAPSHOT --name cidf-dev`. |
| ConfigMap values not updated after `kubectl apply` | Pod not restarted after ConfigMap change | Run `kubectl rollout restart deployment/cidf-engine -n cidf-dev`. Pods do not automatically reload ConfigMap changes. |
| `replace-me` value rejected at runtime | Secret not overridden before apply | Follow Section 2, Step 2 to create the Secret with real dev credentials before applying the full manifest set. |
| `emptyDir` at `/app/configs` has no files | `pipeline-configs` volume is still the placeholder | Follow Section 3 to replace the emptyDir with a hostPath or ConfigMap volume. |
