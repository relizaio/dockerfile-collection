# OCI Artifact Backup

An OCI artifacts backup tool that streams data from a container registry directly into AWS S3 or Azure Blob Storage. This tool uses the `oras` CLI, applying inline `gzip` compression and `age` encryption.

## Features

* **Zero-Disk I/O:** Never writes payload data to disk. Safe for read-only filesystems.
* **Bounded Concurrency:** Configurable worker pool with memory-safe stream interception.
* **Resilience:** Built-in exponential backoff for cloud provider rate limits/drops.
* **Observability:** Emits structured JSON logs natively compatible with Datadog/Fluentd, including atomic byte throughput.

## Configuration

The application is configured entirely via Environment Variables.

| Variable | Description | Example |
| --- | --- | --- |
| `REGISTRY_HOST` | Target OCI registry domain. | `registry.example.com` |
| `REGISTRY_USERNAME` | Registry authentication username. | `admin` |
| `REGISTRY_TOKEN` | Registry authentication token/password. | `my-secret-token` |
| `REGISTRY_BASE_PATHS` | Comma-separated list of repository paths to backup. | `namespace/repo1,namespace/repo2` |
| `DUMP_PREFIX` | Prefix attached to the final backup filename. | `prod-backup` |
| `BACKUP_STORAGE_TYPE` | Destination cloud provider (`s3` or `azure`). | `s3` |
| `MAX_CONCURRENT_JOBS` | Number of simultaneous streams (Default: 3). | `5` |
| `ENCRYPTION_PASSWORD` | *(Optional)* If set, encrypts the stream using `age`. | `super-secret-key` |

### Provider Specific Variables & Setup

**If `BACKUP_STORAGE_TYPE=s3`:**

* `AWS_BUCKET`
* `AWS_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`

**If `BACKUP_STORAGE_TYPE=azure`:**

* `AZURE_STORAGE_ACCOUNT`: Storage account name (e.g., `mycompanybackups`).
* `AZURE_CONTAINER`: Target blob container name (e.g., `oci-dumps`).
* `AZURE_TENANT_ID`: Directory (tenant) ID from App Registration.
* `AZURE_CLIENT_ID`: Application (client) ID from App Registration.
* `AZURE_CLIENT_SECRET`: Client Secret **Value** (not the Secret ID).

#### Azure Infrastructure Requirements

To successfully stream data to Azure, your infrastructure must be pre-configured:

1. **RBAC Permissions:** Your App Registration / Service Principal must be granted the **Storage Blob Data Contributor** role on the target Storage Account.
2. **Pre-created Container:** The target Blob Container must already exist, be set to Private access.

### Kubernetes Deployment

When deploying, ensure these secrets exist:

#### For S3:

```
kubectl create secret generic rearm-backup \
  --from-literal=aws-bucket="your-bucket" \
  --from-literal=aws-region="us-east-1" \
  --from-literal=aws-access-key-id="AKIA..." \
  --from-literal=aws-secret-access-key="..." \
  --from-literal=encryption-password="optional"
```

#### For Azure: 

```
kubectl create secret generic rearm-backup-azure \
  --from-literal=azure-storage-account="account" \
  --from-literal=azure-tenant-id="..." \
  --from-literal=azure-client-id="..." \
  --from-literal=azure-client-secret="..." \
  --from-literal=azure-container="backups" \
  --from-literal=encryption-password="optional"
```

## Restoring OCI Artifact Backups

### Prerequisites

The machine performing the restore must have:

* `age` CLI (if encryption was enabled)
* `oras` CLI (v1.3.0 or higher)
* Write access to the target container registry

## Step 1: Authenticate

Ensure your local ORAS CLI is logged in to the registry where you want to push the recovered data:

```bash
oras login -u "$REGISTRY_USERNAME" -p "$REGISTRY_TOKEN" "$REGISTRY_HOST"

```

## Step 2: Decrypt the Backup

Download your `.age` encrypted backup file from S3/Azure. Use the `age` CLI and your `ENCRYPTION_PASSWORD` to decrypt it:

```bash
age -d -o restored-backup.tar.gz downloaded-backup.tar.gz.age

```

## Step 3: Restore the Data

*Note: `oras` cannot read `.gz` files natively, so it must be unzipped to a raw `.tar` first.*

```bash
# 1. Strip the gzip compression (leaves a restored-backup.tar file)
gunzip restored-backup.tar.gz

# 2. Push the raw tarball directly to the registry
oras restore --input ./restored-backup.tar registry.example.com/namespace/recovered-repo

```