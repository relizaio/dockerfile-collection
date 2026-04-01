# PostgreSQL Backup

A PostgreSQL backup tool that streams `pg_dump` output directly into AWS S3 or Azure Blob Storage, applying inline `gzip` compression and optional `age` encryption. No data is written to disk.

## Features

* **Zero-Disk I/O:** Streams `pg_dump -Fc` stdout through gzip and age directly to cloud storage.
* **Resilience:** Built-in exponential backoff with up to 3 retry attempts.
* **Observability:** Emits structured JSON logs via `slog`.
* **Encryption:** Optional `age` scrypt encryption (same as OCI backups).

## Configuration

Configured entirely via environment variables.

| Variable | Description | Example |
| --- | --- | --- |
| `PG_HOST` | PostgreSQL host. Also accepts `host:port` syntax (port takes precedence over `PG_PORT`). | `my-postgres-service` |
| `PG_PORT` | PostgreSQL port. Defaults to `5432`. | `5440` |
| `PG_DATABASE` | Database name to dump. | `postgres` |
| `PG_USER` | PostgreSQL username. | `postgres` |
| `PGPASSWORD` | PostgreSQL password. | `secret` |
| `DUMP_PREFIX` | Prefix for the backup filename. | `prod-rearm-db` |
| `BACKUP_STORAGE_TYPE` | Destination cloud provider (`s3` or `azure`). | `s3` |
| `ENCRYPTION_PASSWORD` | *(Optional)* If set, encrypts output with `age`. | `super-secret-key` |

### Provider-Specific Variables

**If `BACKUP_STORAGE_TYPE=s3`:**

* `AWS_BUCKET`
* `AWS_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`

**If `BACKUP_STORAGE_TYPE=azure`:**

* `AZURE_STORAGE_ACCOUNT`: Storage account name.
* `AZURE_CONTAINER`: Target blob container name.
* `AZURE_TENANT_ID`: Directory (tenant) ID from App Registration.
* `AZURE_CLIENT_ID`: Application (client) ID from App Registration.
* `AZURE_CLIENT_SECRET`: Client Secret **Value** (not the Secret ID).

#### Azure Infrastructure Requirements

1. **RBAC:** Service Principal must have **Storage Blob Data Contributor** on the target Storage Account.
2. **Pre-created container:** Target blob container must already exist with Private access.

### Kubernetes Deployment

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

## Output Format

Backup files are named: `{DUMP_PREFIX}-{timestamp}-{random}.dump.gz[.age]`

The dump is in PostgreSQL custom format (`-Fc`), suitable for `pg_restore`.

## Restoring a Backup

### Prerequisites

* `age` CLI (if encryption was enabled)
* `pg_restore` and access to the target PostgreSQL instance

### Step 1: Download and Decrypt

```bash
# Download from S3
aws s3 cp s3://your-bucket/prod-rearm-db-2026-01-01-00-00-00-abc123.dump.gz.age ./backup.dump.gz.age

# Decrypt with age
age -d -o backup.dump.gz backup.dump.gz.age
```

### Step 2: Decompress

```bash
gunzip backup.dump.gz
# Results in: backup.dump
```

### Step 3: Restore

```bash
pg_restore -h <PG_HOST> -U <PG_USER> -d <PG_DATABASE> --clean backup.dump
```
