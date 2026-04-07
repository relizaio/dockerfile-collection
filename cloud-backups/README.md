# Cloud Backup

A unified backup tool that streams OCI registry artifacts and PostgreSQL databases directly to AWS S3 or Azure Blob Storage, with inline `age` encryption. Zero disk I/O — all data flows through memory pipes.

## Commands

```
cloud-backup oci backup          # Stream OCI artifacts to cloud storage
cloud-backup oci restore         # Restore a single OCI backup from cloud storage to a registry
cloud-backup oci restore-rolling # Restore the most recent backup per repo/month in a rolling window
cloud-backup pg backup           # Stream a PostgreSQL pg_dump to cloud storage
cloud-backup pg restore          # Restore a PostgreSQL backup from cloud storage
```

## Shared flags (all subcommands)

| Flag | Env Variable | Description | Default |
| --- | --- | --- | --- |
| `--backup-storage-type` | `BACKUP_STORAGE_TYPE` | Cloud provider: `s3` or `azure` | `s3` |
| `--encryption-password` | `ENCRYPTION_PASSWORD` | If set, encrypts the stream using `age` | *(optional)* |
| `--dump-prefix` | `DUMP_PREFIX` | Prefix for the backup filename | `backup` |
| `--timeout` | `TIMEOUT` | Per-job stream timeout, e.g. `2h`, `90m` | `2h` |

---

## OCI — `cloud-backup oci`

Streams OCI artifact tarballs from a registry to cloud storage using `oras`. Output uses **deterministic naming**: the last path segment of each repository becomes the filename. Output format: `{repo-name}.tar.gz[.age]`. Re-running a backup overwrites the previous file. With `--append-rolling-months` the suffix `-{YYYY-MM}` is appended before the extension, yielding `{repo-name}-{YYYY-MM}.tar.gz[.age]`.

### OCI flags

| Flag | Env Variable | Description | Default |
| --- | --- | --- | --- |
| `--registry-host` | `REGISTRY_HOST` | Target OCI registry domain | *(required)* |
| `--registry-username` | `REGISTRY_USERNAME` | Registry authentication username | *(required)* |
| `--registry-token` | `REGISTRY_TOKEN` | Registry authentication token/password | *(required)* |
| `--max-concurrent-jobs` | `MAX_CONCURRENT_JOBS` | Simultaneous streams | `3` |
| `--plain-http` | `PLAIN_HTTP` | Use plain HTTP instead of HTTPS for the registry | `false` |

### `oci backup` flags

| Flag | Env Variable | Description |
| --- | --- | --- |
| `--registry-base-paths` | `REGISTRY_BASE_PATHS` | Comma-separated repository paths to back up |
| `--append-rolling-months` | `APPEND_ROLLING_MONTHS` | Append `YYYY-MM` suffix for current and previous month |

### `oci restore` flags

| Flag | Env Variable | Description |
| --- | --- | --- |
| `--backup-file` | `BACKUP_FILE` | Remote filename of the backup file in cloud storage |
| `--restore-to` | `RESTORE_TO` | Full target repository path in the registry: `namespace/repo` — must include the repo name, not just the namespace |

### `oci restore-rolling` flags

Restores a matrix of `repo × month` backups in one shot. Two mutually exclusive modes:

- **Mode A** (default): last N months ending at an optional anchor date. Expects backup files named `{repo}-{YYYY-MM}.tar.gz[.age]` in the configured storage bucket.
- **Mode B**: explicit inclusive date range `--from` / `--to`.

| Flag | Env Variable | Description | Default |
| --- | --- | --- | --- |
| `--restore-namespace` | `RESTORE_NAMESPACE` | Registry namespace to restore into | *(required)* |
| `--repos` | `REPOS` | Comma-separated repo names (without namespace) | *(required)* |
| `--months` | — | **Mode A**: number of recent months to restore | `2` |
| `--cutoff-date` | `CUTOFF_DATE` | **Mode A**: anchor date `YYYY-MM-DD`; defaults to today | today |
| `--from` | `FROM` | **Mode B**: start of date range `YYYY-MM-DD` | — |
| `--to` | `TO` | **Mode B**: end of date range `YYYY-MM-DD` | — |

> `--from`/`--to` and `--months`/`--cutoff-date` are mutually exclusive.

### OCI examples

```bash
# Backup two repos
cloud-backup oci backup \
  --registry-host registry.example.com \
  --registry-username admin \
  --registry-token "$TOKEN" \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --registry-base-paths "namespace/repo1,namespace/repo2"

# Backup with rolling months (backs up current + previous month variants)
cloud-backup oci backup \
  --registry-host registry.example.com \
  --registry-username admin \
  --registry-token "$TOKEN" \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --registry-base-paths "namespace/artifacts" \
  --append-rolling-months
# Produces files like:
#   artifacts-2026-04.tar.gz
#   artifacts-2026-03.tar.gz

# Restore to a different namespace (same repo name)
# List backup files first:
#   aws s3 ls s3://my-backup-bucket/
cloud-backup oci restore \
  --registry-host registry.example.com \
  --registry-username "$NEW_USERNAME" \
  --registry-token "$NEW_TOKEN" \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --backup-file "repo1.tar.gz" \
  --restore-to new-namespace/repo1
```

```bash
# restore-rolling — Mode A: last 2 months (default), repos in my-namespace
cloud-backup oci restore-rolling \
  --registry-host registry.example.com \
  --registry-username admin \
  --registry-token "$TOKEN" \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --restore-namespace my-namespace \
  --repos "rebom-artifacts,downloadable-artifacts"
# Fetches: rebom-artifacts-2026-03.tar.gz, rebom-artifacts-2026-04.tar.gz
#          downloadable-artifacts-2026-03.tar.gz, downloadable-artifacts-2026-04.tar.gz

# restore-rolling — Mode A: last 3 months ending at a specific date
cloud-backup oci restore-rolling \
  --registry-host registry.example.com \
  --registry-username admin --registry-token "$TOKEN" \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --restore-namespace my-namespace \
  --repos "rebom-artifacts" \
  --months 3 --cutoff-date 2026-03-01
# Restores: rebom-artifacts-2026-01.tar.gz, rebom-artifacts-2026-02.tar.gz, rebom-artifacts-2026-03.tar.gz

# restore-rolling — Mode B: explicit range
cloud-backup oci restore-rolling \
  --registry-host registry.example.com \
  --registry-username admin --registry-token "$TOKEN" \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --restore-namespace my-namespace \
  --repos "rebom-artifacts,downloadable-artifacts" \
  --from 2025-11-01 --to 2026-01-31
# Restores Nov-2025, Dec-2025, Jan-2026 for each repo
```

### OCI manual restore

```bash
# Decrypt (only if encrypted)
age -d -o restored.tar.gz downloaded.tar.gz.age

# Decompress and push
gunzip restored.tar.gz
oras restore --input ./restored.tar registry.example.com/namespace/repo
```

---

## PostgreSQL — `cloud-backup pg`

Streams `pg_dump -Fc` output directly to cloud storage. The custom format is already compressed, so no additional gzip wrapping is applied. Output format: `{prefix}-{timestamp}-{random}.dump[.age]`

**Note:** `PGPASSWORD` must be set in the environment — it is not accepted as a flag for security reasons.

### PG flags

| Flag | Env Variable | Description | Default |
| --- | --- | --- | --- |
| `--pg-host` | `PG_HOST` | PostgreSQL host. Accepts `host:port` syntax. | *(required)* |
| `--pg-port` | `PG_PORT` | PostgreSQL port | `5432` |
| `--pg-database` | `PG_DATABASE` | Database name | *(required)* |
| `--pg-user` | `PG_USER` | PostgreSQL username | *(required)* |

### `pg restore` flags

| Flag | Env Variable | Description |
| --- | --- | --- |
| `--backup-file` | `BACKUP_FILE` | Remote path of the backup file in cloud storage |
| `--restore-to` | `RESTORE_TO` | Target database name for `pg_restore` (optional — defaults to `--pg-database`) |
| `--download-only` | — | Download and decrypt to a local file instead of running `pg_restore` |
| `--output` | `OUTPUT` | Local output file path (required with `--download-only`) |

### PG examples

```bash
# Backup
export PGPASSWORD="secret"
cloud-backup pg backup \
  --pg-host my-postgres-host --pg-database mydb --pg-user myuser \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --dump-prefix "prod-mydb"

# Full automated restore (--restore-to defaults to --pg-database when omitted)
export PGPASSWORD="secret"
cloud-backup pg restore \
  --pg-host my-postgres-host --pg-database mydb --pg-user myuser \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --backup-file "prod-mydb-2025-01-15-03-00-00-abc123.dump"

# Restore to a different database
export PGPASSWORD="secret"
cloud-backup pg restore \
  --pg-host my-postgres-host --pg-database mydb --pg-user myuser \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --backup-file "prod-mydb-2025-01-15-03-00-00-abc123.dump" \
  --restore-to mydb_restored

# Download-only (manual pg_restore)
cloud-backup pg restore \
  --backup-storage-type s3 \
  --aws-bucket my-backup-bucket --aws-region us-east-1 \
  --aws-access-key-id "$KEY_ID" --aws-secret-access-key "$SECRET" \
  --backup-file "prod-mydb-2025-01-15-03-00-00-abc123.dump" \
  --download-only --output ./backup.dump

# Then restore manually:
pg_restore -h <PG_HOST> -U <PG_USER> -d <PG_DATABASE> --clean backup.dump
```

---

## Cloud Storage Configuration

### AWS S3

| Flag | Env Variable | Description |
| --- | --- | --- |
| `--aws-bucket` | `AWS_BUCKET` | S3 bucket name |
| `--aws-region` | `AWS_REGION` | AWS region |
| `--aws-access-key-id` | `AWS_ACCESS_KEY_ID` | AWS access key ID |
| `--aws-secret-access-key` | `AWS_SECRET_ACCESS_KEY` | AWS secret access key |

### Azure Blob Storage

| Flag | Env Variable | Description |
| --- | --- | --- |
| `--azure-storage-account` | `AZURE_STORAGE_ACCOUNT` | Storage account name |
| `--azure-container` | `AZURE_CONTAINER` | Target blob container name |
| `--azure-tenant-id` | `AZURE_TENANT_ID` | Directory (tenant) ID from App Registration |
| `--azure-client-id` | `AZURE_CLIENT_ID` | Application (client) ID from App Registration |
| `--azure-client-secret` | `AZURE_CLIENT_SECRET` | Client Secret **Value** (not the Secret ID) |

**Azure requirements:** Service Principal needs **Storage Blob Data Contributor** on the Storage Account. The container must pre-exist with Private access.

### Kubernetes secret examples

```bash
# S3
kubectl create secret generic cloud-backup \
  --from-literal=aws-bucket="your-bucket" \
  --from-literal=aws-region="us-east-1" \
  --from-literal=aws-access-key-id="AKIA..." \
  --from-literal=aws-secret-access-key="..." \
  --from-literal=encryption-password="optional"

# Azure
kubectl create secret generic cloud-backup-azure \
  --from-literal=azure-storage-account="account" \
  --from-literal=azure-tenant-id="..." \
  --from-literal=azure-client-id="..." \
  --from-literal=azure-client-secret="..." \
  --from-literal=azure-container="backups" \
  --from-literal=encryption-password="optional"
```