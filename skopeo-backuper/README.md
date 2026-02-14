# Skopeo Backuper

Alpine-based container image for backing up Kubernetes container images to S3 using Skopeo, kubectl, and AWS CLI.

## Features

- **Automatic image discovery** - Scans Kubernetes namespace and extracts all container images
- **Multi-arch support** - Builds for amd64 and arm64
- **Encrypted backups** - Optional AES-256-CBC encryption with PBKDF2 (600k iterations)
- **S3 upload** - Automatic upload to AWS S3
- **Image manifest** - Separate encrypted list of backed-up images
- **Digest support** - Works with both tag-based and digest-based image references

## Included Tools

- **Skopeo** - Container image copy and backup
- **kubectl v1.33.7** - Kubernetes cluster interaction
- **AWS CLI v2.33.15** - S3 upload
- **OpenSSL** - Encryption

## Required Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `K8S_NAMESPACE` | Kubernetes namespace to scan for pods | `production` |
| `AWS_BUCKET` | S3 bucket name for backup upload | `my-backup-bucket` |

## Optional Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AUTH_FILE` | Path to container registry auth config | `/auth/config.json` |
| `ENCRYPTION_PASSWORD` | Password for AES-256-CBC encryption | _(none - no encryption)_ |
| `BACKUP_PREFIX` | Prefix for S3 object names | `backup` |
| `AWS_ACCESS_KEY_ID` | AWS credentials | _(from environment)_ |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials | _(from environment)_ |
| `AWS_DEFAULT_REGION` | AWS region | _(from environment)_ |

## Kubernetes Setup

### 1. Create ConfigMap for registry auth

```bash
kubectl create secret generic registry-auth \
  --from-file=config.json=/path/to/docker/config.json \
  -n backup-namespace
```

### 2. Create ServiceAccount with RBAC

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: skopeo-backuper
  namespace: backup-namespace
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: pod-reader
  namespace: production  # namespace to backup
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: skopeo-backuper-pod-reader
  namespace: production
subjects:
- kind: ServiceAccount
  name: skopeo-backuper
  namespace: backup-namespace
roleRef:
  kind: Role
  name: pod-reader
  apiGroup: rbac.authorization.k8s.io
```

### 3. Create CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: skopeo-backuper
  namespace: backup-namespace
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: skopeo-backuper
          restartPolicy: OnFailure
          containers:
          - name: backuper
            image: registry.relizahub.com/library/skopeo-backuper:latest
            env:
            - name: K8S_NAMESPACE
              value: "production"
            - name: AWS_BUCKET
              value: "my-backup-bucket"
            - name: BACKUP_PREFIX
              value: "k8s-images"
            - name: ENCRYPTION_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: backup-secrets
                  key: encryption-password
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: aws-credentials
                  key: access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: aws-credentials
                  key: secret-access-key
            - name: AWS_DEFAULT_REGION
              value: "us-east-1"
            volumeMounts:
            - name: registry-auth
              mountPath: /auth
              readOnly: true
          volumes:
          - name: registry-auth
            secret:
              secretName: registry-auth
```

## Workflow

1. **Extract images** - Scans pods in specified namespace using `kubectl get po -o yaml`
2. **Filter and deduplicate** - Removes bare SHA256 digests and empty entries, keeps full image references
3. **Backup with Skopeo** - Copies each image to an individual `docker-archive` tar (digests and tags are stripped, all images tagged as `:backup`)
4. **Bundle** - Combines all individual image archives into a single tar
5. **Compress** - Gzips the tar archive
6. **Create manifest** - Generates timestamped list of backed-up images
7. **Encrypt** - Optionally encrypts both backup and manifest with AES-256-CBC
8. **Upload to S3** - Uploads both files with timestamped names

## S3 Artifacts

Two files are uploaded per backup:

1. **Backup archive**: `s3://${AWS_BUCKET}/${BACKUP_PREFIX}-${TIMESTAMP}.tar.gz[.enc]`
2. **Image manifest**: `s3://${AWS_BUCKET}/${BACKUP_PREFIX}-${TIMESTAMP}-images.txt[.enc]`

Timestamp format: `YYYY-MM-DD-HH-MM`

## Restore Process

### 1. Download and decrypt backup

```bash
# Download from S3
aws s3 cp s3://my-bucket/backup-2026-02-14-02-00.tar.gz.enc ./backup.tar.gz.enc

# Decrypt
openssl enc -aes-256-cbc -d -a -pbkdf2 -iter 600000 \
  -pass pass:"$ENCRYPTION_PASSWORD" \
  -in backup.tar.gz.enc -out backup.tar.gz

# Extract
gunzip backup.tar.gz
```

### 2. Extract and load images

The backup archive contains individual `docker-archive` tar files per image.

```bash
# Extract the bundle
mkdir restore && tar xf backup.tar -C restore

# Load each image
for f in restore/*.tar; do
    echo "Loading $f"
    # Docker
    docker load < "$f"
    # Or containerd (for Kubernetes)
    # ctr -n k8s.io image import "$f"
done
```

All images will be loaded with the `:backup` tag (e.g., `registry.io/app:backup`).

### 3. View image list

```bash
# Download and decrypt manifest
aws s3 cp s3://my-bucket/backup-2026-02-14-02-00-images.txt.enc ./images.txt.enc
openssl enc -aes-256-cbc -d -a -pbkdf2 -iter 600000 \
  -pass pass:"$ENCRYPTION_PASSWORD" \
  -in images.txt.enc -out images.txt

cat images.txt
```

## Notes

- **Image tagging**: All backed-up images are stored with a uniform `:backup` tag regardless of original tag or digest. After restore, retag as needed (e.g., `docker tag registry.io/app:backup registry.io/app:v1.2.3`)
- **Auth file**: Must be in Docker config.json format with registry credentials
- **Storage**: Backup size depends on number and size of container images in the namespace
- **Network**: Requires outbound access to container registries and S3

## Security Considerations

- Store `ENCRYPTION_PASSWORD` in Kubernetes Secrets, never in plain text
- Use IAM roles for pods instead of static AWS credentials when possible
- Restrict RBAC permissions to minimum required (read-only pod access)
- Rotate encryption passwords periodically
- Enable S3 bucket encryption and versioning for additional protection

## License

MIT License - See LICENSE file in repository root
